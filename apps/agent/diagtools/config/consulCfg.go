package config

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net/http"
	"net/url"
	"strings"

	"github.com/Netcracker/qubership-profiler-agent/diagtools/constants"
	"github.com/Netcracker/qubership-profiler-agent/diagtools/log"
	"github.com/Netcracker/qubership-profiler-agent/diagtools/utils"

	capi "github.com/hashicorp/consul/api"
)

type ConsulCfg struct {
	CSConfig
	KV        *capi.KV
	ConsulUrl string
}

func (consulCfg *ConsulCfg) InitConfig(ctx context.Context) (err error) {
	consulCfg.ConsulUrl, err = constants.ConsulUrl()
	if err == nil {
		_, err = url.ParseRequestURI(consulCfg.ConsulUrl)
	}
	if err == nil {
		err = consulCfg.getServiceInfo(ctx) // Trying to get service details (namespace and service name)
	}
	if err == nil {
		consulCfg.IdpUrl, err = constants.IdpUrl() // Trying to get identity provider url
	}
	return
}

// ExportConfig allows to change nc-diagnostic-agent settings in case when CONSUL_ENABLED=true
// In this case will use the following envs:
// * CONSUL_ADDRESS    - Consul address for fetch settings from Consul
// * IDP_URL           - IDP address for fetching a token
// * CLOUD_NAMESPACE   - microservice namespace
// * MICROSERVICE_NAME - microservice name
func (consulCfg *ConsulCfg) ExportConfig(ctx context.Context) (err error) {
	err = consulCfg.InitConfig(ctx)
	if err != nil {
		return
	}

	aclToken, err := consulCfg.prepareAclToken(ctx)
	if err != nil {
		return err
	}

	err = consulCfg.prepareClient(ctx, aclToken)
	if err != nil {
		return err
	}

	for _, property := range consulCfg.Properties {
		configValue, errProp := consulCfg.configFromConsul(property)
		if errProp == nil {
			log.Infof(ctx, "successfully read property from Consul: '%s'='%s'", property, configValue)
			err = consulCfg.saveFile(ctx, property, configValue)
		} else {
			log.Errorf(ctx, errProp, "failed to get property '%s' from Consul ", property)
		}
	}
	return
}

func (consulCfg *ConsulCfg) CheckEscConfigFile(ctx context.Context, filename string) (err error) {
	return consulCfg.MoveEscConfigFile(ctx, filename, constants.CustomConfigXmlFilePath)
}

// configFromConsul
// Config path: config/<microservice_namespace>/<microservice_name>/<property>
// Return: config or nil if it does not exist
func (consulCfg *ConsulCfg) configFromConsul(property string) ([]byte, error) {
	propPath := path("config", consulCfg.Namespace, consulCfg.ServiceName, property)
	pair, _, err := consulCfg.KV.Get(propPath, nil)
	if err != nil || pair == nil {
		propPath = path("config", consulCfg.Namespace, constants.ServerAppPath, property)
		pair, _, err = consulCfg.KV.Get(propPath, nil)
		if err != nil {
			return nil, fmt.Errorf("property '%s': %s", propPath, err.Error())
		}
	}
	if pair == nil {
		return nil, fmt.Errorf("property '%s': KV pair is nil", propPath)
	}
	return pair.Value, nil
}

//nolint:unused
func (consulCfg *ConsulCfg) setProperty(ctx context.Context, svc, property string, value string) (err error) {
	if len(svc) == 0 {
		svc = consulCfg.ServiceName
	}
	propPath := path("config", consulCfg.Namespace, svc, property)

	p := &capi.KVPair{Key: propPath, Value: []byte(value)}
	_, err = consulCfg.KV.Put(p, nil)
	if err != nil {
		log.Errorf(ctx, err, "failed to put property '%s'", property)
	}
	return err
}

func (consulCfg *ConsulCfg) prepareClient(ctx context.Context, aclToken string) error {
	// Create consul connection config
	config := capi.DefaultConfig()
	config.Address = consulCfg.ConsulUrl
	config.Datacenter = constants.ConsulDC
	config.Token = aclToken

	// Get a new consul API client
	client, err := capi.NewClient(config)
	if err != nil {
		log.Error(ctx, err, "problem with creating Consul client")
		return fmt.Errorf("export from Consul failed: %s", err)
	}

	// Get a handle to the KV API
	consulCfg.KV = client.KV()
	return nil
}

func (consulCfg *ConsulCfg) prepareAclToken(ctx context.Context) (string, error) {
	// Read client credentials for IDP
	err := consulCfg.readClientCredentials(ctx)
	if err != nil {
		return "", err
	}
	// Generate m2m token
	err = consulCfg.generateM2MToken(ctx)
	if err != nil {
		return "", fmt.Errorf("export from Consul failed: %v", err)
	}

	// Generate ACL token
	var aclToken string
	aclToken, err = consulCfg.getSecretIdByM2MToken(ctx)
	if err != nil {
		return "", fmt.Errorf("export from Consul failed: %v", err)
	}
	return aclToken, nil
}

func (consulCfg *ConsulCfg) getSecretIdByM2MToken(ctx context.Context) (token string, err error) {
	if token, has := testMockMap["aclToken"]; has {
		return token, nil
	}

	payload := map[string]string{
		"AuthMethod":  consulCfg.Namespace,
		"BearerToken": consulCfg.M2MToken,
	}
	var payloadJson []byte
	payloadJson, err = json.Marshal(payload)
	if err != nil {
		return
	}

	var consulAlcLoginUrl string
	consulAlcLoginUrl, err = url.JoinPath(consulCfg.ConsulUrl, constants.ConsulAclPath)
	if err != nil {
		return
	}

	var response *http.Response
	client, err := utils.HttpClient()

	if err != nil {
		return
	}

	response, err = client.Post(consulAlcLoginUrl, "application/json", strings.NewReader(string(payloadJson)))
	if err != nil {
		return
	}
	defer func(Body io.ReadCloser) {
		errClose := Body.Close()
		if errClose != nil {
			err = errClose
		}
	}(response.Body)

	var respBody []byte
	respBody, err = io.ReadAll(response.Body)
	if err != nil {
		return
	}
	if response.StatusCode != http.StatusOK {
		return "", fmt.Errorf("error while generating ACL token: response code: %s", response.Status)
	}
	var respBodyMap map[string]interface{}
	err = json.Unmarshal(respBody, &respBodyMap)
	if err != nil {
		return
	}
	secretId, isOk := respBodyMap["SecretID"].(string)
	if !isOk {
		return "", errors.New("error while creating ACL token")
	}
	log.Info(ctx, "Successfully created ACL token")
	return secretId, nil
}
