package config

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net/http"
	"net/url"

	"github.com/Netcracker/qubership-profiler-agent/diagtools/constants"
	"github.com/Netcracker/qubership-profiler-agent/diagtools/log"
	"github.com/Netcracker/qubership-profiler-agent/diagtools/utils"
)

type ServerCfg struct {
	CSConfig
	ConfigServerUrl string
}

func (serverCfg *ServerCfg) InitConfig(ctx context.Context) (err error) {
	serverCfg.ConfigServerUrl, err = constants.ConfigServerUrl()
	if err == nil {
		_, err = url.ParseRequestURI(serverCfg.ConfigServerUrl)
	}
	if err == nil {
		err = serverCfg.getServiceInfo(ctx) // Trying to get service details (namespace and service name)
	}
	if err == nil {
		serverCfg.IdpUrl, err = constants.IdpUrl() // Trying to get identity provider url
	}
	return
}

// ExportConfig allows to change nc-diagnostic-agent settings in case when CONFIG_SERVER is not empty
// In this case will use the following envs:
// * CONFIG_SERVER     - Config Server address for fetch settings from
// * IDP_URL           - IDP address for fetching a token
// * CLOUD_NAMESPACE   - microservice namespace
// * MICROSERVICE_NAME - microservice name
func (serverCfg *ServerCfg) ExportConfig(ctx context.Context) (err error) {
	err = serverCfg.InitConfig(ctx)
	if err != nil {
		log.Error(ctx, err, "could not init Config Server")
		return
	}
	err = serverCfg.prepareM2mToken(ctx)
	if err != nil {
		log.Error(ctx, err, "could not prepare m2m token")
		return
	}

	var body map[string]interface{}
	body, err = serverCfg.requestConfig(ctx)
	if err != nil {
		log.Error(ctx, err, "could not get config from Config Server")
		return
	}

	for _, property := range serverCfg.Properties {
		value := body[property]
		if value == nil {
			continue
		}
		stringValue := value.(string)
		log.Infof(ctx, "successfully read property from Config Server: '%s'='%s'", property, value)
		err = serverCfg.saveFile(ctx, property, []byte(stringValue))
	}

	return
}

func (serverCfg *ServerCfg) CheckEscConfigFile(ctx context.Context, filename string) (err error) {
	return serverCfg.MoveEscConfigFile(ctx, filename, constants.CustomConfigXmlFilePath)
}

func (serverCfg *ServerCfg) prepareM2mToken(ctx context.Context) (err error) {
	err = serverCfg.readClientCredentials(ctx) // Read client credentials for IDP
	if err != nil {
		return
	}
	err = serverCfg.generateM2MToken(ctx) // Generate m2m token
	return err
}

func (serverCfg *ServerCfg) requestConfig(ctx context.Context) (body map[string]interface{}, err error) {
	var endpoint string
	client, err := utils.HttpClient()
	if err != nil {
		return nil, err
	}
	endpoint, err = url.JoinPath(serverCfg.ConfigServerUrl, serverCfg.ServiceName, "default")
	if err != nil {
		return nil, err
	}
	log.Infof(ctx, "config server url: %s", endpoint)

	req, _ := http.NewRequest(http.MethodGet, endpoint, nil)
	req.Header.Add("Authorization", fmt.Sprintf("Bearer %s", serverCfg.M2MToken))
	var res *http.Response
	res, err = client.Do(req)
	if err != nil {
		return nil, errors.Join(errors.New("failed send request to config-server"), err)
	}
	log.Infof(ctx, "config server response status: %s", res.Status)
	log.Infof(ctx, "Bearer %s", serverCfg.M2MToken)
	defer func(Body io.ReadCloser) {
		errClose := Body.Close()
		if errClose != nil {
			err = errClose
		}
	}(res.Body)

	body, err = parseBody(ctx, res.Body)
	if err != nil {
		return nil, errors.Join(errors.New("error parsing from config-server"), err)
	}
	return body, nil
}

type configServerEnv struct {
	Name            string                             `json:"name"`
	Profiles        []string                           `json:"profiles"`
	PropertySources []configServerPropertySourceEntity `json:"propertySources"`
}

type configServerPropertySourceEntity struct {
	Name   string                 `json:"name"`
	Source map[string]interface{} `json:"source"`
}

func parseBody(ctx context.Context, body io.Reader) (map[string]interface{}, error) {
	restBody, readErr := io.ReadAll(body)
	if readErr != nil {
		log.Error(ctx, readErr, "ReadAll error")
		return nil, readErr
	}

	res := configServerEnv{}
	jsonErr := json.Unmarshal(restBody, &res)
	if jsonErr != nil {
		log.Error(ctx, jsonErr, "Unmarshal error")
		return nil, jsonErr
	}
	if len(res.PropertySources) == 0 {
		return nil, fmt.Errorf("empty list of property sources")
	}
	return res.PropertySources[0].Source, nil
}
