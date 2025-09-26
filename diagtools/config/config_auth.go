package config

import (
	"bufio"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net/http"
	"net/url"
	"os"
	"strconv"
	"strings"

	"github.com/Netcracker/qubership-profiler-agent/diagtools/constants"
	"github.com/Netcracker/qubership-profiler-agent/diagtools/log"
	"github.com/Netcracker/qubership-profiler-agent/diagtools/utils"
)

type CSConfig struct {
	Config
	IdpUrl       string
	ClientId     string
	ClientSecret string
	M2MToken     string
}

var (
	testMockMap = map[string]string{}
)

func (csConfig *CSConfig) readClientCredentials(ctx context.Context) (err error) {
	csConfig.ClientId, err = readSecret(ctx, constants.UserSecret)
	if err == nil {
		csConfig.ClientSecret, err = readSecret(ctx, constants.PswdSecret)
	}
	if err != nil {
		log.Error(ctx, err, "could not read secrets for IDP")
	}
	return
}

func readSecret(ctx context.Context, path string) (value string, err error) {
	if val, has := testMockMap[path]; has {
		return val, nil
	}

	var file *os.File
	file, err = os.Open(path)
	if err != nil {
		log.Errorf(ctx, err, "failed to open file %s", path)
		return
	}
	defer func(file *os.File) {
		errClose := file.Close()
		if errClose != nil {
			log.Errorf(ctx, errClose, "failed to close file %s", path)
			err = errClose
		}
	}(file)

	fileScanner := bufio.NewScanner(file)
	fileScanner.Split(bufio.ScanLines)
	for fileScanner.Scan() {
		value = fileScanner.Text()
		break
	}

	return
}

func (csConfig *CSConfig) generateM2MToken(ctx context.Context) (err error) {
	if accessToken, has := testMockMap["m2mToken"]; has {
		csConfig.M2MToken = accessToken
		return
	}
	clientM2M, err := utils.HttpClient()
	if err != nil {
		return errors.Join(errors.New("error while configuring HTTP client with TLS"), err)
	}
	// Create body payload
	data := url.Values{}
	data.Set("client_secret", csConfig.ClientSecret)
	data.Set("client_id", csConfig.ClientId)
	data.Add("grant_type", "client_credentials")
	encodedData := data.Encode()

	// Create POST to get token
	var tokenUrl string
	tokenUrl, err = url.JoinPath(csConfig.IdpUrl + constants.TokenPath)
	if err != nil {
		return errors.Join(errors.New("error while creating url path"), err)
	}

	// prepare request
	var req *http.Request
	req, err = http.NewRequest(http.MethodPost, tokenUrl, strings.NewReader(encodedData))
	if err != nil {
		return errors.Join(errors.New("error while creating request"), err)
	}
	req.Header.Add("Content-Type", "application/x-www-form-urlencoded")
	req.Header.Add("Content-Length", strconv.Itoa(len(data.Encode())))

	// Process POST request
	var response *http.Response
	response, err = clientM2M.Do(req)
	if err != nil {
		return errors.Join(errors.New("error while creating m2m token"), err)
	}

	// Read response body
	var body []byte
	body, err = io.ReadAll(response.Body)
	if err != nil {
		return errors.Join(errors.New("error while read response body"), err)
	}
	err = response.Body.Close()
	if err != nil {
		return errors.Join(errors.New("error while close response body"), err)
	}

	// Decoding the json data and storing in the idpResponse map
	var idpResponse map[string]interface{}
	err = json.Unmarshal(body, &idpResponse)
	if err != nil {
		return errors.Join(errors.New("error while decoding the data: "), err)
	}

	accessToken, isOk := idpResponse["access_token"].(string)
	if !isOk {
		err = errors.New("error while creating m2m token")
		for key, vale := range idpResponse {
			err = errors.Join(err, fmt.Errorf("%s:%s", key, vale))
		}
		return err
	}
	csConfig.M2MToken = accessToken
	log.Info(ctx, "m2m token is successfully created")
	return
}
