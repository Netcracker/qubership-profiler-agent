package config

import (
	"bufio"
	"context"
	"errors"
	"io/fs"
	"os"
	"path/filepath"
	"slices"
	"strings"

	"github.com/Netcracker/qubership-profiler-agent/diagtools/constants"
	"github.com/Netcracker/qubership-profiler-agent/diagtools/log"
)

type Config struct {
	Namespace    string
	ServiceName  string
	TargetFolder string
	Properties   []string
}

type ExportCfg interface {
	ExportConfig(ctx context.Context) error
}

func (config *Config) Prepare(args []string) error {
	if len(args) < 2 {
		return errors.New("command have to have 2 args at least")
	}

	config.TargetFolder = args[0]
	config.Properties = args[1:]

	err := os.MkdirAll(config.TargetFolder, 0755)
	return err
}

func (config *Config) FilterProperties(ctx context.Context) error {
	props := filterProperties(config.Properties, constants.NcDiagAgentService, constants.NcDiagMode)
	if len(props) > 0 {
		return config.ExportConfigFileContentToEnv(ctx, props)
	}
	return nil
}

func filterProperties(original []string, keywords ...string) []string {
	m := map[string]bool{}
	var props []string
	for _, property := range keywords {
		if slices.Contains(original, property) {
			if _, has := m[property]; !has {
				props = append(props, property)
			}
			m[property] = true
		}
	}
	return props
}

func (config *Config) ExportConfigFileContentToEnv(ctx context.Context, properties []string) (err error) {
	for _, property := range properties {
		targetFile := config.propFile(property)

		data, exists, e := config.readFullFile(ctx, targetFile)
		if !exists || e != nil {
			continue
		}

		propValue := string(data)
		log.Infof(ctx, "Overriding property '%s' with data from config: %s", property, propValue)
		err = os.Setenv(property, propValue)
		if err != nil {
			log.Errorf(ctx, err, "failed to set ENV var %s", property)
			return err
		}
	}
	return
}

func (config *Config) MoveEscConfigFile(ctx context.Context, filename string, newFilename string) (err error) {
	escConfigFile := config.propFile(filename)

	var lines int
	lines, _, err = config.readFile(ctx, escConfigFile, nil)
	if err != nil {
		return err
	}

	if lines >= 2 {
		diagFolder, found := os.LookupEnv(constants.NcDiagnosticFolder)
		if found {
			newPath := filepath.Join(diagFolder, newFilename)
			err = os.Rename(escConfigFile, newPath)
			if err != nil {
				log.Errorf(ctx, err, "failed to move file from %s to %s", escConfigFile, newPath)
			}
		} else {
			log.Errorf(ctx, err, "env variable %s is to be set", constants.NcDiagnosticFolder)
		}
	} else {
		log.Infof(ctx, "configuration for %s is not set", escConfigFile)
	}

	return
}

func (config *Config) getServiceInfo(ctx context.Context) (err error) {
	config.Namespace = strings.TrimSpace(os.Getenv(constants.NcCloudNamespace))
	if len(config.Namespace) == 0 {
		err = errors.New(constants.NcCloudNamespace + " is empty")
		log.Error(ctx, err, "environment variable "+constants.NcCloudNamespace+" is to be set up")
		return
	}
	config.ServiceName = strings.TrimSpace(os.Getenv(constants.NcMicroServiceName))
	if len(config.ServiceName) == 0 {
		log.Info(ctx, constants.NcMicroServiceName+" is empty. Trying to fallback to "+constants.NcServiceName)
		config.ServiceName = strings.TrimSpace(os.Getenv(constants.NcServiceName))
	}
	return
}

func (config *Config) propFile(fileName string) string {
	return filepath.Join(config.TargetFolder, fileName)
}

func (config *Config) saveFile(ctx context.Context, property string, val []byte) error {
	if val == nil {
		return nil
	}
	if len(val) == 0 {
		log.Infof(ctx, "got empty data for property '%s'", property)
		return nil
	}
	log.Infof(ctx, "persisting property value (%d bytes) to file '%s/%s'", len(val), config.TargetFolder, property)
	log.Infof(ctx, "actual value for property '%s' : %v", property, string(val))

	target := config.propFile(property)
	err := os.WriteFile(target, val, 0644)
	if err != nil {
		log.Errorf(ctx, err, "failed to write property to file '%s/%s'", config.TargetFolder, property)
	}
	return err
}

func (config *Config) readFullFile(ctx context.Context, filePath string) (body []byte, exists bool, err error) {
	exists, err = config.checkExists(ctx, filePath)
	if err != nil || !exists {
		return nil, exists, err
	}

	body, err = os.ReadFile(filePath)
	if err != nil {
		log.Errorf(ctx, err, "failed to read file %s", filePath)
	}
	return body, true, err
}

func (config *Config) readFile(ctx context.Context, filePath string, read func(string)) (lines int, exists bool, err error) {
	exists, err = config.checkExists(ctx, filePath)
	if err != nil || !exists {
		return 0, exists, err
	}

	file, _ := os.Open(filePath)
	fileScanner := bufio.NewScanner(file)
	for fileScanner.Scan() {
		if read != nil {
			line := fileScanner.Text()
			read(line)
		}
		lines++
	}

	err = file.Close()
	if err != nil {
		log.Errorf(ctx, err, "failed to close file")
	}
	return lines, true, err
}

func (config *Config) checkExists(ctx context.Context, filePath string) (exists bool, err error) {
	_, err = os.Stat(filePath)
	if err != nil {
		if errors.Is(err, fs.ErrNotExist) {
			log.Infof(ctx, "File %s does not exist", filePath)
			return false, nil
		} else {
			log.Errorf(ctx, err, "failed to get file: %v", filePath)
			return false, err
		}
	}
	return true, nil
}
