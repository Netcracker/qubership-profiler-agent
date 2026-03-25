package config

import (
	"context"
	"fmt"
	"slices"

	"github.com/Netcracker/qubership-profiler-backend/libs/files"
	"github.com/Netcracker/qubership-profiler-backend/libs/log"
	"github.com/Netcracker/qubership-profiler-backend/libs/storage"
	"gopkg.in/yaml.v3"
)

const (
	defaultTempTablesCreationRange = 2       // 2 hours
	defaultTempTablesRemovalRange  = 2       // 2 hours
	defaultS3RemovalRange          = 24 * 14 // 2 weeks
	defaultMetadataRemovalRange    = 24 * 14 // 2 weeks
)

type TimeHours uint

type JobConfig struct {
	TempTableCreation TempTableCreationJobConfig `yaml:"tempTableCreation"`
	TempTableRemoval  TempTableRemovalJobConfig  `yaml:"tempTableRemoval"`
	S3FileRemoval     S3RemoveJobConfig          `yaml:"s3FileRemoval"`
	MetadataRemoval   MetadataRemovalJobConfig   `yaml:"metadataRemoval"`
}

type TempTableCreationJobConfig TimeHours
type TempTableRemovalJobConfig TimeHours
type MetadataRemovalJobConfig TimeHours

type S3RemoveJobConfig struct {
	Calls CallsS3RemoveJobConfig `yaml:"calls"`
	Dumps DumpsS3RemoveJobConfig `yaml:"dumps"`
	Heaps HeapsS3RemoveJobConfig `yaml:"heaps"`
	// traces are not supported
}

type CallsS3RemoveJobConfig struct {
	Map map[model.DurationRange]TimeHours `yaml:"-"`
}

func (c *CallsS3RemoveJobConfig) Get(dr model.DurationRange) TimeHours {
	return c.Map[dr]
}

func (c *CallsS3RemoveJobConfig) DurationRangesList() []model.DurationRange {
	var result = make([]model.DurationRange, 0, len(c.Map))
	for key := range c.Map {
		result = append(result, key)
	}
	return result
}

func (c *CallsS3RemoveJobConfig) UnmarshalYAML(n *yaml.Node) error {
	obj := map[string]TimeHours{}
	if err := n.Decode(obj); err != nil {
		return err
	}

	for drStr, hours := range obj {
		if dr := model.Durations.GetByName(drStr); dr != nil {
			c.Map[*dr] = hours
		} else {
			return fmt.Errorf("found unsupported duration range in configuration: %s", drStr)
		}
	}

	return nil
}

type DumpsS3RemoveJobConfig struct {
	Map map[model.DumpType]TimeHours `yaml:"-"`
}

func (c *DumpsS3RemoveJobConfig) Get(dumpType model.DumpType) TimeHours {
	return c.Map[dumpType]
}

func (c *DumpsS3RemoveJobConfig) DumpTypesList() []model.DumpType {
	var result = make([]model.DumpType, 0, len(c.Map))
	for key := range c.Map {
		result = append(result, key)
	}
	return result
}

func (c *DumpsS3RemoveJobConfig) UnmarshalYAML(n *yaml.Node) error {
	obj := map[model.DumpType]TimeHours{}
	if err := n.Decode(obj); err != nil {
		return err
	}

	for dt, hours := range obj {
		if slices.Contains(model.AllDumpTypes[:], dt) {
			c.Map[dt] = hours
		} else {
			return fmt.Errorf("found unsupported dump type in configuration: %s", dt)
		}
	}

	return nil
}

type HeapsS3RemoveJobConfig TimeHours

func GetDefaultConfig() *JobConfig {
	defaultCallsConfig := map[model.DurationRange]TimeHours{}
	for _, dr := range model.Durations.List {
		defaultCallsConfig[dr] = defaultS3RemovalRange
	}
	defaultDumpsConfig := map[model.DumpType]TimeHours{}
	for _, dt := range model.AllDumpTypes {
		defaultDumpsConfig[dt] = defaultS3RemovalRange
	}

	return &JobConfig{
		TempTableCreation: defaultTempTablesCreationRange,
		TempTableRemoval:  defaultTempTablesRemovalRange,
		S3FileRemoval: S3RemoveJobConfig{
			Calls: CallsS3RemoveJobConfig{defaultCallsConfig},
			Dumps: DumpsS3RemoveJobConfig{defaultDumpsConfig},
			Heaps: defaultS3RemovalRange,
		},
		MetadataRemoval: defaultMetadataRemovalRange,
	}
}

func ParseConfigFromFile(ctx context.Context, configLocation string) (*JobConfig, error) {
	jobConfig := GetDefaultConfig()
	// Return empty value if no job config specified
	if configLocation == "" {
		log.Info(ctx, "No job config file specified, default one will be used")
		return jobConfig, nil
	}
	if err := files.ParseYamlFile(configLocation, jobConfig); err != nil {
		return nil, err
	}

	return jobConfig, nil
}
