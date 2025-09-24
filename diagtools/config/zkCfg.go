package config

import (
	"context"
	"errors"
	"os"
	"strings"
	"time"

	"github.com/Netcracker/qubership-profiler-agent/diagtools/constants"
	"github.com/Netcracker/qubership-profiler-agent/diagtools/log"

	"github.com/go-zookeeper/zk"
)

type ZkCfg struct {
	Config
	ZkAddresses []string
	ZkConnect   *zk.Conn
}

func (zkCfg *ZkCfg) InitConfig(ctx context.Context) (err error) {
	err = zkCfg.getServiceInfo(ctx)
	if err != nil {
		return
	}

	zkAddressesStr := strings.TrimSpace(os.Getenv(constants.ZkAddress))
	if len(zkAddressesStr) == 0 {
		err = errors.New("failed to get zookeeper address from " + constants.ZkAddress)
		return
	}
	zkCfg.ZkAddresses = strings.Split(zkAddressesStr, ",")
	for i, addr := range zkCfg.ZkAddresses {
		zkCfg.ZkAddresses[i] = strings.TrimSpace(addr)
	}
	return
}

// ExportConfig allows to change nc-diagnostic-agent settings in case when ZOOKEEPER_ENABLED=true
// In this case will use the following envs:
// * ZOOKEEPER_ADDRESS - ZooKeeper address for fetch settings from ZooKeeper
// * CLOUD_NAMESPACE - microservice namespace
// * MICROSERVICE_NAME - microservice name
func (zkCfg *ZkCfg) ExportConfig(ctx context.Context) (err error) {
	err = zkCfg.InitConfig(ctx)
	if err != nil {
		return
	}

	zkCfg.ZkConnect, _, err = zk.Connect(zkCfg.ZkAddresses, time.Second)
	if err != nil {
		return
	}
	defer zkCfg.ZkConnect.Close()

	for _, property := range zkCfg.Properties {
		val := zkCfg.configFromZK(ctx, property)
		if val == nil {
			continue
		}
		log.Infof(ctx, "successfully read property from Zookeeper: '%s'='%s'", property, *val)
		errWrt := zkCfg.saveFile(ctx, property, []byte(*val))
		if errWrt != nil {
			err = errors.Join(err, errWrt)
		}
	}
	return
}

// ConfigFromZK search configs by paths, in specified order:
// 1. /config/<microservice_namespace>/<microservice_name>/<property>
// 2. /config/<microservice_namespace>/application/<property>
// If not found - return nil
func (zkCfg *ZkCfg) configFromZK(ctx context.Context, property string) *string {
	propPath := path(constants.ZkConfigPrefix, zkCfg.Namespace, zkCfg.ServiceName, property)
	msgBytes, _, err := zkCfg.ZkConnect.Get(propPath)
	if errors.Is(err, zk.ErrNoNode) {
		propPath = path(constants.ZkConfigPrefix, zkCfg.Namespace, constants.ServerAppPath, property)
		msgBytes, _, err = zkCfg.ZkConnect.Get(propPath)
		if errors.Is(err, zk.ErrNoNode) {
			log.Errorf(ctx, err, "failed to retrieve from ZK (%s)", propPath)
			return nil
		}
	}
	if err != nil {
		log.Errorf(ctx, err, "failed to retrieve from ZK (%s)", propPath)
		return nil
	}
	result := string(msgBytes)
	return &result
}

func (zkCfg *ZkCfg) CheckEscConfigFile(ctx context.Context, filename string) (err error) {
	return zkCfg.MoveEscConfigFile(ctx, filename, constants.ZkConfigPrefix+"/"+constants.ZkConfigXmlFilePath)
}

// utilities for test purposes:

func (zkCfg *ZkCfg) setProperty(ctx context.Context, svc, property string, value string) (err error) {
	if len(svc) == 0 {
		svc = zkCfg.ServiceName
	}

	err = zkCfg.create(ctx, path(constants.ZkConfigPrefix), "")
	if err == nil {
		err = zkCfg.create(ctx, path(constants.ZkConfigPrefix, zkCfg.Namespace), "")
	}
	if err == nil {
		err = zkCfg.create(ctx, path(constants.ZkConfigPrefix, zkCfg.Namespace, svc), "")
	}
	if err == nil {
		err = zkCfg.create(ctx, path(constants.ZkConfigPrefix, zkCfg.Namespace, svc, property), value)
	}
	if err != nil {
		log.Errorf(ctx, err, "failed create property for ZK (%s)", property)
	}
	return err
}

func (zkCfg *ZkCfg) create(ctx context.Context, path string, value string) error { // create/update zk node
	_, err := zkCfg.ZkConnect.Create(path, []byte(value), 0, zk.WorldACL(zk.PermAll))
	if err != nil && errors.Is(err, zk.ErrNodeExists) {
		var stat *zk.Stat
		_, stat, err = zkCfg.ZkConnect.Get(path)
		_, err = zkCfg.ZkConnect.Set(path, []byte(value), stat.Version)
	}
	if err != nil {
		log.Errorf(ctx, err, "Could not create zk node for property '%s' ", path)
	} else {
		log.Infof(ctx, "Set zk property '%s' to '%s'", path, value)
	}
	return err
}

func (zkCfg *ZkCfg) inConnect(ctx context.Context, f func()) error {
	err := zkCfg.InitConfig(ctx)
	if err != nil {
		return err
	}

	zkCfg.ZkConnect, _, err = zk.Connect(zkCfg.ZkAddresses, time.Second)
	if err != nil {
		return err
	}
	defer zkCfg.ZkConnect.Close()

	f()
	return err
}

func path(args ...string) string {
	return strings.Join(args, "/")
}
