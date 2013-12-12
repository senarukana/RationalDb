package conf

import (
	"fmt"
)

type DldbProxyConfiguration struct {
	ProxyConfiguration
	ServersConfiguration
	ClientConfiguration
	BufferPoolConfiguration
	BalancersConfiguration
	LogConfiguration
}

func InitProxyDldbConfiguration(configureFileName string) *DldbProxyConfiguration {
	proxyConfiguration := new(DldbProxyConfiguration)
	var (
		configure *ConfigFile
		err       error
	)
	if configureFileName != "" {
		if configure, err = ReadConfigFile(configureFileName); err != nil {
			panic(fmt.Sprintf("read configure file %s error %v", configureFileName, err))
		}
	} else {
		configure, err = ReadConfigFile(defaultConfigureName)
		if configure, err = ReadConfigFile(defaultConfigureName); err != nil {
			panic(fmt.Sprintf("read configure file %s error %v", defaultConfigureName, err))
		}
	}
	initProxyConfiguration(&proxyConfiguration.ProxyConfiguration, configure)
	initServersConfiguration(&proxyConfiguration.ServersConfiguration, configure)
	initBalancersConfiguration(&proxyConfiguration.BalancersConfiguration, configure)
	initLogConfiguration(&proxyConfiguration.LogConfiguration, configure)
	return proxyConfiguration
}
