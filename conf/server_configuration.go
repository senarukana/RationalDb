package conf

import (
	"fmt"
)

type DldbServerConfiguration struct {
	ServerConfiguration
	ClientConfiguration
	EngineConfiguration
	BalancersConfiguration
	LogConfiguration
}

func InitServerDldbConfiguration(configureFileName string) *DldbServerConfiguration {
	serverConfiguration := new(DldbServerConfiguration)
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
	initServerConfiguration(&serverConfiguration.ServerConfiguration, configure)
	initClientConfiguration(&serverConfiguration.ClientConfiguration, configure)
	initBalancersConfiguration(&serverConfiguration.BalancersConfiguration, configure)
	initEngineConfiguration(&serverConfiguration.EngineConfiguration, configure)
	initLogConfiguration(&serverConfiguration.LogConfiguration, configure)
	return serverConfiguration
}
