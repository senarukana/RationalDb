package engine

type DBConfigs struct {
	ServerId   uint32
	DbName     string
	DataPath   string
	BinLogPath string
}

type EngineConnection struct {
	UName  string
	Pass   string
	DbName string
}
