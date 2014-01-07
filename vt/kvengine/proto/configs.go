package proto

type DBConfigs struct {
	ServerId   uint32
	EngineName string
	DataName   string
	DataPath   string
	BinLogPath string

	Keyspace string
	Shard    string

	*RocksDbConfigs
	*LevelDbConfigs

	AppConnectParams   *DbConnectParams
	AdminConnectParams *DbConnectParams
}

type DbConnectParams struct {
	DbName   string
	UserName string
	Pass     string
}

type RocksDbConfigs struct {
	CreateIfMissing   bool
	ParanoidCheck     bool
	LRUCacheSize      int
	BloomFilterLength int
}

type LevelDbConfigs struct {
	CreateIfMissing   bool
	ParanoidCheck     bool
	LRUCacheSize      int
	BloomFilterLength int
}
