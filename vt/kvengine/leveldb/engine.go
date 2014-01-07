package leveldb

import (
	"sync/atomic"

	"github.com/jmhodges/levigo"
	"github.com/senarukana/rationaldb/vt/kvengine/proto"
)

type LevelDbEngine struct {
	config           *proto.DBConfigs
	dbOptions        *levigo.Options
	nextConnectionId int64
	db               *levigo.DB
}

var DefaultLevelDbConf = &proto.LevelDbConfigs{
	CreateIfMissing:   true,
	ParanoidCheck:     false,
	LRUCacheSize:      2 >> 10,
	BloomFilterLength: 0,
}

func NewLevelDbEngine() proto.DbEngine {
	levelEngine := new(LevelDbEngine)
	return levelEngine
}

func (engine *LevelDbEngine) Name() string {
	return "LevelDb"
}

func (engine *LevelDbEngine) Init(config *proto.DBConfigs) error {
	if config == nil {
		return proto.ErrNoEngineConfig
	}
	if config.LevelDbConfigs == nil {
		config.LevelDbConfigs = DefaultLevelDbConf
	}
	options := levigo.NewOptions()
	// options.SetCreateIfMissing(config.CreateIfMissing)
	options.SetCreateIfMissing(true)
	options.SetParanoidChecks(config.LevelDbConfigs.ParanoidCheck)
	if config.LevelDbConfigs.LRUCacheSize > 0 {
		options.SetCache(levigo.NewLRUCache(config.LevelDbConfigs.LRUCacheSize))
	}
	if config.LevelDbConfigs.BloomFilterLength > 0 {
		options.SetFilterPolicy(levigo.NewBloomFilter(config.LevelDbConfigs.BloomFilterLength))
	}
	engine.config = config
	engine.dbOptions = options
	db, err := levigo.Open(engine.config.DataPath, engine.dbOptions)
	if err != nil {
		return err
	}
	engine.db = db
	return nil
}

func (engine *LevelDbEngine) Connect(params *proto.DbConnectParams) (proto.DbConnection, error) {
	connection := new(LevelDbConnection)
	connection.id = atomic.AddInt64(&engine.nextConnectionId, 1)
	connection.db = engine.db
	// authorize
	return connection, nil
}

func (engine *LevelDbEngine) Shutdown() error {
	engine.dbOptions.Close()
	engine.db.Close()
	return nil
}

func (engine *LevelDbEngine) Destroy() error {
	engine.db.Close()
	err := levigo.DestroyDatabase(engine.config.DataPath, engine.dbOptions)
	if err != nil {
		return err
	}
	engine.dbOptions.Close()
	return nil
}
