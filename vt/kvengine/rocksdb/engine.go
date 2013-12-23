package rocksdb

import (
	"sync/atomic"

	"github.com/senarukana/ratgo"
	"github.com/senarukana/rationaldb/vt/kvengine/proto"
)

type RocksDbEngine struct {
	config           *proto.DBConfigs
	dbOptions        *ratgo.Options
	nextConnectionId int64
	db               *ratgo.DB
}

var DefaultRocksDbConf = &proto.RocksDbConfigs{
	CreateIfMissing:   true,
	ParanoidCheck:     false,
	LRUCacheSize:      2 >> 10,
	BloomFilterLength: 0,
}

func NewRocksDbEngine() proto.DbEngine {
	rocksEngine := new(RocksDbEngine)
	return rocksEngine
}

func (engine *RocksDbEngine) Name() string {
	return "rocksdb"
}

func (engine *RocksDbEngine) Init(config *proto.DBConfigs) error {
	if config == nil {
		return proto.ErrNoEngineConfig
	}
	if config.RocksDbConfigs == nil {
		config.RocksDbConfigs = DefaultRocksDbConf
	}
	options := ratgo.NewOptions()
	// options.SetCreateIfMissing(config.CreateIfMissing)
	options.SetCreateIfMissing(true)
	options.SetParanoidChecks(config.ParanoidCheck)
	if config.LRUCacheSize > 0 {
		options.SetCache(ratgo.NewLRUCache(config.LRUCacheSize))
	}
	if config.BloomFilterLength > 0 {
		options.SetFilterPolicy(ratgo.NewBloomFilter(config.BloomFilterLength))
	}
	engine.config = config
	engine.dbOptions = options
	db, err := ratgo.Open(engine.config.DataPath, engine.dbOptions)
	if err != nil {
		panic(err)
		return err
	}
	engine.db = db
	return nil
}

func (engine *RocksDbEngine) Connect(params *proto.DbConnectParams) (proto.DbConnection, error) {
	connection := new(RocksDbConnection)
	connection.id = atomic.AddInt64(&engine.nextConnectionId, 1)
	connection.db = engine.db
	// authorize
	return connection, nil
}

func (engine *RocksDbEngine) Shutdown() error {
	engine.dbOptions.Close()
	engine.db.Close()
	return nil
}

func (engine *RocksDbEngine) Destroy() error {
	engine.db.Close()
	err := ratgo.DestroyDatabase(engine.config.DataPath, engine.dbOptions)
	if err != nil {
		return err
	}
	engine.dbOptions.Close()
	return nil
}
