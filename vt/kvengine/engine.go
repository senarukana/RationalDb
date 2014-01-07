package kvengine

import (
	"github.com/senarukana/rationaldb/log"
	"github.com/senarukana/rationaldb/vt/kvengine/leveldb"
	"github.com/senarukana/rationaldb/vt/kvengine/proto"
	"github.com/senarukana/rationaldb/vt/kvengine/rocksdb"
)

type engineInitFunc func() proto.DbEngine

var engineImpls = make(map[string]engineInitFunc)

func Register(name string, engineInit engineInitFunc) {
	engineImpls[name] = engineInit
}

type Engine struct {
	dbEngine proto.DbEngine
}

func NewEngine(name string) (*Engine, error) {
	em := new(Engine)
	if engineInit, ok := engineImpls[name]; ok {
		log.Info("Get Engine : %v", name)
		engine := engineInit()
		em.dbEngine = engine
		return em, nil
	} else {
		return nil, proto.ErrUnknownDBEngineName
	}
}

func (em *Engine) Init(conf *proto.DBConfigs) error {
	log.Info("Begin init engine")
	if err := em.dbEngine.Init(conf); err != nil {
		log.Info("Init engine %v error, %v", em.dbEngine.Name(), err)
		return proto.ErrDbInitError
	}
	log.Info("Init engine %v complete", em.dbEngine.Name())
	return nil
}

func (em *Engine) Shutdown() error {
	em.dbEngine.Shutdown()
	em.dbEngine = nil
	return nil
}

func (em *Engine) Destroy() error {
	return em.dbEngine.Destroy()
}

func (em *Engine) Connect(params *proto.DbConnectParams) (conn *DBConnection, err error) {
	var c proto.DbConnection
	c, err = em.dbEngine.Connect(params)
	log.Info("Come to a new client %v, id is %d", params.UserName, c.Id())
	if conn != nil && err != nil {
		conn.Close()
		return nil, err
	}
	conn = &DBConnection{connectionParams: params, DbConnection: c}
	return conn, nil
}

func init() {
	Register("rocksdb", rocksdb.NewRocksDbEngine)
	Register("leveldb", leveldb.NewLevelDbEngine)
}
