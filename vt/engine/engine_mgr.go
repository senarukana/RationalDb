package engine

import (
	"errors"

	"github.com/senarukana/rationaldb/log"
	"github.com/senarukana/rationaldb/vt/engine/proto"
	"github.com/senarukana/rationaldb/vt/engine/rocksdb"
)

var (
	ErrDbInitError = errors.New("Init Db Engine Error")
)

type engineInitFunc func() proto.DbEngine

var engineImpls = make(map[string]engineInitFunc)

func Register(name string, engineInit engineInitFunc) {
	if engineInit == nil {
		panic("engine: register engine operation is nil")
	}
	if _, dup := engineImpls[name]; dup {
		panic("engine: register engine called twice")
	}
	engineImpls[name] = engineInit
}

type EngineManager struct {
	dbEngine proto.DbEngine
}

func NewEngineManager(name string) *EngineManager {
	em := new(EngineManager)
	if engineInit, ok := engineImpls[name]; ok {
		log.Info("Get Engine : %v", name)
		engine := engineInit()
		em.dbEngine = engine
		return em
	} else {
		panic("engine: unknown engine name")
	}
}

func (em *EngineManager) Init(conf *proto.DBConfigs) {
	if err := em.dbEngine.Init(conf); err != nil {
		panic(ErrDbInitError.Error())
	}
}

func (em *EngineManager) Shutdown() {
	em.dbEngine.Shutdown()
	em.dbEngine = nil
}

func (em *EngineManager) Destroy() {
	em.dbEngine.Shutdown()
}

func (em *EngineManager) Connect(params *proto.DbConnectParams) (proto.DbConnection, error) {
	return em.dbEngine.Connect(params)
}

func init() {
	Register("rocksdb", rocksdb.NewRocksDbEngine)
}
