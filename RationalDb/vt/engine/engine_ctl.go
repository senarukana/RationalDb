package engine

import (
	"errors"

	"github.com/senarukana/rationaldb/log"
	"github.com/senarukana/rationaldb/vt/engine/proto"
)

var (
	ErrDbInitError = errors.New("InitDb Error")
)

type engineInitFunc func(conf *proto.DBConfigs) proto.DbEngine

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
	dbEngine    *proto.DbEngine
	Connections int64
}

func (em *EngineManager) InitEngine(conf *proto.DBConfigs) proto.DbEngine {
	if engineInit, ok := engineImpls[conf.EngineName]; ok {
		log.Info("Get Engine : %v", conf.EngineName)
		engine := engineInit(conf)
		if err := engine.Init(); err != nil {
			panic(ErrDbInitError.Error())
		}

	} else {
		panic("engine: unknown engine name")
	}
}

func (em *EngineManager) ShutdownEngine() {
	em.dbEngine.Shutdown()
	em.dbEngine = nil
}

func (em *EngineManager) DestroyEngine() {

}
