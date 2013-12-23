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

func GetEngine(conf *proto.DBConfigs) proto.DbEngine {
	if engineInit, ok := engineImpls[conf.EngineName]; ok {
		log.Info("Get Engine : %v", conf.EngineName)
		return engineInit(conf)
	} else {
		panic("engine: unknown engine name")
	}
}
