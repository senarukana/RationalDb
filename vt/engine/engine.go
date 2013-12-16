package engine

import (
	"errors"

	"github.com/senarukana/rationaldb/log"
	"github.com/senarukana/rationaldb/vt/engine/proto"
)

var (
	ErrDbInitError = errors.New("InitDb Error")
)

type DBConfigs struct {
	ServerId   uint32
	EngineName string
	DbName     string
	DataPath   string
	BinLogPath string

	*RocksDbConfigs
}

type engineInitFunc func(conf *DBConfigs) proto.DbEngine

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

func GetEngine(name string, conf *DBConfigs) proto.DbEngine {
	if engineInit, ok := engineImpls[name]; ok {
		log.Info("Get Engine : %v", name)
		return engineInit(conf)
	} else {
		panic("engine: unknown engine name")
	}
}
