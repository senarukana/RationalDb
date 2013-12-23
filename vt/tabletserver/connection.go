package tabletserver

import (
	"github.com/senarukana/rationaldb/vt/kvengine"
	eproto "github.com/senarukana/rationaldb/vt/kvengine/proto"
)

type PoolConnection interface {
	eproto.DbConnection
	Recycle()
}

type CreateConnectionFun func() (connection *DBConnection, err error)

type DBConnection struct {
	*kvengine.DBConnection
}

func NewConnection(params *eproto.DbConnectParams, engine *kvengine.Engine) (*DBConnection, error) {
	connection, err := engine.Connect(params)
	if err != nil {
		return nil, err
	}
	return &DBConnection{DBConnection: connection}, nil
}

func ConnectionCreator(params *eproto.DbConnectParams, engine *kvengine.Engine) CreateConnectionFun {
	return func() (connection *DBConnection, err error) {
		return NewConnection(params, engine)
	}
}
