package engine

import (
	"fmt"
	"github.com/senarukana/ratgo"
)

type RocksDbConnectionParams struct {
	UName  string
	DbName string
}

type RocksDbConnection struct {
	connectionParams RocksDbConnectionParams
	*ratgo.DB
}

func NewRocksDbConnection(params RocksDbConnectionParams) *RocksDbConnection {
	return &RocksDbConnection{connectionParams: params, DB: rocksDbEngine.db}
}

func (conn *RocksDbConnection) ExecuteFetch(query string, maxrows int, wantfields bool) {}
