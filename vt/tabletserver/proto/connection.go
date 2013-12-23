package proto

import (
	"github.com/senarukana/rationaldb/schema"
	"github.com/senarukana/rationaldb/sqltypes"
	"github.com/senarukana/rationaldb/vt/kvengine/proto"
)

type PoolConnection interface {
	// Connection Operation
	Id() int64
	Close()
	IsClosed() bool
	Recycle()

	// Basic Operation
	Get(key []byte) ([]byte, error)
	GetList(key []byte) ([][]byte, error)
	Put(key, value []byte) error
	PutList(key, value []byte) error

	// CRUD Operation
	Insert(tableInfo *schema.Table, insertedRowValues []map[string]sqltypes.Value, sync bool) (err error)
	InsertSubquery() (err error)
	Delete(tableInfo *schema.Table, primaryKeys [][]byte, sync bool) (err error)
	Update(tableInfo *schema.Table, primaryKeys [][]byte, updateValues map[string]sqltypes.Value, sync bool) (err error)
	PkInFetch(tableInfo *schema.Table, primaryKeys [][]byte, fields []string, ro *proto.DbReadOptions) (qr *proto.QueryResult, err error)
	PkNotInFetch(tableInfo *schema.Table, selectValue []proto.FieldValue, ro *proto.DbReadOptions) (primaryKeys [][]byte, err error)
}

type CreateConnectionFun func() (connection PoolConnection, err error)
