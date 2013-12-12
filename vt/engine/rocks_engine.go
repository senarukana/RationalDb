package engine

import (
	"fmt"
	"github.com/senarukana/ratgo"
	"github.com/senarukana/rationaldb/engine/proto"
)

type RocksDBConfigs struct {
	DBConfigs

	CreateIfMissing bool
	ParanoidCheck   bool
	LRUCacheSize    int
}

type RocksDbError struct {
	Num     int
	Message string
	Query   string
}

func NewRocksDbError(number int, format string, args ...interface{}) *RocksDbError {
	return &RocksDbError{Num: number, Message: fmt.Sprintf(format, args...)}
}

func (se *RocksDbError) Error() string {
	if se.Query == "" {
		return fmt.Sprintf("%v (errno %v)", se.Message, se.Num)
	}
	return fmt.Sprintf("%v (errno %v) during query: %s", se.Message, se.Num, se.Query)
}

func (se *RocksDbError) Number() int {
	return se.Num
}

func handleError(err *error) {
	if x := recover(); x != nil {
		terr := x.(*RocksDbError)
		*err = terr
	}
}

type rocksDbEngine struct {
	config RocksDBConfigs
	db     *ratgo.DB
}

var rocksEngine *rocksDbEngine

func Init(config *RocksDBConfigs) {
	rocksEngine.config = config

	options := ratgo.NewOptions()
	options.SetCreateIfMissing(config.CreateIfMissing)
	options.SetParanoidChecks(config.ParanoidCheck)
	options.SetCache(ratgo.NewLRUCache(config.LRUCacheSize))

	db, err := ratgo.Open(config.DbName, options)
	if err != nil {
		panic(fmt.Sprintf("open rocksdb:%s failed, err %v", config.DbName, err))
	}
	rocksEngine.db = db
}

func (rocksDbEngine *engine) Insert(dbName, tableName string, primaryKeyIndex int, insertedValue [][]proto.FieldValue) (qr *proto.QueryResult, err error) {

}
