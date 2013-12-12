package engine

import (
	"fmt"

	"github.com/senarukana/ratgo"
	"github.com/senarukana/rationaldb/engine/proto"
	"github.com/senarukana/rationaldb/log"
	"github.com/senarukana/rationaldb/schema"
	"github.com/senarukana/rationaldb/sqltypes"
)

type RocksDBConfigs struct {
	DBConfigs

	CreateIfMissing bool
	ParanoidCheck   bool
	LRUCacheSize    int
}

type RocksDbError struct {
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

func (engine *rocksDbEngine) Insert(tableInfo *schema.Table, insertedRowValues []map[string]sqltypes.Value, sync bool) (qr *proto.QueryResult, err error) {
	wo := ratgo.NewWriteOptions()
	wo.SetSync(sync)
	batch := ratgo.NewWriteBatch()
	tableName := tableInfo.Name
	var key string
	for i := range insertedRowValues {
		row := insertedRowValues[i]
		var pk string
		pkColumn := tableInfo.GetPKColumn()
		if pkColumn.IsAuto {
			pk = string(tableInfo.Columns[tableInfo.PKColumn].GetNextIncrementalID())
		} else if pkColumn.IsUUID {

		} else {
			// we've already checked that pk in in the row
			pk = row[pkColumn.Name].String()
		}
		for columnName, columnValue := range row {
			if columnName == pkColumn.Name {
				// primary key, we just put a fake 0 as its value
				key = fmt.Sprintf("%s|%s", tableName, pk)
				batch.Put(key, []byte{'0'})
			} else {
				key = fmt.Sprintf("%s|%s|%s", tableName, columnName, pk)
				batch.Put(key, columnValue)
			}
		}
	}
	err = engine.db.Write(wo, batch)
	if err != nil {
		log.Error("Rocksdb Insert error, %v", err.Error())
		return nil, err
	}
	qr = new(proto.QueryResult)
	qr.RowsAffected = len(insertedValue)
	return qr, nil
}

func (engine *rocksDbEngine) Delete(tableInfo *schema.Table, primaryKeys []string, sync bool) {
	wo := ratgo.NewWriteOptions()
	wo.SetSync(sync)
	batch := ratgo.NewWriteBatch()
	tableName := tableInfo.Name
	var key string
	for _, pk := range primaryKeys {
		for _, column := range tableInfo.Columns {
			if column == tableInfo.GetPKColumn() {
				key = fmt.Sprintf("%s|%s", tableName, pk)
				batch.Delete(key)
			} else {
				key = fmt.Sprintf("%s|%s|%s", tableName, column.Name, pk)
				batch.Delete(key)
			}
		}
	}
	err = engine.db.Write(wo, batch)
	if err != nil {
		log.Error("Rocksdb Delete error, %v", err.Error())
		return nil, err
	}
	qr = new(proto.QueryResult)
	qr.RowsAffected = len(insertedValue)
	return qr, nil
}
