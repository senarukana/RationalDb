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

func (engine *rocksDbEngine) Delete(tableInfo *schema.Table, primaryKeys []string, sync bool) (qr *proto.QueryResult, err error) {
	wo := ratgo.NewWriteOptions()
	defer wo.Close()
	wo.SetSync(sync)
	batch := ratgo.NewWriteBatch()
	defer batch.Close()

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

func (engine *rocksDbEngine) Update(tableInfo *schema.Table, primaryKeys []string, updateValues map[string]sqltypes.Value, sync bool) (qr *proto.QueryResult, err error) {
	wo := ratgo.NewWriteOptions()
	defer wo.Close()
	wo.SetSync(sync)

	batch := ratgo.NewWriteBatch()
	defer batch.Close()

	tableName := tableInfo.Name
	var key string
	for _, pk := range primaryKeys {
		for columnName, value := range updateValues {
			key = fmt.Sprintf("%s|%s|%s", tableName, columnName, pk)
			batch.Put(key, value)
		}
	}
	err = engine.db.Write(wo, batch)
	if err != nil {
		log.Error("Rocksdb Update error, %v", err.Error())
	}
	qr = new(proto.QueryResult)
	qr.RowsAffected = len(primaryKeys)
	return qr, nil
}

func (engine *rocksDbEngine) Select(tableInfo *schema.Table, primaryKeys []string, fields []string, ro *ratgo.ReadOptions) (qr *proto.QueryResult, err error) {
	tableName := tableInfo.Name
	// gather keys
	keys := make([]string, len(primaryKeys)*len(fields))
	for i, pk := range primaryKeys {
		for j, field := range fields {
			keys[i*len[primaryKeys]+j] = fmt.Sprintf("%s|%s|%s", tableName, field, pk)
		}
	}

	results, errors := engine.db.MultiGet(ro, keys)

	// if any errors occured, give up this result
	qr = new(proto.QueryResult)
	qr.RowsAffected = len(primaryKeys)
	qr.Rows = make([][]sqltypes.Value, len(primaryKeys))
	// gather results
	for i := range primaryKeys {
		row := qr.Rows[i]
		row = make([]sqltypes.Value, len(fields))
		for j, field := range fields {
			idx := i*len(fields) + j
			if errors[idx] != nil {
				return nil, err
			}
			result := results[idx]
			column := tableInfo.Columns[tableInfo.FindColumn(field)]
			// check if default value is set
			if result == nil {
				row[j] = column.Default
			} else {
				row[j] = BuildValue(result, column.Type)
			}
		}
	}

	qr.Fields = make(qr.Fields, len(fields))
	for i, field := range fields {
		columnIdx := tableInfo.FindColumn(field)
		if columnIdx == -1 {
			return nil, fmt.Errorf("Field %s doesn't exist in the Table:%s", field, tableName)
		}
		qr.Fields[i] = proto.Field{Name: field, Type: tableInfo.Columns[columnIdx].Type}
	}
	return qr, nil
}

func (engine *rocksDbEngine) SelectSub(tableInfo *schema.Table, selectValue []proto.FieldValue, ro *ratgo.ReadOptions) (primaryKeys []string, err error) {

}

func BuildValue(bytes []byte, filedType uint32) sqltypes.Value {
	switch filedType {
	case schema.TYPE_FRACTIONAL:
		return sqltypes.MakeFractional(bytes)
	case schema.TYPE_NUMERIC:
		return sqltypes.MakeNumeric(bytes)
	case schema.TYPE_OTHER:
		return sqltypes.MakeString(bytes)
	}
}
