package tabletserver

import (
	"errors"
	"fmt"

	"github.com/senarukana/rationaldb/vt/engine"
	"github.com/senarukana/rationaldb/vt/engine/proto"
)

type DbSessionParams struct {
	UName  string
	DbName string
}

type DbExecutor struct {
	sessionParams *RocksDbSessionParams
	engine        *proto.DbEngine
}

func NewDbExecutor(params *DbSessionParams, engine *proto.DbEngine) *DbExecutor {
	return &DbExecutor{sessionParams: params, engine: engine}
}

func (conn *DbExecutor) ExecuteFetch(query string, maxrows int, wantfields bool) (qr *proto.QueryResult, err error) {

}

func ExecuteStreamFetch(query string) (err error) {

}

func Fields() (fields []proto.Field) {

}

func FetchNext() (row []sqltypes.Value, err error) {

}

func CloseResult() {

}

/*type JsonObject interface {
	Json() string
}

func (engine *DbExecutor) SetData(dataKey []byte, dataValue interface{}, wo *ratgo.WriteOptions) (err error) {
	if v, ok := dataValue.(JsonObject); ok {
		err = engine.db.Put(wo, []byte(tableName), []byte(tableInfo.Json()))
	} else if v, ok := dataValue.([]byte); ok {
		err = engine.db.Put(wo, []byte(dataKey), v)
	} else {
		return errors.New("not supported type for Set data")
	}
	return nil
}

func (engine *DbExecutor) GetData(dataKey []byte, ro *ratgo.ReadOptions) (value interface{}, err error) {
	return engine.Get(ro, []byte(key))
}*/

func (engine *DbExecutor) insert(tableInfo *schema.Table, insertedRowValues []map[string]sqltypes.Value, sync bool) (err error) {
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
				batch.Put([]byte(key), []byte{'0'})
			} else {
				key = fmt.Sprintf("%s|%s|%s", tableName, columnName, pk)
				batch.Put([]byte(key), columnValue)
			}
		}
	}
	err = engine.db.Write(wo, batch)
	if err != nil {
		log.Error("Rocksdb Insert error, %v", err.Error())
		return err
	}
	return nil
}

func (engine *DbExecutor) delete(tableInfo *schema.Table, primaryKeys [][]byte, sync bool) (err error) {
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
				key = fmt.Sprintf("%s|%v", tableName, pk)
				batch.Delete([]byte(key))
			} else {
				key = fmt.Sprintf("%s|%s|%v", tableName, column.Name, pk)
				batch.Delete([]byte(key))
			}
		}
	}
	err = engine.db.Write(wo, batch)
	if err != nil {
		log.Error("Rocksdb Delete error, %v", err.Error())
		return nil, err
	}
	return qr, nil
}

func (engine *DbExecutor) update(tableInfo *schema.Table, primaryKeys [][]byte, updateValues map[string]sqltypes.Value, sync bool) (err error) {
	wo := ratgo.NewWriteOptions()
	defer wo.Close()
	wo.SetSync(sync)

	batch := ratgo.NewWriteBatch()
	defer batch.Close()

	tableName := tableInfo.Name
	var key string
	for _, pk := range primaryKeys {
		for columnName, value := range updateValues {
			key = fmt.Sprintf("%s|%s|%v", tableName, columnName, pk)
			batch.Put([]byte(key), value.Raw())
		}
	}
	err = engine.db.Write(wo, batch)
	if err != nil {
		log.Error("Rocksdb Update error, %v", err.Error())
		return err
	}
	return nil
}

func (engine *RocksDbEngine) fetch(tableInfo *schema.Table, primaryKeys []string, fields []string, ro *ratgo.ReadOptions) (qr *proto.QueryResult, err error) {
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
				row[j] = buildValue(result, column.Type)
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

func (engine *RocksDbEngine) fetchSub(tableInfo *schema.Table, selectValue []proto.FieldValue, ro *ratgo.ReadOptions) (primaryKeys []string, err error) {

}

func buildValue(bytes []byte, filedType uint32) sqltypes.Value {
	switch filedType {
	case schema.TYPE_FRACTIONAL:
		return sqltypes.MakeFractional(bytes)
	case schema.TYPE_NUMERIC:
		return sqltypes.MakeNumeric(bytes)
	case schema.TYPE_OTHER:
		return sqltypes.MakeString(bytes)
	}
}
