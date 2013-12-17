package tabletserver

import (
	"encoding/json"
	"errors"
	"fmt"

	"github.com/senarukana/rationaldb/schema"
	"github.com/senarukana/rationaldb/sqltypes"
	"github.com/senarukana/rationaldb/vt/engine"
	eproto "github.com/senarukana/rationaldb/vt/engine/proto"
	"github.com/senarukana/rationaldb/vt/tabletserver/proto"
)

type ErrTableExists string

func (self *ErrTableExists) Error() string { return "Table : " + string(self) + " exists" }

type KVEngineConnector struct {
	connectionParams *eproto.DbConnectParams
	engine           eproto.DbEngine
}

func NewKVEngineConnector(params *eproto.DbConnectParams, engine eproto.DbEngine) *KVEngineConnector {
	return &KVEngineConnector{connectionParams: params, engine: engine}
}

func KVEngineConnectionCreator(params *eproto.DbConnectParams, engine eproto.DbEngine) {
	return func() (connection *proto.KVEngineConnection, err error) {
		return NewKVEngineConnector(params, engine)
	}
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

func (executor *KVEngineConnector) getTableKey(tableName string) string {
	return fmt.Sprintf("%s|tables|%s", executor.connectionParams.DbName, tableName)
}

func (executor *KVEngineConnector) getTableDescriptionKey(tableName string) string {
	return fmt.Sprintf("%s|tables|%s|description", executor.connectionParams.DbName, tableName)
}

func (executor *KVEngineConnector) getTablesKey() string {
	return fmt.Sprintf("%s|tables|", executor.connectionParams.DbName)
}

func (executor *KVEngineConnector) getTableRowKey(tableName string) string {
	return fmt.Sprintf("%s|%s|", executor.connectionParams.DbName, tableName)
}

func (executor *KVEngineConnector) getTableRowPkColumnKey(tableName string, columnName string) {
	return fmt.Sprintf("%v|%v|%v", executor.connectionParams.DbName, tableName, columnName)
}

func (executor *KVEngineConnector) getTableRowColumnKey(tableName string, columnName string, pk []byte) {
	return fmt.Sprintf("%v|%v|%v|%v", executor.connectionParams.DbName, tableName, columnName, pk)
}

/*type JsonObject interface {
	Json() string
}

func (engine *KVEngineConnector) SetData(dataKey []byte, dataValue interface{}, wo *ratgo.WriteOptions) (err error) {
	if v, ok := dataValue.(JsonObject); ok {
		err = engine.db.Put(wo, []byte(tableName), []byte(tableInfo.Json()))
	} else if v, ok := dataValue.([]byte); ok {
		err = engine.db.Put(wo, []byte(dataKey), v)
	} else {
		return errors.New("not supported type for Set data")
	}
	return nil
}

func (engine *KVEngineConnector) GetData(dataKey []byte, ro *ratgo.ReadOptions) (value interface{}, err error) {
	return engine.Get(ro, []byte(key))
}*/

func getPkValues(tableInfo *schema.Table, row map[string]sqltypes.Value) (pk string) {
	if len(tableInfo.PKColumns) == 1 {
		pkColumn := tableInfo.Columns[tableInfo.PKColumns[0]]
		if pkColumn.IsAuto {
			//TODO
			pk = string(tableInfo.Columns[tableInfo.PKColumn].GetNextIncrementalID())
		} else if pkColumn.IsUUID {

		} else {
			pk = row[pkColumn.Name].String()
		}
	} else {
		for i, pkIdx := range tableInfo.PKColumns {
			pk += row[tableInfo.Columns[pkIdx].Name].String()
			if i != len(tableInfo.PKColumns)-1 {
				pk += "|"
			}
		}
	}
	return pk
}

//
// Basic Operation
//

func (executor *KVEngineConnector) Get(key []byte) ([]byte, error) {
	return executor.engine.Get(nil, key)
}

func (executor *KVEngineConnector) Put(key, value []byte) error {
	return executor.engine.Put(nil, key, value)
}

//
// CRUD Operation
//

func (executor *KVEngineConnector) Insert(tableInfo *schema.Table, insertedRowValues []map[string]sqltypes.Value, sync bool) (err error) {
	tableName := tableInfo.Name
	var pk, key string
	keys := make([][]byte, len(insertedRowValues)*len(tableInfo.Columns))
	values := make([][]byte, len(insertedRowValues)*len(tableInfo.Columns))
	idx := 0
	for _, row := range insertedRowValues {
		pk = getPkValues(tableInfo, row)
		for i, columnDef := range tableInfo.Columns {
			/*if columnDef.IsPk {
				if columnDef.IsAuto {

				}
			}*/
			if columnName == pkColumnName {
				key = executor.getTableRowPkColumnKey(tableName, columnName)
				values[idx] = []byte{'0'}
				keys[idx] = []byte(key)
			} else {
				key = executor.getTableRowColumnKey(tableName, columnName, []byte(pk))
				values[idx] = columnValue.Raw()
				keys[idx] = key
			}
			idx++
		}
	}

	wo := new(proto.DbWriteOptions)
	wo.Sync = sync
	err = executor.engine.Puts(wo, keys, values)
	if err != nil {
		log.Error("Rocksdb Insert error, %v", err.Error())
		return err
	}
	return nil
}

func (executor *KVEngineConnector) Delete(tableInfo *schema.Table, primaryKeys [][]byte, sync bool) (err error) {
	keys := make([][]byte, len(primaryKeys)*len(tableInfo.Columns))
	tableName := tableInfo.Name
	var key string
	idx := 0
	for _, pk := range primaryKeys {
		for _, column := range tableInfo.Columns {
			keys[idx] = executor.getTableRowColumnKey(tableName, column.Name, pk)
			idx++
		}
	}
	wo := new(proto.DbWriteOptions)
	wo.Sync = sync
	err = executor.engine.Deletes(wo, keys)
	if err != nil {
		log.Error("Rocksdb Delete error, %v", err.Error())
		return err
	}
	return nil
}

func (executor *KVEngineConnector) Update(tableInfo *schema.Table, primaryKeys [][]byte, updateValues map[string]sqltypes.Value, sync bool) (err error) {
	keys := make([][]byte, len(primaryKeys)*len(updateValues))
	values := make([][]byte, len(primaryKeys)*len(updateValues))
	idx := 0
	tableName := tableInfo.Name
	var key string
	for _, pk := range primaryKeys {
		for columnName, value := range updateValues {
			keys[idx] = executor.getTableRowColumnKey(tableName, columnName, pk)
			values[idx] = value.Raw()
			idx++
		}
	}
	wo := new(proto.DbWriteOptions)
	wo.Sync = sync
	err = executor.engine.Set(wo, keys, values)
	if err != nil {
		log.Error("Rocksdb Update error, %v", err.Error())
		return err
	}
	return nil
}

func (executor *KVEngineConnector) PkInFetch(tableInfo *schema.Table, primaryKeys [][]byte, fields []string, ro *ratgo.ReadOptions) (qr *proto.QueryResult, err error) {
	tableName := tableInfo.Name
	// gather keys
	keys := make([]string, len(primaryKeys)*len(fields))
	for i, pk := range primaryKeys {
		for j, field := range fields {
			keys[i*len[primaryKeys]+j] = executor.getTableRowColumnKey(tableName, field, pk)
		}
	}

	results, errors := engine.db.MultiGet(ro, keys)

	// if any errors occured, give up this result
	qr = new(proto.QueryResult)
	qr.RowsAffected = len(primaryKeys)
	qr.Rows = make([][]sqltypes.Value, len(primaryKeys))
	idx := 0
	// gather results
	for i := range primaryKeys {
		row := qr.Rows[i]
		row = make([]sqltypes.Value, len(fields))
		for j, field := range fields {
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
			idx++
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

func (executor *KVEngineConnector) PkNotInFetch(tableInfo *schema.Table, selectValue []proto.FieldValue, ro *ratgo.ReadOptions) (primaryKeys []string, err error) {

}

//
// Table
//

/*
 * create table user (
 * name varchar(20) primary key,
 * email varchar(40) not null)
 */

// check if the table exists first.
// If not then create table, otherwise return error
func (executor *KVEngineConnector) CreateTable(tableInfo *schema.Table) (err error) {
	tableKey := executor.getTableKey(tableInfo.Name)

	t, err := executor.engine.Get(nil, tableKey)
	if err != nil {
		return err
	}
	if t != nil {
		return ErrTableExists(tableInfo.Name)
	}

	wo := new(proto.DbWriteOptions)
	wo.Sync = true
	return executor.engine.Set(wo, tableKey, []byte(tableInfo.Json()))
}

// table stored path is : dbname|tables|tableName
func (executor *KVEngineConnector) ShowTables() (tables []*schema.Table, err error) {
	// we exepect table name should not contain the folloing 3 char '|,},Del'
	keyStart := executor.getTablesKey()
	keyEnd := executor.getTablesKey() + "|"
	iter := executor.engine.Iterate(nil, keyStart, keyEnd)
	var tablesName [][]byte
	for ; iter.Valid(); iter.Next() {
		tablesName = append(tablesName, string(iter.Value()))
	}
	if err = iter.Error(); err != nil {
		return nil, err
	}

	tablesBytes, err := executor.engine.Gets(nil, tablesName)
	if err != nil {
		return err
	}
	for _, tableBytes := range tablesBytes {
		table := new(schema.Table)
		err = json.Unmarshal(tableByte, tableInfo)
		if err != nil {
			return nil, err
		}
		tables = append(tables, table)
	}

	return
}

// dbName|tables|tableName|description
func (executor *KVEngineConnector) ShowTable(tableName string) (tableInfo *schema.Table, err error) {
	tableByte, err := executor.engine.Get(nil, executor.getTableDescriptionKey(tableName))
	if err != nil {
		return nil, err
	}
	err = json.Unmarshal(tableByte, tableInfo)
	if err != nil {
		return nil, err
	}
	return
}
