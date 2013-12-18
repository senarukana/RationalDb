package tabletserver

import (
	"encoding/json"
	"fmt"

	"github.com/senarukana/rationaldb/schema"
	"github.com/senarukana/rationaldb/sqltypes"
	"github.com/senarukana/rationaldb/util/sync2"
	"github.com/senarukana/rationaldb/vt/engine"
	eproto "github.com/senarukana/rationaldb/vt/engine/proto"
	"github.com/senarukana/rationaldb/vt/tabletserver/proto"
)

type ErrTableExists string

func (self *ErrTableExists) Error() string { return "Table : " + string(self) + " exists" }

type KVEngineExecutor struct {
	connectionParams *eproto.DbConnectParams
	dbConnection     eproto.DbConnection
}

func NewKVEngineExecutor(params *eproto.DbConnectParams, manger *engine.EngineManager) (*KVExecutorPoolConnection, error) {
	connection, err := manger.Connect(params)
	if err != nil {
		return nil, err
	}
	return &KVEngineExecutor{connectionParams: params, dbConnection: connection}, nil
}

func KVExecutorCreator(params *eproto.DbConnectParams, manager *engine.EngineManager) {
	return func() (connection *KVEngineExecutor, err error) {
		return NewKVEngineExecutor(params, manager)
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

func (ee *KVEngineExecutor) buildTableyKey(tableName string) string {
	return fmt.Sprintf("%s|tables|%s", ee.connectionParams.DbName, tableName)
}

func (ee *KVEngineExecutor) buildTableDescriptionKey(tableName string) string {
	return fmt.Sprintf("%s|tables|%s|description", ee.connectionParams.DbName, tableName)
}

func (ee *KVEngineExecutor) buildTablesKey() string {
	return fmt.Sprintf("%s|tables|", ee.connectionParams.DbName)
}

func (ee *KVEngineExecutor) buildTableRowKey(tableName string) string {
	return fmt.Sprintf("%s|%s|", ee.connectionParams.DbName, tableName)
}

func (ee *KVEngineExecutor) buildTableRowPkColumnKey(tableName string, columnName string) {
	return fmt.Sprintf("%v|%v|%v", ee.connectionParams.DbName, tableName, columnName)
}

func (ee *KVEngineExecutor) buildTableRowColumnKey(tableName string, columnName string, pk []byte) {
	return fmt.Sprintf("%v|%v|%v|%v", ee.connectionParams.DbName, tableName, columnName, pk)
}

/*type JsonObject interface {
	Json() string
}

func (engine *KVEngineExecutor) SetData(dataKey []byte, dataValue interface{}, wo *ratgo.WriteOptions) (err error) {
	if v, ok := dataValue.(JsonObject); ok {
		err = engine.db.Put(wo, []byte(tableName), []byte(tableInfo.Json()))
	} else if v, ok := dataValue.([]byte); ok {
		err = engine.db.Put(wo, []byte(dataKey), v)
	} else {
		return errors.New("not supported type for Set data")
	}
	return nil
}

func (engine *KVEngineExecutor) GetData(dataKey []byte, ro *ratgo.ReadOptions) (value interface{}, err error) {
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

func (ee *KVEngineExecutor) Id() int64 {
	return ee.dbConnection.Id()
}

func (ee *KVEngineExecutor) Close() {
	ee.dbConnection.Close()
	ee.dbConnection = nil
}

func (ee *KVEngineExecutor) IsClosed() {
	return ee.dbConnection == nil
}

//
// Basic Operation
//

func (ee *KVEngineExecutor) Get(key []byte) ([]byte, error) {
	return ee.engine.Get(nil, key)
}

func (ee *KVEngineExecutor) Put(key, value []byte) error {
	return ee.engine.Put(nil, key, value)
}

//
// CRUD Operation
//

func (ee *KVEngineExecutor) Insert(tableInfo *schema.Table, insertedRowValues []map[string]sqltypes.Value, sync bool) (err error) {
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
				key = ee.buildTableRowPkColumnKey(tableName, columnName)
				values[idx] = []byte{'0'}
				keys[idx] = []byte(key)
			} else {
				key = ee.buildTableRowColumnKey(tableName, columnName, []byte(pk))
				values[idx] = columnValue.Raw()
				keys[idx] = key
			}
			idx++
		}
	}

	wo := new(proto.DbWriteOptions)
	wo.Sync = sync
	err = ee.engine.Puts(wo, keys, values)
	if err != nil {
		log.Error("Rocksdb Insert error, %v", err.Error())
		return err
	}
	return nil
}

func (ee *KVEngineExecutor) Delete(tableInfo *schema.Table, primaryKeys [][]byte, sync bool) (err error) {
	keys := make([][]byte, len(primaryKeys)*len(tableInfo.Columns))
	tableName := tableInfo.Name
	var key string
	idx := 0
	for _, pk := range primaryKeys {
		for _, column := range tableInfo.Columns {
			keys[idx] = ee.buildTableRowColumnKey(tableName, column.Name, pk)
			idx++
		}
	}
	wo := new(proto.DbWriteOptions)
	wo.Sync = sync
	err = ee.engine.Deletes(wo, keys)
	if err != nil {
		log.Error("Rocksdb Delete error, %v", err.Error())
		return err
	}
	return nil
}

func (ee *KVEngineExecutor) Update(tableInfo *schema.Table, primaryKeys [][]byte, updateValues map[string]sqltypes.Value, sync bool) (err error) {
	keys := make([][]byte, len(primaryKeys)*len(updateValues))
	values := make([][]byte, len(primaryKeys)*len(updateValues))
	idx := 0
	tableName := tableInfo.Name
	var key string
	for _, pk := range primaryKeys {
		for columnName, value := range updateValues {
			keys[idx] = ee.buildTableRowColumnKey(tableName, columnName, pk)
			values[idx] = value.Raw()
			idx++
		}
	}
	wo := new(proto.DbWriteOptions)
	wo.Sync = sync
	err = ee.engine.Set(wo, keys, values)
	if err != nil {
		log.Error("Rocksdb Update error, %v", err.Error())
		return err
	}
	return nil
}

func (ee *KVEngineExecutor) PkInFetch(tableInfo *schema.Table, primaryKeys [][]byte, fields []string, ro *eproto.DbReadOptions) (qr *eproto.QueryResult, err error) {
	tableName := tableInfo.Name
	// gather keys
	keys := make([]string, len(primaryKeys)*len(fields))
	for i, pk := range primaryKeys {
		for j, field := range fields {
			keys[i*len[primaryKeys]+j] = ee.buildTableRowColumnKey(tableName, field, pk)
		}
	}

	results, errors := ee.engine.Gets(ro, keys)

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
		qr.Fields[i] = eproto.Field{Name: field, Type: tableInfo.Columns[columnIdx].Type}
	}
	return qr, nil
}

func (ee *KVEngineExecutor) PkNotInFetch(tableInfo *schema.Table, selectValue []eproto.FieldValue, ro *eproto.DbReadOptions) (primaryKeys [][]byte, err error) {
	return
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
func (ee *KVEngineExecutor) CreateTable(tableInfo *schema.Table) (err error) {
	tableKey := ee.buildTableyKey(tableInfo.Name)

	t, err := ee.engine.Get(nil, tableKey)
	if err != nil {
		return err
	}
	if t != nil {
		return ErrTableExists(tableInfo.Name)
	}

	wo := new(proto.DbWriteOptions)
	wo.Sync = true
	return ee.engine.Set(wo, tableKey, []byte(tableInfo.Json()))
}

// table stored path is : dbname|tables|tableName
func (ee *KVEngineExecutor) ShowTables() (tables []*schema.Table, err error) {
	// we exepect table name should not contain the folloing 3 char '|,},Del'
	keyStart := ee.buildTablesKey()
	keyEnd := ee.buildTablesKey() + "|"
	iter := ee.engine.Iterate(nil, keyStart, keyEnd)
	var tablesName [][]byte
	for ; iter.Valid(); iter.Next() {
		tablesName = append(tablesName, string(iter.Value()))
	}
	if err = iter.Error(); err != nil {
		return nil, err
	}

	tablesBytes, err := ee.engine.Gets(nil, tablesName)
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
func (ee *KVEngineExecutor) ShowTable(tableName string) (tableInfo *schema.Table, err error) {
	tableByte, err := ee.engine.Get(nil, ee.buildTableDescriptionKey(tableName))
	if err != nil {
		return nil, err
	}
	err = json.Unmarshal(tableByte, tableInfo)
	if err != nil {
		return nil, err
	}
	return
}
