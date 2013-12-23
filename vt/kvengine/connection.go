package kvengine

import (
	"github.com/senarukana/rationaldb/vt/kvengine/proto"
)

type DBConnection struct {
	connectionParams *proto.DbConnectParams
	proto.DbConnection
}

/*func buildValue(bytes []byte, filedType uint32) sqltypes.Value {
	switch filedType {
	case schema.TYPE_FRACTIONAL:
		return sqltypes.MakeFractional(bytes)
	case schema.TYPE_NUMERIC:
		return sqltypes.MakeNumeric(bytes)
	case schema.TYPE_OTHER:
		return sqltypes.MakeString(bytes)
	default:
		panic("Unknown field type")
	}
}*/

/*func (conn *Connection) buildTableyKey(tableName string) string {
	return fmt.Sprintf("%s|tables|%s", conn.connectionParams.DbName, tableName)
}

func (conn *Connection) buildTableDescriptionKey(tableName string) string {
	return fmt.Sprintf("%s|tables|%s|description", conn.connectionParams.DbName, tableName)
}

func (conn *Connection) buildTableIndexKey(tableName string) string {
	return fmt.Sprintf("%s|tables|%s|index", conn.connectionParams.DbName, tableName)
}

func (conn *Connection) buildTablesKey() string {
	return fmt.Sprintf("%s|tables|", conn.connectionParams.DbName)
}

func (conn *Connection) buildTableRowColumnKey(tableName string, columnName string, pk []byte) string {
	return fmt.Sprintf("%v|%v|%v|%v", conn.connectionParams.DbName, tableName, columnName, pk)
}*/

/*type JsonObject interface {
	Json() string
}

func (dbConnection *Connection) SetData(dataKey []byte, dataValue interface{}, wo *ratgo.WriteOptions) (err error) {
	if v, ok := dataValue.(JsonObject); ok {
		err = dbConnection.db.Put(wo, []byte(tableName), []byte(tableInfo.Json()))
	} else if v, ok := dataValue.([]byte); ok {
		err = dbConnection.db.Put(wo, []byte(dataKey), v)
	} else {
		return errors.New("not supported type for Set data")
	}
	return nil
}

func (dbConnection *Connection) GetData(dataKey []byte, ro *ratgo.ReadOptions) (value interface{}, err error) {
	return dbConnection.Get(ro, []byte(key))
}*/

/*func getPkValues(tableInfo *schema.Table, row map[string]sqltypes.Value) (pk string) {
	var buffer bytes.Buffer
	if len(tableInfo.PKColumns) == 1 {
		pkColumn := tableInfo.Columns[tableInfo.PKColumns[0]]
		if pkColumn.IsAuto {
			//TODO
			buffer.WriteString(strconv.FormatInt(tableInfo.Columns[tableInfo.PKColumns[0]].GetNextId(), 64))
		} else if pkColumn.IsUUID {

		} else {
			buffer.Write(row[pkColumn.Name].Raw())
		}
	} else {
		for i, pkIdx := range tableInfo.PKColumns {
			buffer.Write(row[tableInfo.Columns[pkIdx].Name].Raw())
			if i != len(tableInfo.PKColumns)-1 {
				buffer.WriteByte('|')
			}
		}
	}
	pk = buffer.String()
	return pk
}

func (conn *Connection) Id() int64 {
	return conn.dbConnection.Id()
}

func (conn *Connection) Close() {
	conn.dbConnection.Close()
	conn.dbConnection = nil
}

func (conn *Connection) IsClosed() bool {
	return conn.dbConnection == nil
}

//
// Basic Operation
//

func (conn *Connection) Get(key []byte, ro *proto.proto.DbReadOptions) (result []byte, err error) {
	return conn.dbConnection.Get(nil, key)
}

func (conn *Connection) GetList(key []byte, ro *proto.proto.DbReadOptions) (results [][]byte, err error) {
	return results, err
}

func (conn *Connection) Put(key, value []byte, wo *proto.proto.DbWriteOptions) (err error) {
	return conn.dbConnection.Put(nil, key, value)
}

func (conn *Connection) PutList(key, value []byte, wo *proto.proto.DbWriteOptions) (err error) {
	return err
}

//
// CRUD Operation
//

func (conn *Connection) Insert(tableInfo *schema.Table, insertedRowValues []map[string]sqltypes.Value) (qr *proto.QueryResult, err error) {
	tableName := tableInfo.Name
	var pk, key, columnName string
	keys := make([][]byte, 0, len(insertedRowValues)*len(tableInfo.Columns))
	values := make([][]byte, 0, len(insertedRowValues)*len(tableInfo.Columns))
	pkColumnName := tableInfo.GetPKColumn(0).Name
	for _, row := range insertedRowValues {
		pk = getPkValues(tableInfo, row)
		for _, columnDef := range tableInfo.Columns {
			if columnDef.IsPk {
				if columnDef.IsAuto {

				}
			}
			columnName = columnDef.Name
			if columnName == pkColumnName {
				if columnDef.IsAuto {

				} else if columnDef.IsUUID {

				} else {
					if row[columnName].IsNull() {
						return nil, ErrPkNotProvided
					}
					key = conn.buildTableRowColumnKey(tableName, columnName, row[columnName].Raw())
				}
				keys = append(keys, []byte(key))
				values = append(values, []byte{'0'})
			} else {
				if !row[columnName].IsNull() {
					key = conn.buildTableRowColumnKey(tableName, columnName, []byte(pk))
					keys = append(keys, []byte(key))
					values = append(values, row[columnName].Raw())
				}
			}
		}
	}

	err = conn.dbConnection.Puts(nil, keys, values)
	if err != nil {
		log.Error("Insert error, %v", err.Error())
		return nil, err
	}

	qr = &proto.QueryResult{RowsAffected: uint64(len(insertedRowValues))}
	return qr, nil
}

func (conn *Connection) Delete(tableInfo *schema.Table, primaryKeys [][]byte) (qr *proto.QueryResult, err error) {
	keys := make([][]byte, len(primaryKeys)*len(tableInfo.Columns))
	tableName := tableInfo.Name
	idx := 0
	for _, pk := range primaryKeys {
		for _, column := range tableInfo.Columns {
			keys[idx] = []byte(conn.buildTableRowColumnKey(tableName, column.Name, pk))
			idx++
		}
	}
	wo := new(proto.proto.DbWriteOptions)
	err = conn.dbConnection.Deletes(wo, keys)
	if err != nil {
		log.Error("Delete error, %v", err.Error())
		return nil, err
	}
	qr = &proto.QueryResult{RowsAffected: uint64(len(primaryKeys))}
	return qr, nil
}

func (conn *Connection) Update(tableInfo *schema.Table, primaryKeys [][]byte, updateValues map[string]sqltypes.Value) (qr *proto.QueryResult, err error) {
	keys := make([][]byte, len(primaryKeys)*len(updateValues))
	values := make([][]byte, len(primaryKeys)*len(updateValues))
	idx := 0
	tableName := tableInfo.Name
	for _, pk := range primaryKeys {
		for columnName, value := range updateValues {
			keys[idx] = []byte(conn.buildTableRowColumnKey(tableName, columnName, pk))
			values[idx] = value.Raw()
			idx++
		}
	}

	//TODO
	wo := new(proto.proto.DbWriteOptions)
	err = conn.dbConnection.Puts(wo, keys, values)
	if err != nil {
		log.Error("Rocksdb Update error, %v", err.Error())
		return nil, err
	}
	qr = &proto.QueryResult{RowsAffected: uint64(len(primaryKeys))}
	return qr, nil
}*/

/*func (conn *Connection) PkInFetch(tableInfo *schema.Table, primaryKeys [][]byte, fields []string, ro *proto.proto.DbReadOptions) (qr *proto.QueryResult, err error) {
	tableName := tableInfo.Name
	// gather keys
	keys := make([][]byte, len(primaryKeys)*len(fields))
	idx := 0
	for _, pk := range primaryKeys {
		for _, field := range fields {
			keys[idx] = []byte(conn.buildTableRowColumnKey(tableName, field, pk))
			idx++
		}
	}

	results, errors := conn.dbConnection.Gets(ro, keys)

	// if any errors occured, give up this result
	qr = new(proto.QueryResult)
	qr.RowsAffected = len(primaryKeys)
	qr.Rows = make([][]sqltypes.Value, len(primaryKeys))
	idx = 0
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

func (conn *Connection) PkNotInFetch(tableInfo *schema.Table, selectValue []proto.FieldValue, ro *proto.proto.DbReadOptions) (qr *proto.QueryResult, err error) {
	return
}
*/
/*
func (conn *Connection) Select(tableInfo *schema.Table, selectedValue []proto.FieldValue, ro *proto.proto.DbReadOptions) (qr *proto.QueryResult, err error) {
	return qr, err
}

func (conn *Connection) Fetch(tableInfo *schema.Table) (qr *proto.QueryResult, err error) {
	return qr, err
}
*/
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
/*func (conn *Connection) CreateTable(tableInfo *schema.Table) (err error) {
	tableKey := []byte(conn.buildTableyKey(tableInfo.Name))

	t, err := conn.dbConnection.Get(nil, tableKey)
	if err != nil {
		return err
	}
	if t != nil {
		return ErrTableExists(tableInfo.Name)
	}

	wo := new(proto.proto.DbWriteOptions)
	wo.Sync = true
	return conn.dbConnection.Put(wo, tableKey, []byte(tableInfo.Json()))
}

// table stored path is : dbname|tables|tableName
func (conn *Connection) ShowTables() (tables []*schema.Table, err error) {
	// we exepect table name should not contain the folloing 3 char '|,},Del'
	keyStart := conn.buildTablesKey()
	keyEnd := conn.buildTablesKey() + "|"
	iter, err := conn.dbConnection.Iterate(nil, []byte(keyStart), []byte(keyEnd))
	if err != nil {
		return nil, err
	}
	var tablesName [][]byte
	for ; iter.Valid(); iter.Next() {
		tablesName = append(tablesName, iter.Value())
	}
	if err = iter.Error(); err != nil {
		return nil, err
	}

	var tablesBytes [][]byte
	tablesBytes, err = conn.dbConnection.Gets(nil, tablesName)
	if err != nil {
		return nil, err
	}
	tables = make([]*schema.Table, len(tablesBytes))
	for _, tableBytes := range tablesBytes {
		table := new(schema.Table)
		err = json.Unmarshal(tableBytes, table)
		if err != nil {
			return nil, err
		}
		tables = append(tables, table)
	}

	return tables, err
}

// dbName|tables|tableName|description
func (conn *Connection) DescribeTable(tableName string) (tableInfo *schema.Table, err error) {
	tableBytes, err := conn.dbConnection.Get(nil, []byte(conn.buildTableDescriptionKey(tableName)))
	if err != nil {
		return nil, err
	}
	err = json.Unmarshal(tableBytes, tableInfo)
	if err != nil {
		return nil, err
	}
	return tableInfo, err
}

func (conn *Connection) ShowTableIndex(tableName string) (indexsInfo []*schema.Index, err error) {
	indexs, err := conn.dbConnection.Get(nil, []byte(conn.buildTableIndexKey(tableName)))
	if err != nil {
		return nil, err
	}
	err = json.Unmarshal(indexs, indexsInfo)
	if err != nil {
		return nil, err
	}
	return indexsInfo, nil
}
*/

type DBEngineError struct {
	Message string
}

func (err *DBEngineError) Error() string {
	return err.Message
}

func (conn *DBConnection) handlerError(err *error) {
	if x := recover(); x != nil {
		terr := x.(*DBEngineError)
		*err = terr
	}
}

func (conn *DBConnection) Close() {
	conn.DbConnection.Close()
	conn.DbConnection = nil
	conn.connectionParams = nil
}

func (conn *DBConnection) IsClosed() bool {
	return conn.DbConnection == nil
}

func (conn *DBConnection) Get(options *proto.DbReadOptions, key []byte) (data []byte, err error) {
	if conn.IsClosed() {
		return nil, &DBEngineError{Message: "Connection is closed"}
	}
	data, err = conn.DbConnection.Get(options, key)
	if err != nil {
		return nil, &DBEngineError{Message: err.Error()}
	}
	return data, nil
}

func (conn *DBConnection) Gets(options *proto.DbReadOptions, key [][]byte) (data [][]byte, err error) {
	if conn.IsClosed() {
		return nil, &DBEngineError{Message: "Connection is closed"}
	}
	data, err = conn.DbConnection.Gets(options, key)
	if err != nil {
		return nil, &DBEngineError{Message: err.Error()}
	}
	return data, nil
}

// Put the data in the engine.
// If the key is already existed, It will replace it.
func (conn *DBConnection) Put(options *proto.DbWriteOptions, key, value []byte) (err error) {
	if conn.IsClosed() {
		return &DBEngineError{Message: "Connection is closed"}
	}
	err = conn.DbConnection.Put(options, key, value)
	if err != nil {
		return &DBEngineError{Message: err.Error()}
	}
	return nil
}

func (conn *DBConnection) Puts(options *proto.DbWriteOptions, keys, values [][]byte) (err error) {
	if conn.IsClosed() {
		return &DBEngineError{Message: "Connection is closed"}
	}
	err = conn.DbConnection.Puts(options, keys, values)
	if err != nil {
		return &DBEngineError{Message: err.Error()}
	}
	return nil
}

func (conn *DBConnection) Delete(options *proto.DbWriteOptions, key []byte) (err error) {
	if conn.IsClosed() {
		return &DBEngineError{Message: "Connection is closed"}
	}
	err = conn.DbConnection.Delete(options, key)
	if err != nil {
		return &DBEngineError{Message: err.Error()}
	}
	return nil
}

func (conn *DBConnection) Deletes(options *proto.DbWriteOptions, keys [][]byte) (err error) {
	if conn.IsClosed() {
		return &DBEngineError{Message: "Connection is closed"}
	}
	err = conn.DbConnection.Deletes(options, keys)
	if err != nil {
		return &DBEngineError{Message: err.Error()}
	}
	return nil
}

func (conn *DBConnection) Iterate(options *proto.DbReadOptions, start []byte, end []byte, limit int) (cursor proto.DbCursor, err error) {
	if conn.IsClosed() {
		return nil, &DBEngineError{Message: "Connection is closed"}
	}
	cursor, err = conn.DbConnection.Iterate(options, start, end, limit)
	if err != nil {
		return nil, &DBEngineError{Message: err.Error()}
	}
	return cursor, nil
}

func (conn *DBConnection) Snapshot() (*proto.DbSnapshot, error) {
	if conn.IsClosed() {
		return nil, &DBEngineError{Message: "Connection is closed"}
	}
	snap, err := conn.DbConnection.Snapshot()
	if err != nil {
		return nil, &DBEngineError{Message: err.Error()}
	}
	return snap, nil
}

func (conn *DBConnection) ReleaseSnapshot(snap *proto.DbSnapshot) error {
	err := conn.DbConnection.ReleaseSnapshot(snap)
	if err != nil {
		return &DBEngineError{Message: err.Error()}
	}
	return nil
}
