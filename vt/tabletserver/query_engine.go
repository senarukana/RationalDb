// Copyright 2012, Google Inc. All rights reserved.
// Use of this source code is governed by a BSD-style
// license that can be found in the LICENSE file.

package tabletserver

import (
	// "bytes"
	"fmt"
	"strconv"
	"sync"
	"sync/atomic"
	"time"

	"github.com/senarukana/rationaldb/log"
	// "github.com/senarukana/rationaldb/schema"
	"github.com/senarukana/rationaldb/sqltypes"
	// "github.com/senarukana/rationaldb/util/hack"
	"github.com/senarukana/rationaldb/util/stats"
	"github.com/senarukana/rationaldb/util/sync2"
	"github.com/senarukana/rationaldb/vt/kvengine"
	eproto "github.com/senarukana/rationaldb/vt/kvengine/proto"
	"github.com/senarukana/rationaldb/vt/sqlparser"
	"github.com/senarukana/rationaldb/vt/tabletserver/proto"

	"github.com/nu7hatch/gouuid"
)

const (
	MAX_RESULT_NAME = "_vtMaxResultSize"

	// SPOT_CHECK_MULTIPLIER determines the precision of the
	// spot check ratio: 1e6 == 6 digits
	SPOT_CHECK_MULTIPLIER = 1e6
)

//-----------------------------------------------
type QueryEngine struct {
	// Obtain read lock on mu to execute queries
	// Obtain write lock to start/stop query service
	mu sync.RWMutex

	engine         *kvengine.Engine
	connPool       *ConnectionPool
	streamConnPool *ConnectionPool
	reservedPool   *ReservedPool
	schemaInfo     *SchemaInfo
	consolidator   *Consolidator

	maxResultSize    sync2.AtomicInt64
	streamBufferSize sync2.AtomicInt64
	activeConnection int64
}

type CompiledPlan struct {
	Query string
	*ExecPlan
	BindVars      map[string]interface{}
	TransactionId int64
	ConnectionId  int64
}

// stats are globals to allow anybody to set them
var (
	queryStats, waitStats *stats.Timings
	errorStats            *stats.Counters
	resultStats           *stats.Histogram
	spotCheckCount        *stats.Int
	QPSRates              *stats.Rates
)

var resultBuckets = []int64{0, 1, 5, 10, 50, 100, 500, 1000, 5000, 10000}

// CacheInvalidator provides the abstraction needed for an instant invalidation
// vs. delayed invalidation in the case of in-transaction dmls
type CacheInvalidator interface {
	Delete(key string) bool
}

func NewQueryEngine(config Config) *QueryEngine {
	var err error
	qe := &QueryEngine{}
	qe.engine, err = kvengine.NewEngine(config.EngineName)
	if err != nil {
		panic(NewTabletErrorDB(FATAL, err))
	}
	qe.connPool = NewConnectionPool("ConnPool", config.PoolSize, time.Duration(config.IdleTimeout*1e9))
	qe.schemaInfo = NewSchemaInfo(config.QueryCacheSize, time.Duration(config.IdleTimeout*1e9))
	qe.consolidator = NewConsolidator()
	qe.maxResultSize = sync2.AtomicInt64(config.MaxResultSize)
	qe.streamBufferSize = sync2.AtomicInt64(config.StreamBufferSize)
	stats.Publish("MaxResultSize", stats.IntFunc(qe.maxResultSize.Get))
	stats.Publish("StreamBufferSize", stats.IntFunc(qe.streamBufferSize.Get))
	queryStats = stats.NewTimings("Queries")
	QPSRates = stats.NewRates("QPS", queryStats, 15, 60*time.Second)
	waitStats = stats.NewTimings("Waits")
	errorStats = stats.NewCounters("Errors")
	resultStats = stats.NewHistogram("Results", resultBuckets)
	return qe
}

func (qe *QueryEngine) Open(config *eproto.DBConfigs) {
	// Wait for Close, in case it's running
	qe.mu.Lock()
	defer qe.mu.Unlock()
	err := qe.engine.Init(config)
	if err != nil {
		log.Info(err.Error())
		panic(NewTabletErrorDB(FATAL, err))
	}
	connFactory := ConnectionCreator(config.AppConnectParams, qe.engine)
	// log.Info("fxxx")
	qe.connPool.Open(connFactory)
	// qe.streamConnPool.Open(connFactory)
	// qe.reservedPool.Open(connFactory)
	start := time.Now().UnixNano()
	qe.schemaInfo.Open(connFactory)
	log.Info("Time taken to load the schema: %v ms", (time.Now().UnixNano()-start)/1e6)
}

func (qe *QueryEngine) Close() {
	// Ensure all read locks are released (no more queries being served)
	qe.mu.Lock()
	defer qe.mu.Unlock()
	qe.connPool.Close()
	qe.streamConnPool.Close()
	qe.reservedPool.Close()

	qe.schemaInfo.Close()
}

func (qe *QueryEngine) Begin(logStats *sqlQueryStats, connectionId int64) (transactionId int64) {
	return 0
}

func (qe *QueryEngine) Commit(logStats *sqlQueryStats, transactionId int64) {

}

func (qe *QueryEngine) Rollback(logStats *sqlQueryStats, transactionId int64) {

}

func (qe *QueryEngine) Execute(logStats *sqlQueryStats, query *proto.Query) (reply *eproto.QueryResult) {
	qe.mu.RLock()
	defer qe.mu.RUnlock()

	if query.BindVariables == nil { // will help us avoid repeated nil checks
		query.BindVariables = make(map[string]interface{})
	}
	logStats.BindVariables = query.BindVariables
	logStats.OriginalSql = query.Sql
	// cheap hack: strip trailing comment into a special bind var
	stripTrailing(query)
	basePlan := qe.schemaInfo.GetPlan(logStats, query.Sql)
	planName := basePlan.PlanId.String()
	logStats.PlanType = planName
	defer func(start time.Time) {
		duration := time.Now().Sub(start)
		queryStats.Add(planName, duration)
		if reply == nil {
			basePlan.AddStats(1, duration, 0, 1)
		} else {
			basePlan.AddStats(1, duration, int64(len(reply.Rows)), 0)
		}
	}(time.Now())

	/*	if basePlan.PlanId == sqlparser.PLAN_DDL {
		return qe.execDDL(logStats, query.Sql)
	}*/

	plan := &CompiledPlan{query.Sql, basePlan, query.BindVariables, query.TransactionId, query.ConnectionId}
	conn := qe.connPool.Get()
	defer conn.Recycle()
	log.Info("Plan type is %s, reason is %s", plan.PlanId, plan.Reason)

	switch plan.PlanId {
	case sqlparser.PLAN_INSERT_PK:
		reply = qe.execInsertPK(logStats, conn, plan)
	case sqlparser.PLAN_INSERT_SUBQUERY:
		reply = qe.execInsertSubquery(logStats, conn, plan)
	case sqlparser.PLAN_PK_EQUAL:
		reply = qe.selectPkEqual(logStats, conn, plan)
	default:
		panic("Plan currently not supported")
	}
	return reply
}

//-----------------------------------------------
// DDL

/*func (qe *QueryEngine) execDDL(logStats *sqlQueryStats, ddl string) *eproto.QueryResult {
	ddlPlan := sqlparser.DDLParse(ddl)
	if ddlPlan.Action == 0 {
		panic(NewTabletError(FAIL, "DDL is not understood"))
	}

	// Stolen from Begin
	conn := qe.txPool.Get()
	txid, err := qe.activeTxPool.SafeBegin(conn)
	if err != nil {
		conn.Recycle()
		panic(err)
	}
	// Stolen from Commit
	defer qe.activeTxPool.SafeCommit(txid)

	// Stolen from Execute
	conn = qe.activeTxPool.Get(txid)
	defer conn.Recycle()
	result, err := qe.executeSql(logStats, conn, ddl, false)
	if err != nil {
		panic(NewTabletErrorSql(FAIL, err))
	}

	qe.schemaInfo.DropTable(ddlPlan.TableName)
	if ddlPlan.Action != sqlparser.DROP { // CREATE, ALTER, RENAME
		qe.schemaInfo.CreateTable(ddlPlan.NewName)
	}
	return result
}*/

//-----------------------------------------------
// Execution
func (qe *QueryEngine) execInsertPK(logStats *sqlQueryStats, conn PoolConnection, plan *CompiledPlan) (qr *eproto.QueryResult) {
	log.Info("Execute insert pk sql %s", plan.Query)
	tableName := plan.TableName
	tableInfo := qe.schemaInfo.tables[plan.TableName]
	rowColumns := plan.RowColumns
	var pkValue []byte
	var key []byte
	var columnName string
	keys := make([][]byte, 0, len(rowColumns)*len(tableInfo.Columns))
	values := make([][]byte, 0, len(rowColumns)*len(tableInfo.Columns))
	var pkList [][]sqltypes.Value

	if len(tableInfo.PKColumns) > 1 {
		pkList = buildValueList(tableInfo, plan.PKValues, plan.BindVars)
	}
	log.Info("Row column length is %d", len(rowColumns))
	for i, columnsMap := range rowColumns {
		log.Info("map !!!!!:%v", len(columnsMap))
		if pkList != nil { // multiple pk
			pkValue = buildPkValue(pkList[i])
		} else {
			pkColumn := tableInfo.GetPKColumn(0)
			if pkColumn.IsAuto {
				if plan.PKValues != nil {
					panic(NewTabletErrorDB(FAIL, fmt.Errorf("field %s value is auto created", columnName)))
				}
				pkValue = []byte(strconv.FormatInt(pkColumn.GetNextId(), 64))
			} else if pkColumn.IsUUID {
				uid, err := uuid.NewV4()
				if err != nil {
					panic(NewTabletError(FATAL, "Make uuuid error"))
				}
				pkValue = []byte(uid.String())
			} else {
				// pkValue = plan.PKValues
			}
		}
		log.Info("Pk Value is %v", string(pkValue))
		for _, columnDef := range tableInfo.Columns {
			columnName = columnDef.Name
			if columnDef.IsPk {
				key = buildTableRowColumnKey(tableName, columnName, pkValue)
				log.Info("pk key is %v", string(key))
				keys = append(keys, key)
				values = append(values, []byte{'0'})

				// if column is auto increment, update the value
				if columnDef.IsAuto {
					keys = append(keys, buildTableColumnAutoKey(tableName, columnName))
					values = append(values, pkValue)
				}
			} else if columnDef.IsAuto {
				if _, ok := columnsMap[columnName]; ok {
					panic(NewTabletErrorDB(FAIL, fmt.Errorf("field %s value is auto created", columnName)))
				}
				keys = append(keys, buildTableRowColumnKey(tableName, columnName, pkValue))
				nextId := []byte(strconv.FormatInt(columnDef.GetNextId(), 64))
				values = append(values, nextId)

				keys = append(keys, buildTableColumnAutoKey(tableName, columnName))
				values = append(values, nextId)

			} else if columnDef.IsUUID {
				uid, err := uuid.NewV4()
				if err != nil {
					panic(NewTabletError(FATAL, "Make uuuid error"))
				}
				keys = append(keys, buildTableRowColumnKey(tableName, columnName, pkValue))
				values = append(values, []byte(uid.String()))
			} else {
				value, ok := columnsMap[columnName]
				if !ok {
					if !columnDef.Nullable {
						panic(NewTabletErrorDB(FAIL, fmt.Errorf("column %s shouldn't be null", columnDef.Name)))
					}
				}
				if !value.IsNull() {
					key = buildTableRowColumnKey(tableName, columnName, pkValue)
					log.Info("normal key is %v", string(key))
					keys = append(keys, key)
					values = append(values, value.Raw())
					log.Info("normal value is %v", value.String())
				}
			}
		}
	}
	atomic.AddInt64(&qe.activeConnection, 1)
	defer atomic.AddInt64(&qe.activeConnection, -1)
	err := conn.Puts(nil, keys, values)
	if err != nil {
		panic(NewTabletErrorDB(FAIL, err))
	}

	qr = &eproto.QueryResult{RowsAffected: uint64(len(rowColumns))}
	return qr
}

func (qe *QueryEngine) execUpdatePk(logStats *sqlQueryStats, conn PoolConnection, plan *CompiledPlan) (result *eproto.QueryResult) {
	return
}

func (qe *QueryEngine) execInsertSubquery(logStats *sqlQueryStats, conn PoolConnection, plan *CompiledPlan) (result *eproto.QueryResult) {
	return
}

func (qe *QueryEngine) execInsertPKRows(logStats *sqlQueryStats, conn PoolConnection, plan *CompiledPlan, pkRows [][]sqltypes.Value) (result *eproto.QueryResult) {
	return
}

func (qe *QueryEngine) selectPkEqual(logStats *sqlQueryStats, conn PoolConnection, plan *CompiledPlan) (result *eproto.QueryResult) {
	pkRows := buildValueList(plan.Table, plan.PKValues, plan.BindVars)
	if len(pkRows) != 1 || plan.Fields == nil {
		panic("unexpected")
	}

	tableName := plan.TableName
	keys := make([][]byte, len(plan.ColumnNumbers))

	pkValue := buildPkValue(pkRows[0])
	for i, field := range plan.Fields {
		keys[i] = buildTableRowColumnKey(tableName, field.Name, pkValue)
	}
	waitingForConnectionStart := time.Now()
	conn, err := qe.connPool.SafeGet()
	logStats.WaitingForConnection += time.Now().Sub(waitingForConnectionStart)
	if err != nil {
		panic(NewTabletErrorDB(FATAL, err))
	} else {
		defer conn.Recycle()
	}
	values, err := conn.Gets(nil, keys)
	if err != nil {
		panic(NewTabletErrorDB(FAIL, err))
	}
	rowValues := make([]sqltypes.Value, len(plan.ColumnNumbers))
	for i, field := range plan.Fields {
		rowValues[i] = buildValue(values[i], field.Type)
	}
	logStats.QuerySources |= QUERY_SOURCE_DBENGINE

	result = &eproto.QueryResult{}
	result.Fields = plan.Fields
	result.Rows = make([][]sqltypes.Value, 1)
	result.Rows[0] = rowValues
	result.RowsAffected = 1
	return
}

/*func (qe *QueryEngine) fetch(logStats *sqlQueryStats, parsed_query *sqlparser.ParsedQuery,
	bindVars map[string]interface{}, listVars []sqltypes.Value) (result *eproto.QueryResult) {
	sql := qe.generateFinalSql(parsed_query, bindVars, listVars)
	q, ok := qe.consolidator.Create(string(sql))
	if ok {
		defer q.Broadcast()
		waitingForConnectionStart := time.Now()
		conn, err := qe.connPool.SafeGet()
		logStats.WaitingForConnection += time.Now().Sub(waitingForConnectionStart)
		if err != nil {
			q.Err = NewTabletErrorDB(FATAL, err)
		} else {
			defer conn.Recycle()
		}
	}
}*/

// func (qe *QueryEngine) generateFinalSql(parsed_query *sqlparser.ParsedQuery, bindVars map[string]interface{}, listVars []sqltypes.Value) string {
// 	bindVars[MAX_RESULT_NAME] = qe.maxResultSize.Get() + 1
// 	sql, err := parsed_query.GenerateQuery(bindVars, listVars)
// 	if err != nil {
// 		panic(NewTabletError(FAIL, "%s", err))
// 	}
// 	// undo hack done by stripTrailing
// 	sql = restoreTrailing(sql, bindVars)
// 	return hack.String(sql)
// }
