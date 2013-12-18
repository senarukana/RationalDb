// Copyright 2012, Google Inc. All rights reserved.
// Use of this source code is governed by a BSD-style
// license that can be found in the LICENSE file.

package tabletserver

import (
	"sync"
	"sync/atomic"
	"time"

	"github.com/senarukana/rationaldb/log"
	"github.com/senarukana/rationaldb/schema"
	"github.com/senarukana/rationaldb/sqltypes"
	"github.com/senarukana/rationaldb/util/hack"
	"github.com/senarukana/rationaldb/util/stats"
	"github.com/senarukana/rationaldb/util/sync2"
	"github.com/senarukana/rationaldb/vt/engine"
	eproto "github.com/senarukana/rationaldb/vt/engine/proto"
	"github.com/senarukana/rationaldb/vt/sqlparser"
	"github.com/senarukana/rationaldb/vt/tabletserver/proto"
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

	engineMgr      *engine.EngineManager
	connPool       *ConnectionPool
	streamConnPool *ConnectionPool
	reservedPool   *ReservedPool
	schemaInfo     *SchemaInfo
	consolidator   *Consolidator

	spotCheckFreq sync2.AtomicInt64

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
	qe := &QueryEngine{}
	qe.engineMgr = engine.NewEngineManager(config.EngineName)
	qe.connPool = NewConnectionPool("ConnPool", config.PoolSize, time.Duration(config.IdleTimeout*1e9))
	qe.schemaInfo = NewSchemaInfo(config.QueryCacheSize)
	qe.consolidator = NewConsolidator()
	qe.spotCheckFreq = sync2.AtomicInt64(config.SpotCheckRatio * SPOT_CHECK_MULTIPLIER)
	qe.maxResultSize = sync2.AtomicInt64(config.MaxResultSize)
	qe.streamBufferSize = sync2.AtomicInt64(config.StreamBufferSize)
	stats.Publish("MaxResultSize", stats.IntFunc(qe.maxResultSize.Get))
	stats.Publish("StreamBufferSize", stats.IntFunc(qe.streamBufferSize.Get))
	queryStats = stats.NewTimings("Queries")
	QPSRates = stats.NewRates("QPS", queryStats, 15, 60*time.Second)
	waitStats = stats.NewTimings("Waits")
	errorStats = stats.NewCounters("Errors")
	resultStats = stats.NewHistogram("Results", resultBuckets)
	stats.Publish("SpotCheckRatio", stats.FloatFunc(func() float64 {
		return float64(qe.spotCheckFreq.Get()) / SPOT_CHECK_MULTIPLIER
	}))
	spotCheckCount = stats.NewInt("SpotCheckCount")
	return qe
}

func (qe *QueryEngine) Open(config *eproto.DBConfigs) {
	// Wait for Close, in case it's running
	qe.mu.Lock()
	defer qe.mu.Unlock()
	qe.engineMgr.Init(config)
	connFactory := KVEngineConnectionCreator(config.AppConnectParams, qe.engineMgr)
	qe.connPool.Open(connFactory)
	qe.streamConnPool.Open(connFactory)
	qe.reservedPool.Open(connFactory)

	start := time.Now().UnixNano()
	qe.schemaInfo.Open(connFactory)
	log.Info("Time taken to load the schema: %v ms", (time.Now().UnixNano()-start)/1e6)
}

func (qe *QueryEngine) Close() {
	qe.activeTxPool.WaitForEmpty()
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

	// Run it by the rules engine
	action, desc := basePlan.Rules.getAction(logStats.RemoteAddr(), logStats.Username(), query.BindVariables)
	if action == QR_FAIL_QUERY {
		panic(NewTabletError(FAIL, "Query disallowed due to rule: %s", desc))
	}

	if basePlan.PlanId == sqlparser.PLAN_DDL {
		return qe.execDDL(logStats, query.Sql)
	}

	plan := &CompiledPlan{query.Sql, basePlan, query.BindVariables, query.TransactionId, query.ConnectionId}
	if plan.ConnectionId != 0 {
		conn := qe.reservedPool.Get(plan.ConnectionId)
		defer conn.Recycle()
		if plan.PlanId.IsSelect() {
			reply = qe.execDirect(logStats, plan, conn)
		} else if plan.PlanId == sqlparser.PLAN_SET {
			reply = qe.directFetch(logStats, conn, plan.FullQuery, plan.BindVars, nil, nil)
		} else {
			panic(NewTabletError(NOT_IN_TX, "DMLs not allowed outside of transactions"))
		}
	} else {
		switch plan.PlanId {
		case sqlparser.PLAN_PASS_SELECT:
			if plan.Reason == sqlparser.REASON_FOR_UPDATE {
				panic(NewTabletError(FAIL, "Disallowed outside transaction"))
			}
			reply = qe.execSelect(logStats, plan)
		case sqlparser.PLAN_PK_EQUAL:
			reply = qe.execPKEqual(logStats, plan)
		case sqlparser.PLAN_PK_IN:
			reply = qe.execPKIN(logStats, plan)
		case sqlparser.PLAN_SELECT_SUBQUERY:
			reply = qe.execSubquery(logStats, plan)
		case sqlparser.PLAN_SET:
			waitingForConnectionStart := time.Now()
			conn := qe.connPool.Get()
			logStats.WaitingForConnection += time.Now().Sub(waitingForConnectionStart)
			defer conn.Recycle()
			reply = qe.execSet(logStats, conn, plan)
		default:
			panic(NewTabletError(NOT_IN_TX, "DMLs not allowed outside of transactions"))
		}
	}
	if plan.PlanId.IsSelect() {
		logStats.RowsAffected = int(reply.RowsAffected)
		resultStats.Add(int64(reply.RowsAffected))
		logStats.Rows = reply.Rows
	}

	return reply
}

//-----------------------------------------------
// DDL

func (qe *QueryEngine) execDDL(logStats *sqlQueryStats, ddl string) *eproto.QueryResult {
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
}

//-----------------------------------------------
// Execution

func (qe *QueryEngine) execPKEqual(logStats *sqlQueryStats, plan *CompiledPlan) (result *eproto.QueryResult) {
	pkRows := buildValueList(plan.TableInfo, plan.PKValues, plan.BindVars)
	if len(pkRows) != 1 || plan.Fields == nil {
		panic("unexpected")
	}
	row := qe.fetchOne(logStats, plan, pkRows[0])
	result = &eproto.QueryResult{}
	result.Fields = plan.Fields
	if row == nil {
		return
	}
	result.Rows = make([][]sqltypes.Value, 1)
	result.Rows[0] = applyFilter(plan.ColumnNumbers, row)
	result.RowsAffected = 1
	return
}

func (qe *QueryEngine) fetchOne(logStats *sqlQueryStats, plan *CompiledPlan, pk []sqltypes.Value) (row []sqltypes.Value) {
	logStats.QuerySources |= QUERY_SOURCE_ROWCACHE
	tableInfo := plan.TableInfo
	keys := make([]string, 1)
	keys[0] = buildKey(pk)
	resultFromdb := qe.qFetch(logStats, plan.OuterQuery, plan.BindVars, pk)
	if len(resultFromdb.Rows) == 0 {
		logStats.CacheAbsent++
		return nil
	}
	row = resultFromdb.Rows[0]
	return row
}

func (qe *QueryEngine) execPKIN(logStats *sqlQueryStats, plan *CompiledPlan) (result *eproto.QueryResult) {
	pkRows := buildINValueList(plan.TableInfo, plan.PKValues, plan.BindVars)
	return qe.fetchMulti(logStats, plan, pkRows)
}

func (qe *QueryEngine) execSubquery(logStats *sqlQueryStats, plan *CompiledPlan) (result *eproto.QueryResult) {
	innerResult := qe.qFetch(logStats, plan.Subquery, plan.BindVars, nil)
	return qe.fetchMulti(logStats, plan, innerResult.Rows)
}

func (qe *QueryEngine) fetchMulti(logStats *sqlQueryStats, plan *CompiledPlan, pkRows [][]sqltypes.Value) (result *eproto.QueryResult) {
	result = &eproto.QueryResult{}
	if len(pkRows) == 0 {
		return
	}
	if len(pkRows[0]) != 1 || plan.Fields == nil {
		panic("unexpected")
	}

	tableInfo := plan.TableInfo
	keys := make([]string, len(pkRows))
	for i, pk := range pkRows {
		keys[i] = buildKey(pk)
	}
	result.Fields = plan.Fields
	rows := make([][]sqltypes.Value, 0, len(pkRows))
	for i, pk := range pkRows {
		rcresult := rcresults[keys[i]]
		if rcresult.Row != nil {
			if qe.mustVerify() {
				qe.spotCheck(logStats, plan, rcresult, pk)
			}
			rows = append(rows, applyFilter(plan.ColumnNumbers, rcresult.Row))
			hits++
		} else {
			missingRows = append(missingRows, pk[0])
		}
	}
	if len(missingRows) != 0 {
		resultFromdb := qe.qFetch(logStats, plan.OuterQuery, plan.BindVars, missingRows)
		misses = int64(len(resultFromdb.Rows))
		absent = int64(len(pkRows)) - hits - misses
		for _, row := range resultFromdb.Rows {
			rows = append(rows, applyFilter(plan.ColumnNumbers, row))
			key := buildKey(applyFilter(plan.TableInfo.PKColumns, row))
			tableInfo.Cache.Set(key, row, rcresults[key].Cas)
		}
	}

	tableInfo.hits.Add(hits)
	tableInfo.absent.Add(absent)
	tableInfo.misses.Add(misses)
	result.RowsAffected = uint64(len(rows))
	result.Rows = rows
	return result
}

func (qe *QueryEngine) mustVerify() bool {
	return (Rand() % SPOT_CHECK_MULTIPLIER) < qe.spotCheckFreq.Get()
}

// execDirect always sends the query to dbengine
func (qe *QueryEngine) execDirect(logStats *sqlQueryStats, plan *CompiledPlan, conn proto.KVExecutorPoolConnection) (result *eproto.QueryResult) {
	if plan.Fields != nil {
		result = qe.directFetch(logStats, conn, plan.FullQuery, plan.BindVars, nil, nil)
		result.Fields = plan.Fields
		return
	}
	result = qe.fullFetch(logStats, conn, plan.FullQuery, plan.BindVars, nil, nil)
	return
}

// execSelect sends a query to mysql only if another identical query is not running. Otherwise, it waits and
// reuses the result. If the plan is missng field info, it sends the query to mysql requesting full info.
func (qe *QueryEngine) execSelect(logStats *sqlQueryStats, plan *CompiledPlan) (result *eproto.QueryResult) {
	if plan.Fields != nil {
		result = qe.qFetch(logStats, plan.FullQuery, plan.BindVars, nil)
		result.Fields = plan.Fields
		return
	}
	waitingForConnectionStart := time.Now()
	conn := qe.connPool.Get()
	logStats.WaitingForConnection += time.Now().Sub(waitingForConnectionStart)
	defer conn.Recycle()
	result = qe.fullFetch(logStats, conn, plan.FullQuery, plan.BindVars, nil, nil)
	return
}

func (qe *QueryEngine) execInsertPK(logStats *sqlQueryStats, conn proto.KVExecutorPoolConnection, plan *CompiledPlan, invalidator CacheInvalidator) (result *eproto.QueryResult) {
	pkRows := buildValueList(plan.TableInfo, plan.PKValues, plan.BindVars)
	return qe.execInsertPKRows(logStats, conn, plan, pkRows, invalidator)
}

func (qe *QueryEngine) execInsertSubquery(logStats *sqlQueryStats, conn proto.KVExecutorPoolConnection, plan *CompiledPlan, invalidator CacheInvalidator) (result *eproto.QueryResult) {
	innerResult := qe.directFetch(logStats, conn, plan.Subquery, plan.BindVars, nil, nil)
	innerRows := innerResult.Rows
	if len(innerRows) == 0 {
		return &eproto.QueryResult{RowsAffected: 0}
	}
	if len(plan.ColumnNumbers) != len(innerRows[0]) {
		panic(NewTabletError(FAIL, "Subquery length does not match column list"))
	}
	pkRows := make([][]sqltypes.Value, len(innerRows))
	for i, innerRow := range innerRows {
		pkRows[i] = applyFilterWithPKDefaults(plan.TableInfo, plan.SubqueryPKColumns, innerRow)
	}
	// Validating first row is sufficient
	validateRow(plan.TableInfo, plan.TableInfo.PKColumns, pkRows[0])
	plan.BindVars["_rowValues"] = innerRows
	return qe.execInsertPKRows(logStats, conn, plan, pkRows, invalidator)
}

func (qe *QueryEngine) execInsertPKRows(logStats *sqlQueryStats, conn proto.KVExecutorPoolConnection, plan *CompiledPlan, pkRows [][]sqltypes.Value, invalidator CacheInvalidator) (result *eproto.QueryResult) {
	secondaryList := buildSecondaryList(plan.TableInfo, pkRows, plan.SecondaryPKValues, plan.BindVars)
	bsc := buildStreamComment(plan.TableInfo, pkRows, secondaryList)
	result = qe.directFetch(logStats, conn, plan.OuterQuery, plan.BindVars, nil, bsc)
	// TODO: We need to do this only if insert has on duplicate key clause
	if invalidator != nil {
		for _, pk := range pkRows {
			if key := buildKey(pk); key != "" {
				invalidator.Delete(key)
			}
		}
	}
	return result
}

func (qe *QueryEngine) execDMLPK(logStats *sqlQueryStats, conn proto.KVExecutorPoolConnection, plan *CompiledPlan, invalidator CacheInvalidator) (result *eproto.QueryResult) {
	pkRows := buildValueList(plan.TableInfo, plan.PKValues, plan.BindVars)
	secondaryList := buildSecondaryList(plan.TableInfo, pkRows, plan.SecondaryPKValues, plan.BindVars)
	bsc := buildStreamComment(plan.TableInfo, pkRows, secondaryList)
	result = qe.directFetch(logStats, conn, plan.OuterQuery, plan.BindVars, nil, bsc)
	if invalidator != nil {
		for _, pk := range pkRows {
			key := buildKey(pk)
			invalidator.Delete(key)
		}
	}
	return result
}

func (qe *QueryEngine) execDMLSubquery(logStats *sqlQueryStats, conn proto.KVExecutorPoolConnection, plan *CompiledPlan, invalidator CacheInvalidator) (result *eproto.QueryResult) {
	innerResult := qe.directFetch(logStats, conn, plan.Subquery, plan.BindVars, nil, nil)
	// no need to validate innerResult
	return qe.execDMLPKRows(logStats, conn, plan, innerResult.Rows, invalidator)
}

func (qe *QueryEngine) execDMLPKRows(logStats *sqlQueryStats, conn proto.KVExecutorPoolConnection, plan *CompiledPlan, pkRows [][]sqltypes.Value, invalidator CacheInvalidator) (result *eproto.QueryResult) {
	if len(pkRows) == 0 {
		return &eproto.QueryResult{RowsAffected: 0}
	}
	rowsAffected := uint64(0)
	singleRow := make([][]sqltypes.Value, 1)
	for _, pkRow := range pkRows {
		singleRow[0] = pkRow
		secondaryList := buildSecondaryList(plan.TableInfo, singleRow, plan.SecondaryPKValues, plan.BindVars)
		bsc := buildStreamComment(plan.TableInfo, singleRow, secondaryList)
		rowsAffected += qe.directFetch(logStats, conn, plan.OuterQuery, plan.BindVars, pkRow, bsc).RowsAffected
		if invalidator != nil {
			key := buildKey(pkRow)
			invalidator.Delete(key)
		}
	}
	return &eproto.QueryResult{RowsAffected: rowsAffected}
}

func (qe *QueryEngine) execSet(logStats *sqlQueryStats, conn proto.KVExecutorPoolConnection, plan *CompiledPlan) (result *eproto.QueryResult) {
	switch plan.SetKey {
	case "vt_pool_size":
		qe.connPool.SetCapacity(int(plan.SetValue.(float64)))
	case "vt_stream_pool_size":
		qe.streamConnPool.SetCapacity(int(plan.SetValue.(float64)))
	case "vt_schema_reload_time":
		qe.schemaInfo.SetReloadTime(time.Duration(plan.SetValue.(float64) * 1e9))
	case "vt_query_cache_size":
		qe.schemaInfo.SetQueryCacheSize(int(plan.SetValue.(float64)))
	case "vt_max_result_size":
		val := int64(plan.SetValue.(float64))
		if val < 1 {
			panic(NewTabletError(FAIL, "max result size out of range %v", val))
		}
		qe.maxResultSize.Set(val)
	case "vt_stream_buffer_size":
		val := int64(plan.SetValue.(float64))
		if val < 1024 {
			panic(NewTabletError(FAIL, "stream buffer size out of range %v", val))
		}
		qe.streamBufferSize.Set(val)

	case "vt_idle_timeout":
		t := plan.SetValue.(float64) * 1e9
		qe.connPool.SetIdleTimeout(time.Duration(t))
		qe.streamConnPool.SetIdleTimeout(time.Duration(t))
	case "vt_spot_check_ratio":
		qe.spotCheckFreq.Set(int64(plan.SetValue.(float64) * SPOT_CHECK_MULTIPLIER))
	default:
		return qe.directFetch(logStats, conn, plan.FullQuery, plan.BindVars, nil, nil)
	}
	return &eproto.QueryResult{}
}

func (qe *QueryEngine) qFetch(logStats *sqlQueryStats, parsed_query *sqlparser.ParsedQuery, bindVars map[string]interface{}, listVars []sqltypes.Value) (result *eproto.QueryResult) {
	sql := qe.generateFinalSql(parsed_query, bindVars, listVars, nil)
	q, ok := qe.consolidator.Create(string(sql))
	if ok {
		defer q.Broadcast()
		waitingForConnectionStart := time.Now()
		conn, err := qe.connPool.SafeGet()
		logStats.WaitingForConnection += time.Now().Sub(waitingForConnectionStart)
		if err != nil {
			q.Err = NewTabletErrorSql(FATAL, err)
		} else {
			defer conn.Recycle()
			q.Result, q.Err = qe.executeSql(logStats, conn, sql, false)
		}
	} else {
		logStats.QuerySources |= QUERY_SOURCE_CONSOLIDATOR
		q.Wait()
	}
	if q.Err != nil {
		panic(q.Err)
	}
	return q.Result
}

func (qe *QueryEngine) directFetch(logStats *sqlQueryStats, conn proto.KVExecutorPoolConnection, parsed_query *sqlparser.ParsedQuery, bindVars map[string]interface{}, listVars []sqltypes.Value, buildStreamComment []byte) (result *eproto.QueryResult) {
	sql := qe.generateFinalSql(parsed_query, bindVars, listVars, buildStreamComment)
	result, err := qe.executeSql(logStats, conn, sql, false)
	if err != nil {
		panic(err)
	}
	return result
}

// fullFetch also fetches field info
func (qe *QueryEngine) fullFetch(logStats *sqlQueryStats, conn proto.KVExecutorPoolConnection, parsed_query *sqlparser.ParsedQuery, bindVars map[string]interface{}, listVars []sqltypes.Value, buildStreamComment []byte) (result *eproto.QueryResult) {
	sql := qe.generateFinalSql(parsed_query, bindVars, listVars, buildStreamComment)
	result, err := qe.executeSql(logStats, conn, sql, true)
	if err != nil {
		panic(err)
	}
	return result
}

func (qe *QueryEngine) generateFinalSql(parsed_query *sqlparser.ParsedQuery, bindVars map[string]interface{}, listVars []sqltypes.Value, buildStreamComment []byte) string {
	bindVars[MAX_RESULT_NAME] = qe.maxResultSize.Get() + 1
	sql, err := parsed_query.GenerateQuery(bindVars, listVars)
	if err != nil {
		panic(NewTabletError(FAIL, "%s", err))
	}
	if buildStreamComment != nil {
		sql = append(sql, buildStreamComment...)
	}
	// undo hack done by stripTrailing
	sql = restoreTrailing(sql, bindVars)
	return hack.String(sql)
}

func (qe *QueryEngine) executeSql(logStats *sqlQueryStats, conn proto.KVExecutorPoolConnection, sql string, wantfields bool) (*eproto.QueryResult, error) {
	connid := conn.Id()
	qe.activeConnection = atomic.AddInt64(&qe.activeConnection, 1)
	defer func() {
		qe.activeConnection = atomic.AddInt64(&qe.activeConnection, -1)
	}()

	logStats.QuerySources |= QUERY_SOURCE_DBENGINE
	logStats.NumberOfQueries += 1
	logStats.AddRewrittenSql(sql)

	// NOTE(szopa): I am not doing this measurement inside
	// conn.ExecuteFetch because that would require changing the
	// proto.KVExecutorPoolConnection interface. Same applies to executeStreamSql.
	fetchStart := time.Now()
	result, err := conn.ExecuteFetch(sql, int(qe.maxResultSize.Get()), wantfields)
	logStats.DbResponseTime += time.Now().Sub(fetchStart)

	if err != nil {
		return nil, NewTabletErrorSql(FAIL, err)
	}
	return result, nil
}

func rowsAreEqual(row1, row2 []sqltypes.Value) bool {
	if len(row1) != len(row2) {
		return false
	}
	for i := 0; i < len(row1); i++ {
		if row1[i].IsNull() && row2[i].IsNull() {
			continue
		}
		if (row1[i].IsNull() && !row2[i].IsNull()) || (!row1[i].IsNull() && row2[i].IsNull()) || row1[i].String() != row2[i].String() {
			return false
		}
	}
	return true
}
