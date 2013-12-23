// Copyright 2012, Google Inc. All rights reserved.
// Use of this source code is governed by a BSD-style
// license that can be found in the LICENSE file.

package tabletserver

import (
	"encoding/json"
	"fmt"
	"net/http"
	"strconv"
	"sync"
	"time"

	"github.com/senarukana/rationaldb/log"
	"github.com/senarukana/rationaldb/cache"
	mproto "github.com/senarukana/rationaldb/mysql/proto"
	"github.com/senarukana/rationaldb/sqltypes"
	"github.com/senarukana/rationaldb/stats"
	"github.com/senarukana/rationaldb/timer"
	"github.com/senarukana/rationaldb/vt/schema"
	"github.com/senarukana/rationaldb/vt/sqlparser"
)

const base_show_tables = "select table_name, table_type, unix_timestamp(create_time), table_comment from information_schema.tables where table_schema = database()"

const maxTableCount = 10000

type ExecPlan struct {
	*sqlparser.ExecPlan
	TableInfo *TableInfo
	Fields    []mproto.Field
	Rules     *QueryRules

	mu         sync.Mutex
	QueryCount int64
	Time       time.Duration
	RowCount   int64
	ErrorCount int64
}

func (*ExecPlan) Size() int {
	return 1
}

func (ep *ExecPlan) AddStats(queryCount int64, duration time.Duration, rowCount, errorCount int64) {
	ep.mu.Lock()
	ep.QueryCount += queryCount
	ep.Time += duration
	ep.RowCount += rowCount
	ep.ErrorCount += errorCount
	ep.mu.Unlock()
}

func (ep *ExecPlan) Stats() (queryCount int64, duration time.Duration, rowCount, errorCount int64) {
	ep.mu.Lock()
	queryCount = ep.QueryCount
	duration = ep.Time
	rowCount = ep.RowCount
	errorCount = ep.ErrorCount
	ep.mu.Unlock()
	return
}

type SchemaOverride struct {
	Name      string
	PKColumns []string
	Cache     *struct {
		Type   string
		Prefix string
		Table  string
	}
}

type SchemaInfo struct {
	mu             sync.Mutex
	tables         map[string]*TableInfo
	queryCacheSize int
	queries        *cache.LRUCache
	rules          *QueryRules
	connPool       *ConnectionPool
	cachePool      *CachePool
	reloadTime     time.Duration
	lastChange     time.Time
	ticks          *timer.Timer
}

func NewSchemaInfo(queryCacheSize int, reloadTime time.Duration, idleTimeout time.Duration) *SchemaInfo {
	si := &SchemaInfo{
		queryCacheSize: queryCacheSize,
		queries:        cache.NewLRUCache(int64(queryCacheSize)),
		rules:          NewQueryRules(),
		connPool:       NewConnectionPool("", 2, idleTimeout),
		reloadTime:     reloadTime,
		ticks:          timer.NewTimer(reloadTime),
	}
	stats.Publish("QueryCacheLength", stats.IntFunc(si.queries.Length))
	stats.Publish("QueryCacheSize", stats.IntFunc(si.queries.Size))
	stats.Publish("QueryCacheCapacity", stats.IntFunc(si.queries.Capacity))
	stats.Publish("QueryCacheOldest", stats.StringFunc(func() string {
		return fmt.Sprintf("%v", si.queries.Oldest())
	}))
	stats.Publish("SchemaReloadTime", stats.DurationFunc(func() time.Duration {
		return si.reloadTime
	}))
	stats.Publish("TableStats", stats.NewMatrixFunc("Table", "Stats", si.getTableStats))
	stats.Publish("TableInvalidations", stats.CountersFunc(si.getTableInvalidations))
	stats.Publish("QueryCounts", stats.NewMatrixFunc("Table", "Plan", si.getQueryCount))
	stats.Publish("QueryTimesNs", stats.NewMatrixFunc("Table", "Plan", si.getQueryTime))
	stats.Publish("QueryRowCounts", stats.NewMatrixFunc("Table", "Plan", si.getQueryRowCount))
	stats.Publish("QueryErrorCounts", stats.NewMatrixFunc("Table", "Plan", si.getQueryErrorCount))
	http.Handle("/debug/query_plans", si)
	http.Handle("/debug/query_stats", si)
	http.Handle("/debug/table_stats", si)
	return si
}

func (si *SchemaInfo) Open(connFactory CreateConnectionFunc, schemaOverrides []SchemaOverride, cachePool *CachePool, qrs *QueryRules) {
	si.connPool.Open(connFactory)
	conn := si.connPool.Get()
	defer conn.Recycle()

	if !conn.VerifyStrict() {
		panic(NewTabletError(FATAL, "Could not verify strict mode"))
	}

	si.cachePool = cachePool
	tables, err := conn.ExecuteFetch(base_show_tables, maxTableCount, false)
	if err != nil {
		panic(NewTabletError(FATAL, "Could not get table list: %v", err))
	}

	si.tables = make(map[string]*TableInfo, len(tables.Rows))
	si.tables["dual"] = NewTableInfo(conn, "dual", "VIEW", sqltypes.NULL, "", si.cachePool)
	for _, row := range tables.Rows {
		tableName := row[0].String()
		si.updateLastChange(row[2])
		tableInfo := NewTableInfo(
			conn,
			tableName,
			row[1].String(), // table_type
			row[2],          // create_time
			row[3].String(), // table_comment
			si.cachePool,
		)
		if tableInfo == nil {
			continue
		}
		si.tables[tableName] = tableInfo
	}
	if schemaOverrides != nil {
		si.override(schemaOverrides)
	}
	// Clear is not really needed. Doing it for good measure.
	si.queries.Clear()
	si.rules = qrs.Copy()
	si.ticks.Start(func() { si.Reload() })
}

func (si *SchemaInfo) updateLastChange(createTime sqltypes.Value) {
	if createTime.IsNull() {
		return
	}
	t, err := strconv.ParseInt(createTime.String(), 10, 64)
	if err != nil {
		log.Warn("Could not parse time %s: %v", createTime.String(), err)
		return
	}
	if si.lastChange.Unix() < t {
		si.lastChange = time.Unix(t, 0)
	}
}

func (si *SchemaInfo) override(schemaOverrides []SchemaOverride) {
	for _, override := range schemaOverrides {
		table, ok := si.tables[override.Name]
		if !ok {
			log.Warn("Table not found for override: %v", override)
			continue
		}
		if override.PKColumns != nil {
			if err := table.SetPK(override.PKColumns); err != nil {
				log.Warn("%v: %v", err, override)
				continue
			}
		}
		if si.cachePool.IsClosed() || override.Cache == nil {
			continue
		}
		switch override.Cache.Type {
		case "RW":
			table.CacheType = schema.CACHE_RW
			table.Cache = NewRowCache(table, si.cachePool)
		case "W":
			table.CacheType = schema.CACHE_W
			if override.Cache.Table == "" {
				log.Warn("Incomplete cache specs: %v", override)
				continue
			}
			totable, ok := si.tables[override.Cache.Table]
			if !ok {
				log.Warn("Table not found: %v", override)
				continue
			}
			if totable.Cache == nil {
				log.Warn("Table has no cache: %v", override)
				continue
			}
			table.Cache = totable.Cache
		default:
			log.Warn("Ignoring cache override: %v", override)
		}
	}
}

func (si *SchemaInfo) Close() {
	si.ticks.Stop()
	si.connPool.Close()
	si.tables = nil
	si.queries.Clear()
	si.rules = NewQueryRules()
}

func (si *SchemaInfo) Reload() {
	var err error
	defer handleError(&err, nil)
	conn := si.connPool.Get()
	defer conn.Recycle()
	tables, err := conn.ExecuteFetch(fmt.Sprintf("%s and unix_timestamp(create_time) > %v", base_show_tables, si.lastChange.Unix()), maxTableCount, false)
	if err != nil {
		log.Warn("Could not get table list for reload: %v", err)
		return
	}
	log.Info("Reloading schema")
	for _, row := range tables.Rows {
		tableName := row[0].String()
		si.updateLastChange(row[2])
		log.Info("Reloading: %s", tableName)
		si.mu.Lock()
		_, ok := si.tables[tableName]
		si.mu.Unlock()
		if ok {
			si.DropTable(tableName)
		}
		si.createTable(conn, tableName)
	}
}

// safe to call this if Close has been called, as si.ticks will be stopped
// and won't fire
func (si *SchemaInfo) triggerReload() {
	si.ticks.Trigger()
}

func (si *SchemaInfo) CreateTable(tableName string) {
	conn := si.connPool.Get()
	defer conn.Recycle()
	si.createTable(conn, tableName)
}

func (si *SchemaInfo) createTable(conn PoolConnection, tableName string) {
	tables, err := conn.ExecuteFetch(fmt.Sprintf("%s and table_name = '%s'", base_show_tables, tableName), 1, false)
	if err != nil {
		panic(NewTabletError(FAIL, "Error fetching table %s: %v", tableName, err))
	}
	if len(tables.Rows) != 1 {
		panic(NewTabletError(FAIL, "rows for %s !=1: %v", tableName, len(tables.Rows)))
	}
	tableInfo := NewTableInfo(
		conn,
		tableName,
		tables.Rows[0][1].String(), // table_type
		tables.Rows[0][2],          // create_time
		tables.Rows[0][3].String(), // table_comment
		si.cachePool,
	)
	if tableInfo == nil {
		panic(NewTabletError(FATAL, "Could not read table info: %s", tableName))
	}
	if tableInfo.CacheType == schema.CACHE_NONE {
		log.Info("Initialized table: %s", tableName)
	} else {
		log.Info("Initialized cached table: %s", tableInfo.Cache.prefix)
	}
	si.mu.Lock()
	defer si.mu.Unlock()
	if _, ok := si.tables[tableName]; ok {
		panic(NewTabletError(FAIL, "Table %s already exists", tableName))
	}
	si.tables[tableName] = tableInfo
}

func (si *SchemaInfo) DropTable(tableName string) {
	si.mu.Lock()
	defer si.mu.Unlock()
	delete(si.tables, tableName)
	si.queries.Clear()
	log.Info("Table %s forgotten", tableName)
}

func (si *SchemaInfo) GetPlan(logStats *sqlQueryStats, sql string) (plan *ExecPlan) {
	si.mu.Lock()
	defer si.mu.Unlock()
	if plan := si.getQuery(sql); plan != nil {
		return plan
	}

	var tableInfo *TableInfo
	GetTable := func(tableName string) (table *schema.Table, ok bool) {
		tableInfo, ok = si.tables[tableName]
		if !ok {
			return nil, false
		}
		return tableInfo.Table, true
	}
	splan, err := sqlparser.ExecParse(sql, GetTable)
	if err != nil {
		panic(NewTabletError(FAIL, "%s", err))
	}
	plan = &ExecPlan{ExecPlan: splan, TableInfo: tableInfo}
	plan.Rules = si.rules.filterByPlan(sql, plan.PlanId)
	if plan.PlanId.IsSelect() {
		if plan.FieldQuery == nil {
			log.Warn("Cannot cache field info: %s", sql)
		} else {
			conn := si.connPool.Get()
			defer conn.Recycle()
			sql := plan.FieldQuery.Query
			r, err := conn.ExecuteFetch(sql, 1, true)
			logStats.QuerySources |= QUERY_SOURCE_MYSQL
			logStats.NumberOfQueries += 1
			logStats.AddRewrittenSql(sql)
			if err != nil {
				panic(NewTabletError(FAIL, "Error fetching fields: %v", err))
			}
			plan.Fields = r.Fields
		}
	} else if plan.PlanId == sqlparser.PLAN_DDL || plan.PlanId == sqlparser.PLAN_SET {
		return plan
	}
	si.queries.Set(sql, plan)
	return plan
}

// GetStreamPlan is similar to GetPlan, but doesn't use the cache
// and doesn't enforce a limit. It also just returns the parsed query.
func (si *SchemaInfo) GetStreamPlan(sql string) *sqlparser.ParsedQuery {
	fullQuery, err := sqlparser.StreamExecParse(sql)
	if err != nil {
		panic(NewTabletError(FAIL, "%s", err))
	}
	return fullQuery
}

func (si *SchemaInfo) SetRules(qrs *QueryRules) {
	si.mu.Lock()
	defer si.mu.Unlock()
	si.rules = qrs.Copy()
	si.queries.Clear()
}

func (si *SchemaInfo) GetRules() (qrs *QueryRules) {
	si.mu.Lock()
	defer si.mu.Unlock()
	return si.rules.Copy()
}

func (si *SchemaInfo) GetTable(tableName string) *TableInfo {
	si.mu.Lock()
	defer si.mu.Unlock()
	return si.tables[tableName]
}

func (si *SchemaInfo) getQuery(sql string) *ExecPlan {
	if cacheResult, ok := si.queries.Get(sql); ok {
		return cacheResult.(*ExecPlan)
	}
	return nil
}

func (si *SchemaInfo) SetQueryCacheSize(size int) {
	if size <= 0 {
		panic(NewTabletError(FAIL, "cache size %v out of range", size))
	}
	si.queryCacheSize = size
	si.queries.SetCapacity(int64(size))
}

func (si *SchemaInfo) SetReloadTime(reloadTime time.Duration) {
	si.reloadTime = reloadTime
	si.ticks.Trigger()
	si.ticks.SetInterval(reloadTime)
}

func (si *SchemaInfo) getTableStats() map[string]map[string]int64 {
	si.mu.Lock()
	defer si.mu.Unlock()
	tstats := make(map[string]map[string]int64)
	for k, v := range si.tables {
		if v.CacheType != schema.CACHE_NONE {
			hits, absent, misses, _ := v.Stats()
			tblstats := make(map[string]int64)
			tblstats["Hits"] = hits
			tblstats["Absent"] = absent
			tblstats["Misses"] = misses
			tstats[k] = tblstats
		}
	}
	return tstats
}

func (si *SchemaInfo) getTableInvalidations() map[string]int64 {
	si.mu.Lock()
	defer si.mu.Unlock()
	tstats := make(map[string]int64)
	for k, v := range si.tables {
		if v.CacheType != schema.CACHE_NONE {
			_, _, _, invalidations := v.Stats()
			tstats[k] = invalidations
		}
	}
	return tstats
}

func (si *SchemaInfo) getQueryCount() map[string]map[string]int64 {
	f := func(plan *ExecPlan) int64 {
		queryCount, _, _, _ := plan.Stats()
		return queryCount
	}
	return si.getQueryStats(f)
}

func (si *SchemaInfo) getQueryTime() map[string]map[string]int64 {
	f := func(plan *ExecPlan) int64 {
		_, time, _, _ := plan.Stats()
		return int64(time)
	}
	return si.getQueryStats(f)
}

func (si *SchemaInfo) getQueryRowCount() map[string]map[string]int64 {
	f := func(plan *ExecPlan) int64 {
		_, _, rowCount, _ := plan.Stats()
		return rowCount
	}
	return si.getQueryStats(f)
}

func (si *SchemaInfo) getQueryErrorCount() map[string]map[string]int64 {
	f := func(plan *ExecPlan) int64 {
		_, _, _, errorCount := plan.Stats()
		return errorCount
	}
	return si.getQueryStats(f)
}

type queryStatsFunc func(*ExecPlan) int64

func (si *SchemaInfo) getQueryStats(f queryStatsFunc) map[string]map[string]int64 {
	keys := si.queries.Keys()
	qstats := make(map[string]map[string]int64)
	for _, v := range keys {
		if plan := si.getQuery(v); plan != nil {
			table := plan.TableName
			if table == "" {
				table = "Join"
			}
			planType := plan.PlanId.String()
			data := f(plan)
			queryStats, ok := qstats[table]
			if !ok {
				queryStats = make(map[string]int64)
				qstats[table] = queryStats
			}
			queryStats[planType] += data
		}
	}
	return qstats
}

type perQueryStats struct {
	Query      string
	Table      string
	Plan       sqlparser.PlanType
	QueryCount int64
	Time       time.Duration
	RowCount   int64
	ErrorCount int64
}

func (si *SchemaInfo) ServeHTTP(response http.ResponseWriter, request *http.Request) {
	if request.URL.Path == "/debug/query_plans" {
		keys := si.queries.Keys()
		response.Header().Set("Content-Type", "text/plain")
		response.Write([]byte(fmt.Sprintf("Length: %d\n", len(keys))))
		for _, v := range keys {
			response.Write([]byte(fmt.Sprintf("%#v\n", v)))
			if plan := si.getQuery(v); plan != nil {
				if b, err := json.MarshalIndent(plan.ExecPlan, "", "  "); err != nil {
					response.Write([]byte(err.Error()))
				} else {
					response.Write(b)
				}
				response.Write(([]byte)("\n\n"))
			}
		}
	} else if request.URL.Path == "/debug/query_stats" {
		keys := si.queries.Keys()
		response.Header().Set("Content-Type", "application/json; charset=utf-8")
		qstats := make([]perQueryStats, 0, len(keys))
		for _, v := range keys {
			if plan := si.getQuery(v); plan != nil {
				var pqstats perQueryStats
				pqstats.Query = unicoded(v)
				pqstats.Table = plan.TableName
				pqstats.Plan = plan.PlanId
				pqstats.QueryCount, pqstats.Time, pqstats.RowCount, pqstats.ErrorCount = plan.Stats()
				qstats = append(qstats, pqstats)
			}
		}
		if b, err := json.MarshalIndent(qstats, "", "  "); err != nil {
			response.Write([]byte(err.Error()))
		} else {
			response.Write(b)
		}
	} else if request.URL.Path == "/debug/table_stats" {
		response.Header().Set("Content-Type", "application/json; charset=utf-8")
		si.mu.Lock()
		tstats := make(map[string]struct{ hits, absent, misses, invalidations int64 })
		var temp, totals struct{ hits, absent, misses, invalidations int64 }
		for k, v := range si.tables {
			if v.CacheType != schema.CACHE_NONE {
				temp.hits, temp.absent, temp.misses, temp.invalidations = v.Stats()
				tstats[k] = temp
				totals.hits += temp.hits
				totals.absent += temp.absent
				totals.misses += temp.misses
				totals.invalidations += temp.invalidations
			}
		}
		si.mu.Unlock()
		response.Write([]byte("{\n"))
		for k, v := range tstats {
			fmt.Fprintf(response, "\"%s\": {\"Hits\": %v, \"Absent\": %v, \"Misses\": %v, \"Invalidations\": %v},\n", k, v.hits, v.absent, v.misses, v.invalidations)
		}
		fmt.Fprintf(response, "\"Totals\": {\"Hits\": %v, \"Absent\": %v, \"Misses\": %v, \"Invalidations\": %v}\n", totals.hits, totals.absent, totals.misses, totals.invalidations)
		response.Write([]byte("}\n"))
	} else {
		response.WriteHeader(http.StatusNotFound)
	}
}

func applyFieldFilter(columnNumbers []int, input []mproto.Field) (output []mproto.Field) {
	output = make([]mproto.Field, len(columnNumbers))
	for colIndex, colPointer := range columnNumbers {
		if colPointer >= 0 {
			output[colIndex] = input[colPointer]
		}
	}
	return output
}
