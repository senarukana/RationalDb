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

	"github.com/senarukana/rationaldb/cache"
	"github.com/senarukana/rationaldb/log"
	"github.com/senarukana/rationaldb/sqltypes"
	"github.com/senarukana/rationaldb/util/stats"
	eproto "github.com/senarukana/rationaldb/vt/engine/proto"
	"github.com/senarukana/rationaldb/vt/schema"
	"github.com/senarukana/rationaldb/vt/sqlparser"
	"github.com/senarukana/rationaldb/vt/tabletserver/proto"
)

const maxTableCount = 10000

type ExecPlan struct {
	*sqlparser.ExecPlan
	TableInfo *TableInfo
	Fields    []eproto.Field
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
	connPool       *ConnectionPool
	queryCacheSize int
	queries        *cache.LRUCache
	rules          *QueryRules
}

func NewSchemaInfo(queryCacheSize int, idleTimeout time.Duration) *SchemaInfo {
	si := &SchemaInfo{
		queryCacheSize: queryCacheSize,
		queries:        cache.NewLRUCache(int64(queryCacheSize)),
		connPool:       NewConnectionPool("Schema", 2, idleTimeout),
	}
	stats.Publish("QueryCacheLength", stats.IntFunc(si.queries.Length))
	stats.Publish("QueryCacheSize", stats.IntFunc(si.queries.Size))
	stats.Publish("QueryCacheCapacity", stats.IntFunc(si.queries.Capacity))
	stats.Publish("QueryCacheOldest", stats.StringFunc(func() string {
		return fmt.Sprintf("%v", si.queries.Oldest())
	}))
	stats.Publish("QueryCounts", stats.NewMatrixFunc("Table", "Plan", si.getQueryCount))
	stats.Publish("QueryTimesNs", stats.NewMatrixFunc("Table", "Plan", si.getQueryTime))
	stats.Publish("QueryRowCounts", stats.NewMatrixFunc("Table", "Plan", si.getQueryRowCount))
	stats.Publish("QueryErrorCounts", stats.NewMatrixFunc("Table", "Plan", si.getQueryErrorCount))
	http.Handle("/debug/query_plans", si)
	http.Handle("/debug/query_stats", si)
	http.Handle("/debug/table_stats", si)
	return si
}

func (si *SchemaInfo) Open(connFactory proto.CreateKVEngineConnectionFunc) {
	si.connPool.Open(connFactory)
	conn := si.connPool.Get()
	defer conn.Recycle()
	tables, err := conn.ShowTables()
	if err != nil {
		panic(NewTabletError(FATAL, "Could not get table list: %v", err))
	}

	si.tables = make(map[string]*TableInfo, len(tables))
	for _, tableInfo := range tables {
		si.tables[tableName] = tableInfo
	}
	// Clear is not really needed. Doing it for good measure.
	si.queries.Clear()
	// si.ticks.Start(func() { si.Reload() })
}

func (si *SchemaInfo) Close() {
	si.tables = nil
	si.connPool.Close()
	si.queries.Clear()
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
	// plan.Rules = si.rules.filterByPlan(sql, plan.PlanId)
	if plan.PlanId.IsSelect() {
		if plan.FieldQuery == nil {
			log.Warn("Cannot cache field info: %s", sql)
		} else {
			sql := plan.FieldQuery.Query
			//TODO
			// r, err := si.executor(sql, 1, true)
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

/*func (si *SchemaInfo) SetRules(qrs *QueryRules) {
	si.mu.Lock()
	defer si.mu.Unlock()
	si.rules = qrs.Copy()
	si.queries.Clear()
}

func (si *SchemaInfo) GetRules() (qrs *QueryRules) {
	si.mu.Lock()
	defer si.mu.Unlock()
	return si.rules.Copy()
}*/

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
