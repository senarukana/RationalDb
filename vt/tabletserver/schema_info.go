// Copyright 2012, Google Inc. All rights reserved.
// Use of this source code is governed by a BSD-style
// license that can be found in the LICENSE file.

package tabletserver

import (
	"encoding/json"
	"fmt"
	"net/http"
	"sync"
	"time"

	"github.com/senarukana/rationaldb/cache"
	"github.com/senarukana/rationaldb/log"
	"github.com/senarukana/rationaldb/schema"
	"github.com/senarukana/rationaldb/sqltypes"
	"github.com/senarukana/rationaldb/util/stats"
	eproto "github.com/senarukana/rationaldb/vt/kvengine/proto"
	"github.com/senarukana/rationaldb/vt/sqlparser"
)

const maxTableCount = 10000

type ExecPlan struct {
	*sqlparser.ExecPlan
	Table  *schema.Table
	Fields []eproto.Field
	Rules  *QueryRules

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
	tables         map[string]*schema.Table
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

func testTables() *schema.Table {
	// t := new(schema.Table)
	// t.Name = "user"
	// t.AddColumn("id", "int(11)", sqltypes.NULL, "", true)
	// t.AddColumn("name", "varchar(255)", sqltypes.NULL, "", false)

	// index := t.AddIndex("PRIMARY")
	// index.AddColumn("id", 0)

	// t.PKColumns = []int{0}
	var schem map[string]*schema.Table
	schem = make(map[string]*schema.Table)
	var (
		SQLZERO = sqltypes.MakeString([]byte("0"))
	)

	a := schema.NewTable("a")
	a.AddColumn("eid", "int", SQLZERO, "", true)
	a.AddColumn("id", "int", SQLZERO, "", true)
	a.AddColumn("name", "varchar(10)", SQLZERO, "", false)
	a.AddColumn("foo", "varchar(10)", SQLZERO, "", false)
	acolumns := []string{"eid", "id", "name", "foo"}
	a.Indexes = append(a.Indexes, &schema.Index{Name: "PRIMARY", Columns: []string{"eid", "id"}, Cardinality: []uint64{1, 1}, DataColumns: acolumns})
	a.Indexes = append(a.Indexes, &schema.Index{Name: "a_name", Columns: []string{"eid", "name"}, Cardinality: []uint64{1, 1}, DataColumns: a.Indexes[0].Columns})
	a.Indexes = append(a.Indexes, &schema.Index{Name: "b_name", Columns: []string{"name"}, Cardinality: []uint64{3}, DataColumns: a.Indexes[0].Columns})
	a.Indexes = append(a.Indexes, &schema.Index{Name: "c_name", Columns: []string{"name"}, Cardinality: []uint64{2}, DataColumns: a.Indexes[0].Columns})
	a.PKColumns = append(a.PKColumns, 0, 1)
	a.CacheType = schema.CACHE_RW
	schem["a"] = a

	// b := schema.NewTable("b")
	// b.AddColumn("eid", "int", SQLZERO, "")
	// b.AddColumn("id", "int", SQLZERO, "")
	// bcolumns := []string{"eid", "id"}
	// b.Indexes = append(a.Indexes, &schema.Index{Name: "PRIMARY", Columns: []string{"eid", "id"}, Cardinality: []uint64{1, 1}, DataColumns: bcolumns})
	// b.PKColumns = append(a.PKColumns, 0, 1)
	// b.CacheType = schema.CACHE_NONE
	// schem["b"] = b

	// c := schema.NewTable("c")
	// c.AddColumn("eid", "int", SQLZERO, "")
	// c.AddColumn("id", "int", SQLZERO, "")
	// c.CacheType = schema.CACHE_NONE
	// schem["c"] = c

	// d := schema.NewTable("d")
	// d.AddColumn("name", "varbinary(10)", SQLZERO, "")
	// d.AddColumn("id", "int", SQLZERO, "")
	// d.AddColumn("foo", "varchar(10)", SQLZERO, "")
	// d.AddColumn("bar", "varchar(10)", SQLZERO, "")
	// dcolumns := []string{"name"}
	// d.Indexes = append(d.Indexes, &schema.Index{Name: "PRIMARY", Columns: []string{"name"}, Cardinality: []uint64{1}, DataColumns: dcolumns})
	// d.Indexes = append(d.Indexes, &schema.Index{Name: "d_id", Columns: []string{"id"}, Cardinality: []uint64{1}, DataColumns: d.Indexes[0].Columns})
	// d.Indexes = append(d.Indexes, &schema.Index{Name: "d_bar_never", Columns: []string{"bar", "foo"}, Cardinality: []uint64{2, 1}, DataColumns: d.Indexes[0].Columns})
	// d.Indexes = append(d.Indexes, &schema.Index{Name: "d_bar", Columns: []string{"bar", "foo"}, Cardinality: []uint64{3, 1}, DataColumns: d.Indexes[0].Columns})
	// d.PKColumns = append(d.PKColumns, 0)
	// d.CacheType = schema.CACHE_RW
	// schem["d"] = d

	// e := schema.NewTable("e")
	// e.AddColumn("eid", "int", SQLZERO, "")
	// e.AddColumn("id", "int", SQLZERO, "")
	// ecolumns := []string{"eid", "id"}
	// e.Indexes = append(e.Indexes, &schema.Index{Name: "PRIMARY", Columns: []string{"eid", "id"}, Cardinality: []uint64{1, 1}, DataColumns: ecolumns})
	// e.PKColumns = append(a.PKColumns, 0, 1)
	// e.CacheType = schema.CACHE_W
	// schem["e"] = e

	return a
}

func (si *SchemaInfo) Open(connFactory CreateConnectionFun) {
	si.connPool.Open(connFactory)
	conn := si.connPool.Get()
	defer conn.Recycle()
	tables, err := getTables(conn)
	if err != nil {
		panic(NewTabletError(FATAL, "Could not get table list: %v", err))
	} else {
		log.Info("get table %d", len(tables))
	}
	if len(tables) == 0 {
		tables = append(tables, testTables())
	}

	si.tables = make(map[string]*schema.Table)
	for _, tableInfo := range tables {
		si.tables[tableInfo.Name] = tableInfo
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
	log.Warn("plan sql ", sql)
	si.mu.Lock()
	defer si.mu.Unlock()
	if plan := si.getQuery(sql); plan != nil {
		return plan
	}

	var tableInfo *schema.Table
	GetTable := func(tableName string) (table *schema.Table, ok bool) {
		tableInfo, ok = si.tables[tableName]
		if !ok {
			return nil, false
		}
		return tableInfo, true
	}
	splan, err := sqlparser.ExecParse(sql, GetTable)
	if err != nil {
		panic(NewTabletError(FAIL, "%s", err))
	}
	plan = &ExecPlan{ExecPlan: splan, Table: tableInfo}
	// plan.Rules = si.rules.filterByPlan(sql, plan.PlanId)
	/*	if plan.PlanId.IsSelect() {
			if plan.FieldQuery == nil {
				log.Warn("Cannot cache field info: %s", sql)
			} else {
				sql := plan.FieldQuery.Query
				//TODO
				// r, err := si.executor(sql, 1, true)
				logStats.QuerySources |= QUERY_SOURCE_DBENGINE
				logStats.NumberOfQueries += 1
				logStats.AddRewrittenSql(sql)
				if err != nil {
					panic(NewTabletError(FAIL, "Error fetching fields: %v", err))
				}
				plan.Fields = r.Fields
			}
		} else if plan.PlanId == sqlparser.PLAN_DDL || plan.PlanId == sqlparser.PLAN_SET {
			return plan
		}*/
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

func (si *SchemaInfo) GetTable(tableName string) *schema.Table {
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
