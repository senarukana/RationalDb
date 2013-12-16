// Copyright 2012, Google Inc. All rights reserved.
// Use of this source code is governed by a BSD-style
// license that can be found in the LICENSE file.

package tabletserver

import (
	"flag"
	"io/ioutil"
	"net/http"
	"strconv"

	"github.com/senarukana/rationaldb/log"
	mproto "github.com/senarukana/rationaldb/mysql/proto"
	rpcproto "github.com/senarukana/rationaldb/rpcwrap/proto"
	"github.com/senarukana/rationaldb/vt/dbconfigs"
	"github.com/senarukana/rationaldb/vt/tabletserver/proto"
)

var (
	queryLogHandler = flag.String("query-log-stream-handler", "/debug/querylog", "URL handler for streaming queries log")
	// txLogHandler    = flag.String("transaction-log-stream-handler", "/debug/txlog", "URL handler for streaming transactions log")
	// customRules     = flag.String("customrules", "", "custom query rules file")
)

func init() {
	flag.IntVar(&qsConfig.TransactionCap, "queryserver-config-transaction-cap", DefaultQsConfig.TransactionCap, "query server transaction cap")
	flag.Float64Var(&qsConfig.TransactionTimeout, "queryserver-config-transaction-timeout", DefaultQsConfig.TransactionTimeout, "query server transaction timeout")
	flag.IntVar(&qsConfig.MaxResultSize, "queryserver-config-max-result-size", DefaultQsConfig.MaxResultSize, "query server max result size")
	flag.IntVar(&qsConfig.StreamBufferSize, "queryserver-config-stream-buffer-size", DefaultQsConfig.StreamBufferSize, "query server stream buffer size")
	flag.IntVar(&qsConfig.QueryCacheSize, "queryserver-config-query-cache-size", DefaultQsConfig.QueryCacheSize, "query server query cache size")
	flag.Float64Var(&qsConfig.SchemaReloadTime, "queryserver-config-schema-reload-time", DefaultQsConfig.SchemaReloadTime, "query server schema reload time")
	flag.Float64Var(&qsConfig.QueryTimeout, "queryserver-config-query-timeout", DefaultQsConfig.QueryTimeout, "query server query timeout")
	flag.Float64Var(&qsConfig.IdleTimeout, "queryserver-config-idle-timeout", DefaultQsConfig.IdleTimeout, "query server idle timeout")
	flag.Float64Var(&qsConfig.SpotCheckRatio, "queryserver-config-spot-check-ratio", DefaultQsConfig.SpotCheckRatio, "query server rowcache spot check frequency")
	flag.IntVar(&qsConfig.StreamExecThrottle, "queryserver-config-stream-exec-throttle", DefaultQsConfig.StreamExecThrottle, "Maximum number of simultaneous streaming requests that can wait for results")
	flag.Float64Var(&qsConfig.StreamWaitTimeout, "queryserver-config-stream-exec-timeout", DefaultQsConfig.StreamWaitTimeout, "Timeout for stream-exec-throttle")
}

type Config struct {
	TransactionCap     int
	TransactionTimeout float64
	MaxResultSize      int
	StreamBufferSize   int
	QueryCacheSize     int
	SchemaReloadTime   float64
	QueryTimeout       float64
	SpotCheckRatio     float64
	StreamExecThrottle int
	StreamWaitTimeout  float64
}

// DefaultQSConfig is the default value for the query service config.
//
// The value for StreamBufferSize was chosen after trying out a few of
// them. Too small buffers force too many packets to be sent. Too big
// buffers force the clients to read them in multiple chunks and make
// memory copies.  so with the encoding overhead, this seems to work
// great (the overhead makes the final packets on the wire about twice
// bigger than this).
var DefaultQsConfig = Config{
	TransactionCap:     20,
	TransactionTimeout: 30,
	MaxResultSize:      10000,
	QueryCacheSize:     5000,
	SchemaReloadTime:   30 * 60,
	QueryTimeout:       0,
	IdleTimeout:        30 * 60,
	StreamBufferSize:   32 * 1024,
	SpotCheckRatio:     0,
	StreamExecThrottle: 8,
	StreamWaitTimeout:  4 * 60,
}

var qsConfig Config

var SqlQueryRpcService *SqlQuery

func RegisterQueryService() {
	if SqlQueryRpcService != nil {
		log.Warn("RPC service already up %v", SqlQueryRpcService)
		return
	}
	SqlQueryRpcService = NewSqlQuery(qsConfig)
	proto.RegisterAuthenticated(SqlQueryRpcService)
	http.HandleFunc("/debug/health", healthCheck)
}

// AllowQueries can take an indefinite amount of time to return because
// it keeps retrying until it obtains a valid connection to the database.
func AllowQueries(dbconfig dbconfigs.DBConfig, schemaOverrides []SchemaOverride, qrs *QueryRules) {
	defer logError()
	SqlQueryRpcService.allowQueries(dbconfig, schemaOverrides, qrs)
}

// DisallowQueries can take a long time to return (not indefinite) because
// it has to wait for queries & transactions to be completed or killed,
// and also for house keeping goroutines to be terminated.
func DisallowQueries() {
	defer logError()
	SqlQueryRpcService.disallowQueries()
}

// Reload the schema. If the query service is not running, nothing will happen
func ReloadSchema() {
	defer logError()
	SqlQueryRpcService.qe.schemaInfo.triggerReload()
}

func GetSessionId() int64 {
	return SqlQueryRpcService.sessionId
}

func SetQueryRules(qrs *QueryRules) {
	SqlQueryRpcService.qe.schemaInfo.SetRules(qrs)
}

func GetQueryRules() (qrs *QueryRules) {
	return SqlQueryRpcService.qe.schemaInfo.GetRules()
}

// IsHealthy returns nil if the query service is healthy (able to
// connect to the database and serving traffic) or an error explaining
// the unhealthiness otherwise.
func IsHealthy() error {
	return SqlQueryRpcService.Execute(
		new(rpcproto.Context),
		&proto.Query{Sql: "select 1 from dual", SessionId: SqlQueryRpcService.sessionId},
		new(mproto.QueryResult),
	)
}

func healthCheck(w http.ResponseWriter, r *http.Request) {
	w.Header().Set("Content-Type", "text/plain")
	if err := IsHealthy(); err != nil {
		w.Write([]byte("notok"))
	}
	w.Write([]byte("ok"))
}

// InitQueryService registers the query service, after loading any
// necessary config files. It also starts any relevant streaming logs.
func InitQueryService() {
	// SqlQueryLogger.ServeLogs(*queryLogHandler)
	RegisterQueryService()
}
