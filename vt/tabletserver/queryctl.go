// Copyright 2012, Google Inc. All rights reserved.
// Use of this source code is governed by a BSD-style
// license that can be found in the LICENSE file.

package tabletserver

import (
	"flag"
	"net/http"

	"github.com/senarukana/rationaldb/log"
	rpcproto "github.com/senarukana/rationaldb/rpcwrap/proto"
	eproto "github.com/senarukana/rationaldb/vt/kvengine/proto"
	"github.com/senarukana/rationaldb/vt/tabletserver/proto"
)

var (
	queryLogHandler = flag.String("query-log-stream-handler", "/debug/querylog", "URL handler for streaming queries log")
	// txLogHandler    = flag.String("transaction-log-stream-handler", "/debug/txlog", "URL handler for streaming transactions log")
	// customRules     = flag.String("customrules", "", "custom query rules file")
)

func init() {
	flag.StringVar(&qsConfig.EngineName, "queryserver-config-engine-name", DefaultQsConfig.EngineName, "default query server engine name")
	flag.IntVar(&qsConfig.PoolSize, "queryserver-config-pool-size", DefaultQsConfig.PoolSize, "query server pool size")
	flag.IntVar(&qsConfig.MaxResultSize, "queryserver-config-max-result-size", DefaultQsConfig.MaxResultSize, "query server max result size")
	flag.IntVar(&qsConfig.StreamBufferSize, "queryserver-config-stream-buffer-size", DefaultQsConfig.StreamBufferSize, "query server stream buffer size")
	flag.IntVar(&qsConfig.QueryCacheSize, "queryserver-config-query-cache-size", DefaultQsConfig.QueryCacheSize, "query server query cache size")
	flag.Float64Var(&qsConfig.IdleTimeout, "queryserver-config-idle-timeout", DefaultQsConfig.IdleTimeout, "query server idle timeout")
	flag.IntVar(&qsConfig.StreamExecThrottle, "queryserver-config-stream-exec-throttle", DefaultQsConfig.StreamExecThrottle, "Maximum number of simultaneous streaming requests that can wait for results")
}

type Config struct {
	EngineName         string
	PoolSize           int
	StreamPoolSize     int
	MaxResultSize      int
	StreamBufferSize   int
	QueryCacheSize     int
	IdleTimeout        float64
	SpotCheckRatio     float64
	StreamExecThrottle int
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
	EngineName:         "rocksdb",
	PoolSize:           16,
	StreamPoolSize:     750,
	MaxResultSize:      10000,
	QueryCacheSize:     5000,
	IdleTimeout:        30 * 60,
	StreamBufferSize:   32 * 1024,
	SpotCheckRatio:     0,
	StreamExecThrottle: 8,
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
func AllowQueries(dbconfig *eproto.DBConfigs) {
	defer logError()
	SqlQueryRpcService.allowQueries(dbconfig)
}

// DisallowQueries can take a long time to return (not indefinite) because
// it has to wait for queries & transactions to be completed or killed,
// and also for house keeping goroutines to be terminated.
func DisallowQueries() {
	defer logError()
	SqlQueryRpcService.disallowQueries()
}

func GetSessionId() int64 {
	return SqlQueryRpcService.sessionId
}

// IsHealthy returns nil if the query service is healthy (able to
// connect to the database and serving traffic) or an error explaining
// the unhealthiness otherwise.
func IsHealthy() error {
	return SqlQueryRpcService.Execute(
		new(rpcproto.Context),
		&proto.Query{Sql: "get test", SessionId: SqlQueryRpcService.sessionId},
		new(eproto.QueryResult),
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
