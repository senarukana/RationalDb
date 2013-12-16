package servenv

import (
	"flag"

	"github.com/senarukana/rationaldb/log"
	rpc "github.com/senarukana/rationaldb/rpcplus"
	"github.com/senarukana/rationaldb/rpcwrap/auth"
	"github.com/senarukana/rationaldb/rpcwrap/bsonrpc"
	"github.com/senarukana/rationaldb/rpcwrap/jsonrpc"
)

var (
	authConfig = flag.String("auth-credentials", "", "name of file containing auth credentials")
)

func ServeRPC() {
	rpc.HandleHTTP()
	if *authConfig != "" {
		if err := auth.LoadCredentials(*authConfig); err != nil {
			log.Critical("could not load authentication credentials, not starting rpc servers: %v", err)
		}
		bsonrpc.ServeAuthRPC()
		jsonrpc.ServeAuthRPC()
	}

	jsonrpc.ServeHTTP()
	jsonrpc.ServeRPC()
	bsonrpc.ServeHTTP()
	bsonrpc.ServeRPC()
}
