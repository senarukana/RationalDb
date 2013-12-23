// Copyright 2012, Google Inc. All rights reserved.
// Use of this source code is governed by a BSD-style
// license that can be found in the LICENSE file.

package main

import (
	"encoding/json"
	"flag"
	"time"

	"github.com/senarukana/rationaldb/log"
	"github.com/senarukana/rationaldb/vt/servenv"
	ts "github.com/senarukana/rationaldb/vt/tabletserver"
)

var (
	port = flag.Int("port", 6510, "tcp port to serve on")
)

func main() {
	flag.Parse()
	servenv.Init()

	ts.InitQueryService()

	ts.AllowQueries()

	log.Info("starting vtocc %v", *port)
	servenv.OnClose(func() {
		time.Sleep(5 * time.Millisecond)
		ts.DisallowQueries()
	})
	servenv.Run(*port)
}
