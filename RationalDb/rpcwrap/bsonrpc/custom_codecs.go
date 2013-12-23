// Copyright 2012, Google Inc. All rights reserved.
// Use of this source code is governed by a BSD-style
// license that can be found in the LICENSE file.

package bsonrpc

import (
	"bytes"
	rpc "github.com/senarukana/rationaldb/rpcplus"
	"github.com/senarukana/rationaldb/util/bson"
	"github.com/senarukana/rationaldb/util/bytes2"
)

type RequestBson struct {
	*rpc.Request
}

func (req *RequestBson) MarshalBson(buf *bytes2.ChunkedWriter) {
	lenWriter := bson.NewLenWriter(buf)

	bson.EncodeString(buf, "ServiceMethod", req.ServiceMethod)
	bson.EncodeInt64(buf, "Seq", int64(req.Seq))

	buf.WriteByte(0)
	lenWriter.RecordLen()
}

func (req *RequestBson) UnmarshalBson(buf *bytes.Buffer) {
	bson.Next(buf, 4)

	kind := bson.NextByte(buf)
	for kind != bson.EOO {
		key := bson.ReadCString(buf)
		switch key {
		case "ServiceMethod":
			req.ServiceMethod = bson.DecodeString(buf, kind)
		case "Seq":
			req.Seq = bson.DecodeUint64(buf, kind)
		default:
			panic(bson.NewBsonError("Unrecognized tag %s", key))
		}
		kind = bson.NextByte(buf)
	}
}

type ResponseBson struct {
	*rpc.Response
}

func (resp *ResponseBson) MarshalBson(buf *bytes2.ChunkedWriter) {
	lenWriter := bson.NewLenWriter(buf)

	bson.EncodeString(buf, "ServiceMethod", resp.ServiceMethod)
	bson.EncodeInt64(buf, "Seq", int64(resp.Seq))
	bson.EncodeString(buf, "Error", resp.Error)

	buf.WriteByte(0)
	lenWriter.RecordLen()
}

func (resp *ResponseBson) UnmarshalBson(buf *bytes.Buffer) {
	bson.Next(buf, 4)

	kind := bson.NextByte(buf)
	for kind != bson.EOO {
		key := bson.ReadCString(buf)
		switch key {
		case "ServiceMethod":
			resp.ServiceMethod = bson.DecodeString(buf, kind)
		case "Seq":
			resp.Seq = bson.DecodeUint64(buf, kind)
		case "Error":
			resp.Error = bson.DecodeString(buf, kind)
		default:
			panic(bson.NewBsonError("Unrecognized tag %s", key))
		}
		kind = bson.NextByte(buf)
	}
}
