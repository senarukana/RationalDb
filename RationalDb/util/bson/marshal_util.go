// Copyright 2012, Google Inc. All rights reserved.
// Use of this source code is governed by a BSD-style
// license that can be found in the LICENSE file.

// Utility functions for custom encoders

package bson

import (
	"github.com/senarukana/rationaldb/util/bytes2"
)

func EncodeStringArray(buf *bytes2.ChunkedWriter, name string, values []string) {
	if values == nil {
		EncodePrefix(buf, Null, name)
	} else {
		EncodePrefix(buf, Array, name)
		lenWriter := NewLenWriter(buf)
		for i, val := range values {
			EncodeString(buf, Itoa(i), val)
		}
		buf.WriteByte(0)
		lenWriter.RecordLen()
	}
}
