// Copyright 2012, Google Inc. All rights reserved.
// Use of this source code is governed by a BSD-style
// license that can be found in the LICENSE file.

package stats

import (
	"expvar"
	"testing"
)

func TestCounters(t *testing.T) {
	clear()
	c := NewCounters("counter1")
	c.Add("c1", 1)
	c.Add("c2", 1)
	c.Add("c2", 1)
	want1 := `{"c1": 1, "c2": 2}`
	want2 := `{"c2": 2, "c1": 1}`
	if c.String() != want1 && c.String() != want2 {
		t.Error("want %s or %s, got %s", want1, want2, c.String())
	}
	counts := c.Counts()
	if counts["c1"] != 1 {
		t.Error("want 1, got %d", counts["c1"])
	}
	if counts["c2"] != 2 {
		t.Error("want 2, got %d", counts["c2"])
	}
	f := CountersFunc(func() map[string]int64 {
		return map[string]int64{
			"c1": 1,
			"c2": 2,
		}
	})
	if f.String() != want1 && f.String() != want2 {
		t.Error("want %s or %s, got %s", want1, want2, f.String())
	}
}

func TestCountersHook(t *testing.T) {
	var gotname string
	var gotv *Counters
	clear()
	Register(func(name string, v expvar.Var) {
		gotname = name
		gotv = v.(*Counters)
	})

	v := NewCounters("counter2")
	if gotname != "counter2" {
		t.Error("want counter2, got %s", gotname)
	}
	if gotv != v {
		t.Error("want %#v, got %#v", v, gotv)
	}
}
