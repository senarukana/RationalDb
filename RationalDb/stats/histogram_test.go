// Copyright 2012, Google Inc. All rights reserved.
// Use of this source code is governed by a BSD-style
// license that can be found in the LICENSE file.

package stats

import (
	"expvar"
	"testing"
)

func TestHistogram(t *testing.T) {
	clear()
	h := NewHistogram("hist1", []int64{1, 5})
	for i := 0; i < 10; i++ {
		h.Add(int64(i))
	}
	want := `{"1": 2, "5": 4, "Max": 4, "Count": 10, "Total": 45}`
	if h.String() != want {
		t.Error("want %s, got %s", want, h.String())
	}
	counts := h.Counts()
	if counts["1"] != 2 {
		t.Error("want 2, got %d", counts["1"])
	}
	if counts["5"] != 4 {
		t.Error("want 4, got %d", counts["2"])
	}
	if counts["Max"] != 4 {
		t.Error("want 4, got %d", counts["Max"])
	}
	if h.Count() != 10 {
		t.Error("want 10, got %d", h.Count())
	}
	if h.CountLabel() != "Count" {
		t.Error("want Count, got %s", h.CountLabel())
	}
	if h.Total() != 45 {
		t.Error("want 45, got %d", h.Total())
	}
	if h.TotalLabel() != "Total" {
		t.Error("want Total, got %s", h.TotalLabel())
	}
}

func TestGenericHistogram(t *testing.T) {
	clear()
	h := NewGenericHistogram(
		"histgen",
		[]int64{1, 5},
		[]string{"one", "five", "max"},
		"count",
		"total",
	)
	want := `{"one": 0, "five": 0, "max": 0, "count": 0, "total": 0}`
	if h.String() != want {
		t.Error("want %s, got %s", want, h.String())
	}
}

func TestHistogramHook(t *testing.T) {
	var gotname string
	var gotv *Histogram
	clear()
	Register(func(name string, v expvar.Var) {
		gotname = name
		gotv = v.(*Histogram)
	})

	v := NewHistogram("hist2", []int64{1})
	if gotname != "hist2" {
		t.Error("want hist2, got %s", gotname)
	}
	if gotv != v {
		t.Error("want %#v, got %#v", v, gotv)
	}
}
