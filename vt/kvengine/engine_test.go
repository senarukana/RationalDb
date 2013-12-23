package kvengine

import (
	"bytes"
	"testing"

	"github.com/senarukana/rationaldb/vt/kvengine/proto"
)

func TestEngine(t *testing.T) {
	var err error
	var conn *DBConnection
	var data []byte
	e, err := NewEngine("rocksdb")
	if err != nil {
		t.Fatalf("New engine error")
	}
	conf := &proto.DBConfigs{DataPath: "testRocks"}
	e.Init(conf)
	if err != nil {
		t.Fatalf("init db error %v", err)
	}
	conn, err = e.Connect(&proto.DbConnectParams{DbName: "test", UserName: "ok"})
	if err != nil {
		t.Fatalf("Connect db error %v", err)
	}
	err = conn.Put(nil, []byte("k1"), []byte("v1"))
	if err != nil {
		t.Errorf("Put key error %v", err)
	}

	err = conn.Put(nil, []byte("n"), []byte("v2"))
	data, err = conn.Get(nil, []byte("k1"))
	if err != nil {
		t.Errorf("Get error")
	}
	if data == nil || bytes.Compare(data, []byte("v1")) != 0 {
		t.Errorf("get key k1 error")
	}
	var iter proto.DbCursor
	iter, err = conn.Iterate(nil, []byte("k"), []byte("m"), 0)
	if err != nil {
		t.Errorf("iter error %v", err)
	} else {
		for ; iter.Valid(); iter.Next() {
			if bytes.Compare(iter.Value(), []byte("v1")) != 0 {
				t.Error("iter error")
			}
		}
		if err = iter.Error(); err != nil {
			t.Errorf("iter error, %v", err)
		}
	}
	iter.Close()
	err = e.Destroy()
	if err != nil {
		t.Fatalf("Destory db error %v", err)
	}
}
