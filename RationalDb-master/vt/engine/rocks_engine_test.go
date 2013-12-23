package engine

import (
	"bytes"
	"testing"

	"github.com/senarukana/rationaldb/vt/engine/proto"
)

func TestRocksEngine(t *testing.T) {
	// init
	conf := new(DBConfigs)
	conf.DbName = "test"
	engine := GetEngine("rocksdb", conf)
	if engine.Name() != "rocksdb" {
		t.Fatalf("want engine name RocksDb, but result is %v\n", engine.Name())
	}
	var err error

	err = engine.Init()
	if err != nil {
		t.Fatalf("Init engine error, %v\n", err.Error())
	}

	var result []byte
	var results [][]byte

	k1 := []byte("user1")
	k2 := []byte("user2")
	k3 := []byte("user3")
	k4 := []byte("user4")
	// k5 := []byte("user5")
	v1 := []byte("value1")
	v2 := []byte("value2")
	v3 := []byte("value3")
	// v4 := []byte("value4")
	// v5 := []byte("value5")

	wo := new(proto.DbWriteOptions)
	ro := new(proto.DbReadOptions)
	// Put
	err = engine.Put(wo, k1, v1)
	if err != nil {
		t.Errorf("Put Key : %v error, %v\n", string(k1), err.Error())
	}

	err = engine.Puts(wo, [][]byte{k2, k3}, [][]byte{v2, v3})
	if err != nil {
		t.Errorf("Puts Key error, %v\n", err.Error())
	}

	// Get
	result, err = engine.Get(ro, k1)
	if err != nil {
		t.Errorf("Get key : %v error, %v\n", string(k1), err.Error())
	}
	if bytes.Compare(result, v1) != 0 {
		t.Errorf("Expect %v=%v, but is %v\n", string(k1), string(result), string(v1))
	}

	results, err = engine.Gets(ro, [][]byte{k2, k3})
	if err != nil {
		t.Errorf("Gets error, %v\n", err.Error())
	}

	if bytes.Compare(results[0], v2) != 0 || bytes.Compare(results[1], v3) != 0 {
		t.Error("Gets result error")
	}

	// Iterate
	iter, err := engine.Iterate(ro, k2, k4)
	count := 0
	for ; iter.Valid(); iter.Next() {
		count++
	}

	if count != 2 {
		t.Errorf("Iterate error, expect 2, result is %d\n", count)
	}
	iter.Close()

	engine.Close()

	err = engine.Destroy()
	if err != nil {
		t.Fatal("Destory Db error")
	}

}
