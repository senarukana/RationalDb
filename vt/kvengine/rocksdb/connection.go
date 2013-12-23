package rocksdb

import (
	"bytes"
	"math"

	"github.com/senarukana/ratgo"
	"github.com/senarukana/rationaldb/log"
	"github.com/senarukana/rationaldb/vt/kvengine/proto"
)

type RocksDbConnection struct {
	id int64
	db *ratgo.DB
}

func (connection *RocksDbConnection) Close() {
	connection.id = 0
	connection.db = nil
}

func (connection *RocksDbConnection) Id() int64 {
	return connection.id
}

func (connection *RocksDbConnection) Get(options *proto.DbReadOptions, key []byte) ([]byte, error) {
	ro := ratgo.NewReadOptions()
	defer ro.Close()
	if options != nil {
		ro.SetFillCache(options.FillCache)
		ro.SetVerifyChecksums(options.VerifyChecksum)
		if options.Snapshot != nil {
			rocksdbSnapshot := options.Snapshot.Snapshot.(*ratgo.Snapshot)
			ro.SetSnapshot(rocksdbSnapshot)
		}
	}
	return connection.db.Get(ro, key)
}

func (connection *RocksDbConnection) Gets(options *proto.DbReadOptions, keys [][]byte) ([][]byte, error) {
	ro := ratgo.NewReadOptions()
	defer ro.Close()
	if options != nil {
		ro.SetFillCache(options.FillCache)
		ro.SetVerifyChecksums(options.VerifyChecksum)
		if options.Snapshot != nil {
			rocksdbSnapshot := options.Snapshot.Snapshot.(*ratgo.Snapshot)
			ro.SetSnapshot(rocksdbSnapshot)
		}
	}
	results, errors := connection.db.MultiGet(ro, keys)
	for _, err := range errors {
		if err != nil {
			log.Error("Get Key:%v error, error", err)
			return nil, err
		}
	}
	return results, nil
}

func (connection *RocksDbConnection) Put(options *proto.DbWriteOptions, key []byte, value []byte) error {
	wo := ratgo.NewWriteOptions()
	defer wo.Close()
	if options != nil {
		wo.SetSync(options.Sync)
		wo.SetDisableWAL(options.DisableWAL)
	}
	return connection.db.Put(wo, key, value)
}

func (connection *RocksDbConnection) Puts(options *proto.DbWriteOptions, keys [][]byte, values [][]byte) error {
	wo := ratgo.NewWriteOptions()
	defer wo.Close()
	if options != nil {
		wo.SetSync(options.Sync)
		wo.SetDisableWAL(options.DisableWAL)
	}
	batch := ratgo.NewWriteBatch()
	defer batch.Close()
	for i, key := range keys {
		batch.Put(key, values[i])
	}
	return connection.db.Write(wo, batch)
}

func (connection *RocksDbConnection) Delete(options *proto.DbWriteOptions, key []byte) error {
	wo := ratgo.NewWriteOptions()
	defer wo.Close()
	if options != nil {
		wo.SetSync(options.Sync)
		wo.SetDisableWAL(options.DisableWAL)
	}
	return connection.db.Delete(wo, key)
}

func (connection *RocksDbConnection) Deletes(options *proto.DbWriteOptions, keys [][]byte) error {
	wo := ratgo.NewWriteOptions()
	defer wo.Close()
	if options != nil {
		wo.SetSync(options.Sync)
		wo.SetDisableWAL(options.DisableWAL)
	}
	batch := ratgo.NewWriteBatch()
	defer batch.Close()
	for _, key := range keys {
		batch.Delete(key)
	}
	return connection.db.Write(wo, batch)
}

func (connection *RocksDbConnection) Snapshot() (*proto.DbSnapshot, error) {
	snap := &proto.DbSnapshot{Snapshot: connection.db.NewSnapshot()}
	return snap, nil
}

func (connection *RocksDbConnection) ReleaseSnapshot(snap *proto.DbSnapshot) error {
	rocksSnap := snap.Snapshot.(*ratgo.Snapshot)
	connection.db.ReleaseSnapshot(rocksSnap)
	return nil
}

// TODO
type RocksDbCursor struct {
	start    []byte
	end      []byte
	limit    int
	index    int
	iter     *ratgo.Iterator
	isClosed bool
}

func (connection *RocksDbConnection) Iterate(options *proto.DbReadOptions, start []byte, end []byte, limit int) (proto.DbCursor, error) {
	ro := ratgo.NewReadOptions()
	if options != nil {
		ro.SetFillCache(options.FillCache)
		ro.SetVerifyChecksums(options.VerifyChecksum)
	}
	defer ro.Close()
	cursor := new(RocksDbCursor)
	cursor.start = start
	cursor.end = end
	if limit == 0 {
		cursor.limit = int(math.MaxInt32)
	} else {
		cursor.limit = limit
	}
	cursor.iter = connection.db.NewIterator(ro)
	cursor.isClosed = false
	if start != nil {
		cursor.iter.Seek(start)
	}
	return cursor, nil
}

func (cursor *RocksDbCursor) Next() {
	if cursor.isClosed {
		panic("Cursor is closed")
	}
	cursor.index++
	cursor.iter.Next()
}

func (cursor *RocksDbCursor) Prev() {
	if cursor.isClosed {
		panic("Cursor is closed")
	}
	cursor.iter.Prev()
}

func (cursor *RocksDbCursor) Valid() bool {
	if cursor.end != nil {
		return cursor.iter.Valid() && cursor.index < cursor.limit && bytes.Compare(cursor.iter.Key(), cursor.end) <= 0
	}
	return cursor.iter.Valid() && cursor.index < cursor.limit
}

func (cursor *RocksDbCursor) Close() {
	cursor.iter.Close()
	cursor.isClosed = true
}

func (cursor *RocksDbCursor) Key() []byte {
	if !cursor.isClosed && cursor.iter.Valid() {
		return cursor.iter.Key()
	}
	return nil
}

func (cursor *RocksDbCursor) Value() []byte {
	if !cursor.isClosed && cursor.iter.Valid() {
		return cursor.iter.Value()
	}
	return nil
}

func (cursor *RocksDbCursor) Error() error {
	return cursor.iter.GetError()
}
