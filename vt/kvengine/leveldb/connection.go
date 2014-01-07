package leveldb

import (
	"bytes"
	"math"

	"github.com/jmhodges/levigo"
	"github.com/senarukana/rationaldb/vt/kvengine/proto"
)

type LevelDbConnection struct {
	id int64
	db *levigo.DB
}

func (connection *LevelDbConnection) Close() {
	connection.id = 0
	connection.db = nil
}

func (connection *LevelDbConnection) Id() int64 {
	return connection.id
}

func (connection *LevelDbConnection) Exists(options *proto.DbReadOptions, key []byte) (bool, error) {
	v, err := connection.Get(options, key)
	if err != nil {
		return false, err
	}
	if len(v) == 0 {
		return false, nil
	}
	return true, nil
}

func (connection *LevelDbConnection) Get(options *proto.DbReadOptions, key []byte) ([]byte, error) {
	ro := levigo.NewReadOptions()
	defer ro.Close()
	if options != nil {
		ro.SetFillCache(options.FillCache)
		ro.SetVerifyChecksums(options.VerifyChecksum)
		if options.Snapshot != nil {
			leveldbSnapshot := options.Snapshot.Snapshot.(*levigo.Snapshot)
			ro.SetSnapshot(leveldbSnapshot)
		}
	}
	return connection.db.Get(ro, key)
}

func (connection *LevelDbConnection) Gets(options *proto.DbReadOptions, keys [][]byte) (results [][]byte, err error) {
	ro := levigo.NewReadOptions()
	defer ro.Close()
	if options != nil {
		ro.SetFillCache(options.FillCache)
		ro.SetVerifyChecksums(options.VerifyChecksum)
		if options.Snapshot != nil {
			leveldbSnapshot := options.Snapshot.Snapshot.(*levigo.Snapshot)
			ro.SetSnapshot(leveldbSnapshot)
		} else {
			snap := connection.db.NewSnapshot()
			ro.SetSnapshot(snap)
		}
	}
	results = make([][]byte, 0, len(keys))
	for _, key := range keys {
		v, err := connection.db.Get(ro, key)
		if err != nil {
			return nil, err
		}
		results = append(results, v)
	}
	return results, nil
}

func (connection *LevelDbConnection) Put(options *proto.DbWriteOptions, key []byte, value []byte) error {
	wo := levigo.NewWriteOptions()
	defer wo.Close()
	if options != nil {
		wo.SetSync(options.Sync)
	}
	return connection.db.Put(wo, key, value)
}

func (connection *LevelDbConnection) Puts(options *proto.DbWriteOptions, keys [][]byte, values [][]byte) error {
	wo := levigo.NewWriteOptions()
	defer wo.Close()
	if options != nil {
		wo.SetSync(options.Sync)
	}
	batch := levigo.NewWriteBatch()
	defer batch.Close()
	for i, key := range keys {
		batch.Put(key, values[i])
	}
	return connection.db.Write(wo, batch)
}

func (connection *LevelDbConnection) Delete(options *proto.DbWriteOptions, key []byte) error {
	wo := levigo.NewWriteOptions()
	defer wo.Close()
	if options != nil {
		wo.SetSync(options.Sync)
	}
	return connection.db.Delete(wo, key)
}

func (connection *LevelDbConnection) Deletes(options *proto.DbWriteOptions, keys [][]byte) error {
	wo := levigo.NewWriteOptions()
	defer wo.Close()
	if options != nil {
		wo.SetSync(options.Sync)
	}
	batch := levigo.NewWriteBatch()
	defer batch.Close()
	for _, key := range keys {
		batch.Delete(key)
	}
	return connection.db.Write(wo, batch)
}

func (connection *LevelDbConnection) Snapshot() (*proto.DbSnapshot, error) {
	snap := &proto.DbSnapshot{Snapshot: connection.db.NewSnapshot()}
	return snap, nil
}

func (connection *LevelDbConnection) ReleaseSnapshot(snap *proto.DbSnapshot) error {
	rocksSnap := snap.Snapshot.(*levigo.Snapshot)
	connection.db.ReleaseSnapshot(rocksSnap)
	return nil
}

// TODO
type LevelDbCursor struct {
	start    []byte
	end      []byte
	limit    int
	index    int
	iter     *levigo.Iterator
	isClosed bool
}

func (connection *LevelDbConnection) Iterate(options *proto.DbReadOptions, start []byte, end []byte, limit int) (proto.DbCursor, error) {
	ro := levigo.NewReadOptions()
	if options != nil {
		ro.SetFillCache(options.FillCache)
		ro.SetVerifyChecksums(options.VerifyChecksum)
	}
	defer ro.Close()
	cursor := new(LevelDbCursor)
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

func (cursor *LevelDbCursor) Next() {
	if cursor.isClosed {
		panic("Cursor is closed")
	}
	cursor.index++
	cursor.iter.Next()
}

func (cursor *LevelDbCursor) Prev() {
	if cursor.isClosed {
		panic("Cursor is closed")
	}
	cursor.iter.Prev()
}

func (cursor *LevelDbCursor) Valid() bool {
	if cursor.end != nil {
		return cursor.iter.Valid() && cursor.index < cursor.limit && bytes.Compare(cursor.iter.Key(), cursor.end) <= 0
	}
	return cursor.iter.Valid() && cursor.index < cursor.limit
}

func (cursor *LevelDbCursor) Close() {
	cursor.iter.Close()
	cursor.isClosed = true
}

func (cursor *LevelDbCursor) Key() []byte {
	if !cursor.isClosed && cursor.iter.Valid() {
		return cursor.iter.Key()
	}
	return nil
}

func (cursor *LevelDbCursor) Value() []byte {
	if !cursor.isClosed && cursor.iter.Valid() {
		return cursor.iter.Value()
	}
	return nil
}

func (cursor *LevelDbCursor) Error() error {
	return cursor.iter.GetError()
}
