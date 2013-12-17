package engine

import (
	"bytes"

	"github.com/senarukana/ratgo"
	"github.com/senarukana/rationaldb/log"
	"github.com/senarukana/rationaldb/vt/engine/proto"
)

/*type RocksDbError struct {
	Message string
	Query   string
}

func NewRocksDbError(number int, format string, args ...interface{}) *RocksDbError {
	return &RocksDbError{Num: number, Message: fmt.Sprintf(format, args...)}
}

func (se *RocksDbError) Error() string {
	if se.Query == "" {
		return fmt.Sprintf("%v (errno %v)", se.Message, se.Num)
	}
	return fmt.Sprintf("%v (errno %v) during query: %s", se.Message, se.Num, se.Query)
}

func (se *RocksDbError) Number() int {
	return se.Num
}

func handleError(err *error) {
	if x := recover(); x != nil {
		terr := x.(*RocksDbError)
		*err = terr
	}
}*/

type RocksDbEngine struct {
	config    *proto.DBConfigs
	dbOptions *ratgo.Options
	*ratgo.DB
}

var DefaultRocksDbConf = &proto.RocksDbConfigs{
	CreateIfMissing:   true,
	ParanoidCheck:     false,
	LRUCacheSize:      2 >> 10,
	BloomFilterLength: 0,
}

func NewRocksDbEngine(config *proto.DBConfigs) proto.DbEngine {
	if config == nil {
		panic("Not provide engine config")
	}
	if config.RocksDbConfigs == nil {
		config.RocksDbConfigs = DefaultRocksDbConf
	}
	rocksEngine := new(RocksDbEngine)
	options := ratgo.NewOptions()
	options.SetCreateIfMissing(config.CreateIfMissing)
	options.SetParanoidChecks(config.ParanoidCheck)
	if config.LRUCacheSize > 0 {
		options.SetCache(ratgo.NewLRUCache(config.LRUCacheSize))
	}
	if config.BloomFilterLength > 0 {
		options.SetFilterPolicy(ratgo.NewBloomFilter(config.BloomFilterLength))
	}
	rocksEngine.config = config
	rocksEngine.dbOptions = options
	return rocksEngine
}

func (engine *RocksDbEngine) Init() error {
	db, err := ratgo.Open(engine.config.DataPath, engine.dbOptions)
	if err != nil {
		log.Critical("Open Rocksdb Error")
		return ErrDbInitError
	}
	log.Info("Init Engine %v complete", engine.Name())
	engine.DB = db
	return nil
}

func (engine *RocksDbEngine) Name() string {
	return "rocksdb"
}

func (engine *RocksDbEngine) Close() {
	engine.DB.Close()
}

func (engine *RocksDbEngine) Destroy() error {
	return ratgo.DestroyDatabase(engine.config.DataPath, engine.dbOptions)
}

func (engine *RocksDbEngine) Get(options *proto.DbReadOptions, key []byte) ([]byte, error) {
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
	return engine.DB.Get(ro, key)
}

func (engine *RocksDbEngine) Gets(options *proto.DbReadOptions, keys [][]byte) ([][]byte, error) {
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
	results, errors := engine.DB.MultiGet(ro, keys)
	for _, err := range errors {
		if err != nil {
			log.Error("Get Key:%v error, error", err)
			return nil, err
		}
	}
	return results, nil
}

func (engine *RocksDbEngine) Put(options *proto.DbWriteOptions, key []byte, value []byte) error {
	wo := ratgo.NewWriteOptions()
	defer wo.Close()
	if options != nil {
		wo.SetSync(options.Sync)
		wo.SetDisableWAL(options.DisableWAL)
	}
	return engine.DB.Put(wo, key, value)
}

func (engine *RocksDbEngine) Puts(options *proto.DbWriteOptions, keys [][]byte, values [][]byte) error {
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
	return engine.DB.Write(wo, batch)
}

func (engine *RocksDbEngine) Delete(options *proto.DbWriteOptions, key []byte) error {
	wo := ratgo.NewWriteOptions()
	defer wo.Close()
	if options != nil {
		wo.SetSync(options.Sync)
		wo.SetDisableWAL(options.DisableWAL)
	}
	return engine.DB.Delete(wo, key)
}

func (engine *RocksDbEngine) Deletes(options *proto.DbWriteOptions, keys [][]byte) error {
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
	return engine.DB.Write(wo, batch)
}

func (engine *RocksDbEngine) Snapshot() (*proto.DbSnapshot, error) {
	snap := &proto.DbSnapshot{Snapshot: engine.DB.NewSnapshot()}
	return snap, nil
}

func (engine *RocksDbEngine) ReleaseSnapshot(snap *proto.DbSnapshot) error {
	rocksSnap := snap.Snapshot.(*ratgo.Snapshot)
	engine.DB.ReleaseSnapshot(rocksSnap)
	return nil
}

// TODO
type RocksDbCursor struct {
	start    []byte
	end      []byte
	iter     *ratgo.Iterator
	isClosed bool
}

func (engine *RocksDbEngine) Iterate(options *proto.DbReadOptions, start []byte, end []byte) (proto.DbCursor, error) {
	ro := ratgo.NewReadOptions()
	ro.SetFillCache(options.FillCache)
	ro.SetVerifyChecksums(options.VerifyChecksum)
	defer ro.Close()
	cursor := new(RocksDbCursor)
	cursor.start = start
	cursor.end = end
	cursor.iter = engine.DB.NewIterator(ro)
	cursor.isClosed = false
	if start != nil {
		cursor.iter.Seek(start)
	}
	return cursor, nil
}

func (cursor *RocksDbCursor) Next() {
	if !cursor.isClosed {
		cursor.iter.Next()
	}
}

func (cursor *RocksDbCursor) Prev() {
	cursor.iter.Prev()
}

func (cursor *RocksDbCursor) Valid() bool {
	if cursor.end != nil {
		return cursor.iter.Valid() && bytes.Compare(cursor.iter.Key(), cursor.end) <= 0
	}
	return cursor.iter.Valid()
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

func init() {
	Register("rocksdb", NewRocksDbEngine)
}
