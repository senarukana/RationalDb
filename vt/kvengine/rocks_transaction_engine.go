package kvengine

/*import (
	"bytes"
	"errors"
	"fmt"
	"math"
	"sync"

	"github.com/senarukana/ratgo"
	"github.com/senarukana/rationaldb/vt/kvengine/proto"

	"github.com/senarukana/rationaldb/log"
)

type RocksDBConfigs struct {
	DBConfigs

	CreateIfMissing   bool
	ParanoidCheck     bool
	LRUCacheSize      int
	BloomFilterLength int
}

type RocksDbError struct {
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
}

type rocksDbTransaction struct {
	tid  uint64
	snap *ratgo.Snapshot
	// modified keys during this transaction.
	modifiedKeys map[string]bool
}

type RocksDbReadOptions struct {
	VerifyChecksum bool
	FillCache      bool
}

type RocksDbWriteOptions struct {
	Sync       bool
	DisableWAL bool
}

type RocksDbEngine struct {
	config            *RocksDBConfigs
	transactions      map[uint64]*rocksDbTransaction
	mutex             sync.Mutex
	nextTransactionId uint64
	*ratgo.DB
}

func (self *RocksDbEngine) NextTransactionId() uint64 {
	self.mutex.Lock()
	defer self.mutex.Unlock()
	self.nextTransactionId++
	if self.nextTransactionId == math.MaxUint64 {
		self.nextTransactionId = 0
	}
	return self.nextTransactionId
}

func Init(config *RocksDBConfigs) *RocksDbEngine {
	rocksEngine := new(RocksDbEngine)
	rocksEngine.config = config

	options := ratgo.NewOptions()
	options.SetCreateIfMissing(config.CreateIfMissing)
	options.SetParanoidChecks(config.ParanoidCheck)
	if config.LRUCacheSize > 0 {
		options.SetCache(ratgo.NewLRUCache(config.LRUCacheSize))
	}
	if config.BloomFilterLength > 0 {
		options.SetFilterPolicy(ratgo.NewBloomFilter(config.BloomFilterLength))
	}

	db, err := ratgo.Open(config.DbName, options)
	if err != nil {
		panic(fmt.Sprintf("open rocksdb:%s failed, err %v", config.DbName, err))
	}
	rocksEngine.DB = db
	return rocksEngine
}

func (engine *RocksDbEngine) Begin() (uint64, error) {
	tid := engine.NextTransactionId()
	transaction := new(rocksDbTransaction)
	transaction.tid = tid
	transaction.snap = engine.DB.NewSnapshot()
	engine.transactions[tid] = transaction
	return tid, nil
}

func (engine *RocksDbEngine) Commit(tid uint64) error {
	log.Info("Transaction %v complete", tid)
	delete(engine.transactions, tid)
	return nil
}

func (engine *RocksDbEngine) Rollback(tid uint64) error {
	if tid == 0 {
		return fmt.Errorf("Invalid transaction id:%v", tid)
	}
	transaction := engine.transactions[tid]
	if transaction == nil {
		return fmt.Errorf("Invalid transaction id:%v", tid)
	}
	keys := make([][]byte, len(transaction.modifiedKeys))
	i := 0
	for k := range transaction.modifiedKeys {
		keys[i] = []byte(k)
		i++
	}
	ro := ratgo.NewReadOptions()
	ro.SetSnapshot(transaction.snap)

	valueSlices, errors := engine.DB.MultiGet(ro, keys)
	for _, err := range errors {
		if err != nil {
			log.Critical("rollback get data error, %v", err.Error())
			return err
		}
	}
	values := make([][]byte, len(valueSlices))
	for i, value := range valueSlices {
		values[i] = value.Data
	}
	err := engine.Puts(tid, nil, keys, values)
	if err != nil {
		log.Critical("rollback put back data error, %v", err.Error())
		return err
	}
	return nil
}

func (engine *RocksDbEngine) Get(tid uint64, params interface{}, key []byte) (interface{}, error) {
	ro := ratgo.NewReadOptions()
	defer ro.Close()
	if params != nil {
		if readOptions, ok := params.(*RocksDbReadOptions); !ok {
			log.Error("RocksDb Get Operation comes an invalid readoptions")
			return nil, errors.New("invalid ReadOptions")
		} else {
			ro.SetFillCache(readOptions.FillCache)
			ro.SetVerifyChecksums(readOptions.VerifyChecksum)
		}
	}
	if tid != 0 {
		transaction := engine.transactions[tid]
		if transaction == nil {
			return nil, fmt.Errorf("Invalid transaction id:%d", tid)
		}
		ro.SetSnapshot(transaction.snap)
	}
	return engine.DB.Get(ro, key)
}

func (engine *RocksDbEngine) Gets(tid uint64, params interface{}, keys [][]byte) (interface{}, error) {
	ro := ratgo.NewReadOptions()
	defer ro.Close()
	if params != nil {
		if readOptions, ok := params.(*RocksDbReadOptions); !ok {
			log.Error("RocksDb Gets Operation comes an invalid readoptions")
			return nil, errors.New("invalid ReadOptions")
		} else {
			ro.SetFillCache(readOptions.FillCache)
			ro.SetVerifyChecksums(readOptions.VerifyChecksum)
		}
	}
	if tid != 0 {
		transaction := engine.transactions[tid]
		if transaction == nil {
			return nil, fmt.Errorf("Invalid transaction id:%d", tid)
		}
		ro.SetSnapshot(transaction.snap)
	}
	results, errors := engine.DB.MultiGet(ro, keys)
	for _, err := range errors {
		if err != nil {
			return nil, err
		}
	}
	return results, nil
}

func (engine *RocksDbEngine) Put(tid uint64, params interface{}, key []byte, value []byte) error {
	wo := ratgo.NewWriteOptions()
	defer wo.Close()
	if params != nil {
		if writeOptions, ok := params.(*RocksDbWriteOptions); !ok {
			log.Error("RocksDb Put Operation comes an invalid readoptions")
			return errors.New("invalid ReadOptions")
		} else {
			wo.SetSync(writeOptions.Sync)
			wo.SetDisableWAL(writeOptions.DisableWAL)
		}
	}
	if tid != 0 {
		transaction := engine.transactions[tid]
		if transaction == nil {
			return fmt.Errorf("Invalid transaction id:%d", tid)
		}
		// if not change the key previously, push it to the modifieddKeys
		ks := string(key)
		if !transaction.modifiedKeys[ks] {
			transaction.modifiedKeys[ks] = true
		}
	}
	return engine.DB.Put(wo, key, value)
}

func (engine *RocksDbEngine) Puts(tid uint64, params interface{}, keys [][]byte, values [][]byte) error {
	wo := ratgo.NewWriteOptions()
	defer wo.Close()
	if params != nil {
		if writeOptions, ok := params.(*RocksDbWriteOptions); !ok {
			log.Error("RocksDb Put Operation comes an invalid readoptions")
			return errors.New("invalid ReadOptions")
		} else {
			wo.SetSync(writeOptions.Sync)
			wo.SetDisableWAL(writeOptions.DisableWAL)
		}
	}
	batch := ratgo.NewWriteBatch()
	defer batch.Close()
	for i, key := range keys {
		batch.Put(key, values[i])
	}
	if tid != 0 {
		transaction := engine.transactions[tid]
		if transaction == nil {
			return fmt.Errorf("Invalid transaction id:%d", tid)
		}
		for _, key := range keys {
			ks := string(key)
			// if not change the key previously, push it to the modifieddKeys
			if !transaction.modifiedKeys[ks] {
				transaction.modifiedKeys[ks] = true
			}
		}
	}
	return engine.DB.Write(wo, batch)
}

func (engine *RocksDbEngine) Set(tid uint64, params interface{}, key []byte, value []byte) error {
	return engine.Put(tid, params, key, value)
}

func (engine *RocksDbEngine) Sets(tid uint64, params interface{}, keys [][]byte, values [][]byte) error {
	return engine.Puts(tid, params, keys, values)
}

func (engine *RocksDbEngine) Delete(tid uint64, params interface{}, key []byte) error {
	wo := ratgo.NewWriteOptions()
	defer wo.Close()
	if params != nil {
		if writeOptions, ok := params.(*RocksDbWriteOptions); !ok {
			log.Error("RocksDb Put Operation comes an invalid readoptions")
			return errors.New("invalid ReadOptions")
		} else {
			wo.SetSync(writeOptions.Sync)
			wo.SetDisableWAL(writeOptions.DisableWAL)
		}
	}
	if tid != 0 {
		transaction := engine.transactions[tid]
		if transaction == nil {
			return fmt.Errorf("Invalid transaction id:%d", tid)
		}
		// if not change the key previously, push it to the modifieddKeys
		ks := string(key)
		if !transaction.modifiedKeys[ks] {
			transaction.modifiedKeys[ks] = true
		}
	}
	return engine.DB.Delete(wo, key)
}

func (engine *RocksDbEngine) Deletes(tid uint64, params interface{}, keys [][]byte, values [][]byte) error {
	wo := ratgo.NewWriteOptions()
	defer wo.Close()
	if params != nil {
		if writeOptions, ok := params.(*RocksDbWriteOptions); !ok {
			log.Error("RocksDb Put Operation comes an invalid readoptions")
			return errors.New("invalid ReadOptions")
		} else {
			wo.SetSync(writeOptions.Sync)
			wo.SetDisableWAL(writeOptions.DisableWAL)
		}
	}
	batch := ratgo.NewWriteBatch()
	defer batch.Close()
	for _, key := range keys {
		batch.Delete(key)
	}
	if tid != 0 {
		transaction := engine.transactions[tid]
		if transaction == nil {
			return fmt.Errorf("Invalid transaction id:%d", tid)
		}
		for _, key := range keys {
			// if not change the key previously, push it to the modifieddKeys
			ks := string(key)
			if !transaction.modifiedKeys[ks] {
				transaction.modifiedKeys[ks] = true
			}
		}
	}
	return engine.DB.Write(wo, batch)
}

// TODO
type RocksDbCursor struct {
	keyRange *proto.DbRange
	iter     *ratgo.Iterator
	isClosed bool
}

func (cursor *RocksDbCursor) Value() interface{} {
	if cursor.isClosed {
		return nil
	}
	if cursor.iter.Valid() {
		return cursor.iter.Value()
	}
	return nil
}

func (cursor *RocksDbCursor) Next() *RocksDbCursor {
	cursor.iter.Next()
	if cursor.iter.Valid() && bytes.Compare(cursor.iter.Key().Data, cursor.keyRange.End) < 0 {
		return cursor
	} else {
		return nil
	}
}

func (cursor *RocksDbCursor) Close() {
	cursor.iter.Close()
	cursor.isClosed = true
}

func (engine *RocksDbEngine) Iterate(tid uint64, params interface{}, keyRange *proto.DbRange) (cursor *RocksDbCursor, err error) {
	var readOptions *RocksDbReadOptions
	var ok bool
	if readOptions, ok = params.(*RocksDbReadOptions); !ok {
		log.Error("RocksDb Get Operation comes an invalid readoptions")
		return nil, errors.New("invalid ReadOptions")
	}
	ro := ratgo.NewReadOptions()
	ro.SetFillCache(readOptions.FillCache)
	ro.SetVerifyChecksums(readOptions.VerifyChecksum)
	defer ro.Close()
	if tid != 0 {
		transaction := engine.transactions[tid]
		if transaction == nil {
			return nil, fmt.Errorf("Invalid transaction id:%d", tid)
		}
		ro.SetSnapshot(transaction.snap)
	}
	cursor = new(RocksDbCursor)
	cursor.keyRange = keyRange
	cursor.iter = engine.DB.NewIterator(ro)
	cursor.isClosed = false
	return cursor, nil
}
*/
