package proto

type DbRange struct {
	Start []byte
	End   []byte
}

type DbCursor interface {
	Next()
	Prev()
	Valid() bool
	Key() []byte
	Value() []byte
	Close()
	Error() error
}

type DbSnapshot struct {
	Snapshot interface{}
}

type DbTransactionEngine interface {
	Begin() (uint64, error)
	Commit(uint64) error
	RollBack(uint64) error

	Get(tid uint64, options *DbReadOptions, key []byte) (interface{}, error)
	Gets(tid uint64, options *DbReadOptions, key [][]byte) (interface{}, error)
	Put(tid uint64, options *DbWriteOptions, key []byte, value []byte) error
	Puts(tid uint64, options *DbWriteOptions, keys [][]byte, values [][]byte) error
	Set(tid uint64, options *DbWriteOptions, key []byte, value []byte) error
	Sets(tid uint64, options *DbWriteOptions, key [][]byte, values [][]byte) error
	Delete(tid uint64, options *DbWriteOptions, key []byte, value []byte) error
	Deletes(tid uint64, options *DbWriteOptions, key [][]byte, values [][]byte) error

	Iterate(tid uint64, options *DbReadOptions, start []byte, end []byte) error
}

type DbEngine interface {
	Name() string

	Init() error
	Close()
	// Remove everything stored in the engine
	Destroy() error

	Get(options *DbReadOptions, key []byte) ([]byte, error)
	Gets(options *DbReadOptions, key [][]byte) ([][]byte, error)
	Put(options *DbWriteOptions, key []byte, value []byte) error
	Puts(options *DbWriteOptions, keys [][]byte, values [][]byte) error
	Set(options *DbWriteOptions, key []byte, value []byte) error
	Sets(options *DbWriteOptions, key [][]byte, values [][]byte) error
	Delete(options *DbWriteOptions, key []byte) error
	Deletes(options *DbWriteOptions, key [][]byte) error

	Iterate(options *DbReadOptions, start []byte, end []byte) (DbCursor, error)
	Snapshot() (*DbSnapshot, error)
	ReleaseSnapshot(*DbSnapshot) error
}

/*type DbOperation interface {
	Operation(*DbOperationInoptions) (*DbOperationOutoptions, error)
}

type DbOperationInoptions struct {
	OpNum  int
	options *DbReadOptions
}

type DbOperationOutoptions struct {
	options *DbReadOptions
}

type DbGetInoptions struct {
	Key []byte
}

type DbGetOutoptions struct {
	Value []byte
}

type DbGetsInoptions struct {
	Keys [][]byte
}

type DbGetsOutoptions struct {
	Values []byte
}

type DbPutInoptions struct {
	Key   []byte
	Value []byte
}

type DbPutsOutoptions struct {
	Keys  [][]byte
	Value [][]byte
}

type DbSetInoptions struct {
	Key   []byte
	Value []byte
}

type DbSetsOutoptions struct {
	Keys   [][]byte
	Values [][]byte
}

type DbDeleteInoptions struct {
	Key []byte
}

type DbDeletesInoptions struct {
	Keys [][]byte
}
*/
