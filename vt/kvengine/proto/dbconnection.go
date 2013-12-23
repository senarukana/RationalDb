package proto

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

type DbConnection interface {
	Id() int64
	Close()

	Get(options *DbReadOptions, key []byte) ([]byte, error)
	Gets(options *DbReadOptions, key [][]byte) ([][]byte, error)

	// Put the data in the engine.
	// If the key is already existed, It will replace it.
	Put(options *DbWriteOptions, key, value []byte) error
	Puts(options *DbWriteOptions, keys, values [][]byte) error

	Delete(options *DbWriteOptions, key []byte) error
	Deletes(options *DbWriteOptions, key [][]byte) error

	Iterate(options *DbReadOptions, start []byte, end []byte, limit int) (DbCursor, error)
	Snapshot() (*DbSnapshot, error)
	ReleaseSnapshot(snap *DbSnapshot) error
}
