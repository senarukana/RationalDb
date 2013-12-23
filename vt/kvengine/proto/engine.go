package proto

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
	Init(*DBConfigs) error
	Connect(*DbConnectParams) (DbConnection, error)
	Shutdown() error
	Destroy() error
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
