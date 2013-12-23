package proto

type DbReadOptions struct {
	Snapshot *DbSnapshot

	// RocksDbOptions
	VerifyChecksum bool
	FillCache      bool
}

type DbWriteOptions struct {
	New    bool // data should not exist
	Update bool // data should exist
	Sync   bool
	// RocksDvOptions
	DisableWAL bool
}
