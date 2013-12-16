package proto

type DbReadOptions struct {
	Snapshot *DbSnapshot

	// RocksDbOptions
	VerifyChecksum bool
	FillCache      bool
}

type DbWriteOptions struct {
	// RocksDvOptions
	Sync       bool
	DisableWAL bool
}
