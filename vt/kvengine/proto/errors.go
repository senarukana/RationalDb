package proto

import (
	"errors"
)

var (
	ErrUnknownDBEngineName = errors.New("unknown db engine")
	ErrNoEngineConfig      = errors.New("not provide engine config")
	ErrDbInitError         = errors.New("init rocksdb error")
)
