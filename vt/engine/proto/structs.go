package proto

import (
	"github.com/senarukana/rationaldb/sqltypes"
)

type Field struct {
	Name string
	Type int64
}

type FieldValue struct {
	Name  string
	Value sqltypes.Value
}

type QueryResult struct {
	Fields       []Field
	RowsAffected uint64
	InsertId     uint64
	Rows         [][]sqltypes.Value
}
