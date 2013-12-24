package schema

import (
	"strings"
	"sync"
	"sync/atomic"

	"github.com/senarukana/rationaldb/sqltypes"
	"github.com/senarukana/rationaldb/util/jscfg"
)

// Column categories
const (
	CAT_OTHER = iota
	CAT_NUMBER
	CAT_VARBINARY
)

const (
	TYPE_OTHER = iota
	TYPE_NUMERIC
	TYPE_FRACTIONAL
)

// Cache types
const (
	CACHE_NONE = 0
	CACHE_RW   = 1
	CACHE_W    = 2
)

type Table struct {
	Name      string
	Columns   []TableColumn
	Indexes   []*Index
	PKColumns []int
	CacheType int
}

func NewTable(name string) *Table {
	return &Table{
		Name:    name,
		Columns: make([]TableColumn, 0, 16),
		Indexes: make([]*Index, 0, 8),
	}
}

func (self *Table) Json() string {
	return jscfg.ToJson(self)
}

type TableColumn struct {
	Name     string
	Category int
	Type     int
	Nullable bool
	Default  sqltypes.Value
	IsPk     bool
	IsAuto   bool
	NextId   int64
	Mutex    *sync.Mutex
	IsUUID   bool
}

func (self *TableColumn) GetNextId() int64 {
	if !self.IsAuto {
		panic("GetNextI error, this column is not auto increment")
	}
	return atomic.AddInt64(&self.NextId, 1)
}

func (self *Table) AddColumn(name string, columnType string, defval sqltypes.Value, extra string, isPk bool) {
	index := len(self.Columns)
	self.Columns = append(self.Columns, TableColumn{Name: name})
	if strings.Contains(columnType, "int") {
		self.Columns[index].Category = CAT_NUMBER
	} else if strings.HasPrefix(columnType, "varbinary") {
		self.Columns[index].Category = CAT_VARBINARY
	} else {
		self.Columns[index].Category = CAT_OTHER
	}

	if extra == "auto_increment" {
		self.Columns[index].IsAuto = true
		self.Columns[index].Mutex = &sync.Mutex{}
		self.Columns[index].NextId = 0
		// Ignore default value, if any
		return
	}
	// isPk := true
	self.Columns[index].IsPk = isPk

	if defval.IsNull() {
		return
	}
	if self.Columns[index].Category == CAT_NUMBER {
		self.Columns[index].Default = sqltypes.MakeNumeric(defval.Raw())
	} else {
		self.Columns[index].Default = sqltypes.MakeString(defval.Raw())
	}
}

func (ta *Table) FindColumn(name string) int {
	for i, col := range ta.Columns {
		if col.Name == name {
			return i
		}
	}
	return -1
}

func (ta *Table) GetPk() *TableColumn {
	return &ta.Columns[0]
}

func (ta *Table) GetPKColumn(index int) *TableColumn {
	return &ta.Columns[ta.PKColumns[index]]
}

func (ta *Table) AddIndex(name string) (index *Index) {
	index = NewIndex(name)
	ta.Indexes = append(ta.Indexes, index)
	return index
}

type Index struct {
	Name        string
	Columns     []string
	Cardinality []uint64
	DataColumns []string
}

func NewIndex(name string) *Index {
	return &Index{name, make([]string, 0, 8), make([]uint64, 0, 8), nil}
}

func (self *Index) AddColumn(name string, cardinality uint64) {
	self.Columns = append(self.Columns, name)
	if cardinality == 0 {
		cardinality = uint64(len(self.Cardinality) + 1)
	}
	self.Cardinality = append(self.Cardinality, cardinality)
}

func (self *Index) FindColumn(name string) int {
	for i, colName := range self.Columns {
		if name == colName {
			return i
		}
	}
	return -1
}

func (self *Index) FindDataColumn(name string) int {
	for i, colName := range self.DataColumns {
		if name == colName {
			return i
		}
	}
	return -1
}
