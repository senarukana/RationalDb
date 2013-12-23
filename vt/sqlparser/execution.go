// Copyright 2012, Google Inc. All rights reserved.
// Use of this source code is governed by a BSD-style
// license that can be found in the LICENSE file.

package sqlparser

import (
	"fmt"
	"strconv"

	log "github.com/golang/glog"
	"github.com/youtube/vitess/go/sqltypes"
	"sqlProject/schema"
	// "github.com/youtube/vitess/go/vt/schema"
)

type PlanType int

const (
	PLAN_PASS_SELECT PlanType = iota
	PLAN_PASS_DML
	PLAN_PK_EQUAL
	PLAN_PK_IN
	PLAN_SELECT_SUBQUERY
	PLAN_DML_PK
	PLAN_DML_SUBQUERY
	PLAN_INSERT_PK
	PLAN_INSERT_SUBQUERY
	PLAN_SET
	PLAN_DDL
	NumPlans
)

// Must exactly match order of plan constants.
var planName = []string{
	"PASS_SELECT",
	"PK_EQUAL",
	"PK_IN",
	"SELECT_SUBQUERY",
	"DML_PK",
	"DML_SUBQUERY",
	"INSERT_PK",
	"INSERT_SUBQUERY",
	"SET",
	"DDL",
}

func (pt PlanType) String() string {
	return planName[pt]
}

func PlanByName(s string) (pt PlanType, ok bool) {
	for i, v := range planName {
		if v == s {
			return PlanType(i), true
		}
	}
	return NumPlans, false
}

func (pt PlanType) IsSelect() bool {
	return pt == PLAN_PASS_SELECT || pt == PLAN_PK_EQUAL || pt == PLAN_PK_IN || pt == PLAN_SELECT_SUBQUERY
}

func (pt PlanType) MarshalJSON() ([]byte, error) {
	return ([]byte)(fmt.Sprintf("\"%s\"", pt.String())), nil
}

type ReasonType int

const (
	REASON_DEFAULT ReasonType = iota
	REASON_SELECT
	REASON_TABLE
	REASON_NOCACHE
	REASON_SELECT_LIST
	REASON_FOR_UPDATE
	REASON_WHERE
	REASON_ORDER
	REASON_PKINDEX
	REASON_NOINDEX_MATCH
	REASON_TABLE_NOINDEX
	REASON_PK_CHANGE
	REASON_COMPOSITE_PK
	REASON_HAS_HINTS
)

// Must exactly match order of reason constants.
var reasonName = []string{
	"DEFAULT",
	"SELECT",
	"TABLE",
	"NOCACHE",
	"SELECT_LIST",
	"FOR_UPDATE",
	"WHERE",
	"ORDER",
	"PKINDEX",
	"NOINDEX_MATCH",
	"TABLE_NOINDEX",
	"PK_CHANGE",
	"COMPOSITE_PK",
	"HAS_HINTS",
}

func (rt ReasonType) String() string {
	return reasonName[rt]
}

func (rt ReasonType) MarshalJSON() ([]byte, error) {
	return ([]byte)(fmt.Sprintf("\"%s\"", rt.String())), nil
}

// ExecPlan is built for selects and DMLs.
// PK Values values within ExecPlan can be:
// sqltypes.Value: sourced form the query, or
// string: bind variable name starting with ':', or
// nil if no value was specified
type ExecPlan struct {
	PlanId    PlanType
	Reason    ReasonType
	TableName string

	// FieldQuery is used to fetch field info
	FieldQuery *ParsedQuery

	// FullQuery will be set for all plans.
	FullQuery *ParsedQuery

	// For PK plans, only OuterQuery is set.
	// For SUBQUERY plans, Subquery is also set.
	// IndexUsed is set only for PLAN_SELECT_SUBQUERY
	OuterQuery *ParsedQuery
	Subquery   *ParsedQuery
	IndexUsed  string

	// For selects, columns to be returned
	// For PLAN_INSERT_SUBQUERY, columns to be inserted
	ColumnNumbers []int


	// ColumnNames  []string
	// ColumnValues []interface{}

	// columns_num int
	// Columns     map[string]interface{}
	// Columns []map[string]sqltypes.Value
	// PKColumns map[string]interface{}

	RowColumns []map[string]interface{}


	// PLAN_PK_EQUAL, PLAN_DML_PK: where clause values
	// PLAN_PK_IN: IN clause values
	// PLAN_INSERT_PK: values clause
	PKValues []interface{}

	// For update: set clause
	// For insert: on duplicate key clause
	SecondaryPKValues []interface{}

	// For PLAN_INSERT_SUBQUERY: pk columns in the subquery result
	SubqueryPKColumns []int

	// PLAN_SET
	SetKey   string
	SetValue interface{}
}

type DDLPlan struct {
	Action    int
	TableName string
	NewName   string
}

type TableGetter func(tableName string) (*schema.Table, bool)

func ExecParse(sql string, getTable TableGetter) (plan *ExecPlan, err error) {
	defer handleError(&err)

	fmt.Println("sql: ", sql)

	tree, err := Parse(sql)
	// fmt.Println("tree: ", tree.Type)
	// fmt.Println("tree-value-----------------------", tree.Value)
	// fmt.Println("tree-type-----------------------", tree.Sub[3].Sub[0].Sub)

	sub := tree.Sub
	for i := 0; i < len(sub); i++ {
		fmt.Println(sub[i])
		fmt.Println(sub[i].Sub)
	}

	if err != nil {
		return nil, err
	}
	plan = tree.execAnalyzeSql(getTable)
	if plan.PlanId == PLAN_PASS_DML {
		log.Warningf("PASS_DML: %s", sql)
	}
	return plan, nil
}

func StreamExecParse(sql string) (fullQuery *ParsedQuery, err error) {
	defer handleError(&err)

	tree, err := Parse(sql)
	if err != nil {
		return nil, err
	}

	switch tree.Type {
	case SELECT:
		if tree.At(SELECT_FOR_UPDATE_OFFSET).Type == FOR_UPDATE {
			return nil, NewParserError("Select for Update Disallowed with streaming")
		}
	case UNION, UNION_ALL, MINUS, EXCEPT, INTERSECT:
	default:
		return nil, NewParserError("%s not allowed for streaming", string(tree.Value))
	}

	return tree.GenerateFullQuery(), nil
}

func DDLParse(sql string) (plan *DDLPlan) {
	rootNode, err := Parse(sql)
	if err != nil {
		return &DDLPlan{Action: 0}
	}
	switch rootNode.Type {
	case CREATE, ALTER, DROP:
		return &DDLPlan{
			Action:    rootNode.Type,
			TableName: string(rootNode.At(0).Value),
			NewName:   string(rootNode.At(0).Value),
		}
	case RENAME:
		return &DDLPlan{
			Action:    rootNode.Type,
			TableName: string(rootNode.At(0).Value),
			NewName:   string(rootNode.At(1).Value),
		}
	}
	return &DDLPlan{Action: 0}
}

//-----------------------------------------------
// Implementation

func (node *Node) execAnalyzeSql(getTable TableGetter) (plan *ExecPlan) {
	switch node.Type {
	case SELECT, UNION, UNION_ALL, MINUS, EXCEPT, INTERSECT:
		return node.execAnalyzeSelect(getTable)
	case INSERT:
		return node.execAnalyzeInsert(getTable)
	case UPDATE:
		return node.execAnalyzeUpdate(getTable)
	case DELETE:
		return node.execAnalyzeDelete(getTable)
	case SET:
		return node.execAnalyzeSet()
	case CREATE, ALTER, DROP, RENAME:
		return &ExecPlan{PlanId: PLAN_DDL}
	}
	panic(NewParserError("Invalid SQL"))
}

func (node *Node) execAnalyzeSelect(getTable TableGetter) (plan *ExecPlan) {
	// Default plan
	plan = &ExecPlan{PlanId: PLAN_PASS_SELECT, FieldQuery: node.GenerateFieldQuery(), FullQuery: node.GenerateSelectLimitQuery()}

	// There are bind variables in the SELECT list
	if plan.FieldQuery == nil {
		plan.Reason = REASON_SELECT_LIST
		return plan
	}

	if !node.execAnalyzeSelectStructure() {
		plan.Reason = REASON_SELECT
		return plan
	}

	// from
	tableName, hasHints := node.At(SELECT_FROM_OFFSET).execAnalyzeFrom()
	if tableName == "" {
		plan.Reason = REASON_TABLE
		return plan
	}
	tableInfo := plan.setTableInfo(tableName, getTable)

	// Don't improve the plan if the select is for update
	if node.At(SELECT_FOR_UPDATE_OFFSET).Type == FOR_UPDATE {
		plan.Reason = REASON_FOR_UPDATE
		return plan
	}

	// Further improvements possible only if table is row-cached
	// if tableInfo.CacheType == schema.CACHE_NONE || tableInfo.CacheType == schema.CACHE_W {
	// 	plan.Reason = REASON_NOCACHE
	// 	return plan
	// }

	// Select expressions
	selects := node.At(SELECT_EXPR_OFFSET).execAnalyzeSelectExpressions(tableInfo)
	if selects == nil {
		plan.Reason = REASON_SELECT_LIST
		return plan
	}
	plan.ColumnNumbers = selects

	// where
	conditions := node.At(SELECT_WHERE_OFFSET).execAnalyzeWhere()
	if conditions == nil {
		plan.Reason = REASON_WHERE
		return plan
	}
	fmt.Println(".......conditions: ", conditions)
	// order
	if node.At(SELECT_ORDER_OFFSET).Len() != 0 {
		plan.Reason = REASON_ORDER
		return plan
	}

	// This check should never fail because we only cache tables with primary keys.
	if len(tableInfo.Indexes) == 0 || tableInfo.Indexes[0].Name != "PRIMARY" {
		panic("unexpected")
	}
	fmt.Println("...hehe")
	// Attempt PK match only if there's no limit clause
	if node.At(SELECT_LIMIT_OFFSET).Len() == 0 {
		planId, pkValues := getSelectPKValues(conditions, tableInfo.Indexes[0])
		switch planId {
		case PLAN_PK_EQUAL:
			plan.PlanId = PLAN_PK_EQUAL
			plan.OuterQuery = node.GenerateEqualOuterQuery(tableInfo)
			plan.PKValues = pkValues
			return plan
		case PLAN_PK_IN:
			plan.PlanId = PLAN_PK_IN
			plan.OuterQuery = node.GenerateInOuterQuery(tableInfo)
			plan.PKValues = pkValues
			return plan
		}
	}
	fmt.Println("...memeda")

	if len(tableInfo.Indexes[0].Columns) != 1 {
		plan.Reason = REASON_COMPOSITE_PK
		return plan
	}

	// TODO: Analyze hints to improve plan.
	if hasHints {
		plan.Reason = REASON_HAS_HINTS
		return plan
	}

	plan.IndexUsed = getIndexMatch(conditions, tableInfo.Indexes)
	if plan.IndexUsed == "" {
		plan.Reason = REASON_NOINDEX_MATCH
		return plan
	}
	if plan.IndexUsed == "PRIMARY" {
		plan.Reason = REASON_PKINDEX
		return plan
	}
	// TODO: We can further optimize. Change this to pass-through if select list matches all columns in index.
	plan.PlanId = PLAN_SELECT_SUBQUERY
	plan.OuterQuery = node.GenerateInOuterQuery(tableInfo)
	plan.Subquery = node.GenerateSelectSubquery(tableInfo, plan.IndexUsed)
	return plan
}

func (node *Node) execAnalyzeInsert(getTable TableGetter) (plan *ExecPlan) {
	plan = &ExecPlan{PlanId: PLAN_PASS_DML, FullQuery: node.GenerateFullQuery()}
	tableName := string(node.At(INSERT_TABLE_OFFSET).Value)
	tableInfo := plan.setTableInfo(tableName, getTable)


	// if len(tableInfo.Indexes) == 0 || tableInfo.Indexes[0].Name != "PRIMARY" {
	// 	log.Warningf("no primary key for table %s", tableName)
	// 	fmt.Println("no primary key for table %s", tableName)
	// 	plan.Reason = REASON_TABLE_NOINDEX
	// 	return plan
	// }


	pkColumnNumbers := node.At(INSERT_COLUMN_LIST_OFFSET).getInsertPKColumns(tableInfo)
	fmt.Println("pkColumnNumbers: ", pkColumnNumbers)

	// ColumnNames := node.At(INSERT_COLUMN_LIST_OFFSET).getInsertColumns(tableInfo)
	// plan.ColumnNames = ColumnNames
	// fmt.Println("ColumnNames: ", plan.ColumnNames)

	rowValues := node.At(INSERT_VALUES_OFFSET) // VALUES/SELECT
	if rowValues.Type == SELECT {
		plan.PlanId = PLAN_INSERT_SUBQUERY
		plan.OuterQuery = node.GenerateInsertOuterQuery()
		plan.Subquery = rowValues.GenerateSelectLimitQuery()
		// Column list syntax is a subset of select expressions
		// if node.At(INSERT_COLUMN_LIST_OFFSET).Len() != 0 {
		// 	plan.ColumnNumbers = node.At(INSERT_COLUMN_LIST_OFFSET).execAnalyzeSelectExpressions(tableInfo)
		// } else {
		// 	// SELECT_STAR node will expand into all columns
		// 	n := NewSimpleParseNode(NODE_LIST, "")
		// 	n.Push(NewSimplseParseNode(SELECT_STAR, "*"))
		// 	plan.ColumnNumbers = n.execAnalyzeSelectExpressions(tableInfo)
		// }
		// plan.SubqueryPKColumns = pkColumnNumbers
		return plan
	}

	rowList := rowValues.At(0) // VALUES->NODE_LIST
	if pkValues := getInsertPKValues(pkColumnNumbers, rowList, tableInfo); pkValues != nil {
		plan.PlanId = PLAN_INSERT_PK
		plan.OuterQuery = plan.FullQuery
		plan.PKValues = pkValues
	}
	// if ColumnValues := getInsertValues(ColumnNames, rowList, tableInfo); ColumnValues != nil {
	// 	plan.ColumnValues = ColumnValues
	// 	fmt.Println("ColumnValues: ", plan.ColumnValues)
	// }

	if rowColumns := node.At(INSERT_COLUMN_LIST_OFFSET).getInsertColumnValue(tableInfo, rowList); rowColumns != nil {
		plan.RowColumns = rowColumns
		log.Info("row column is %d", len(rowColumns))
	}

	plan.TableName = tableName

	return plan
}

func (node *Node) execAnalyzeUpdate(getTable TableGetter) (plan *ExecPlan) {
	// Default plan
	plan = &ExecPlan{PlanId: PLAN_PASS_DML, FullQuery: node.GenerateFullQuery()}

	tableName := string(node.At(UPDATE_TABLE_OFFSET).Value)
	tableInfo := plan.setTableInfo(tableName, getTable)

	if len(tableInfo.Indexes) == 0 || tableInfo.Indexes[0].Name != "PRIMARY" {
		log.Warningf("no primary key for table %s", tableName)
		plan.Reason = REASON_TABLE_NOINDEX
		return plan
	}

	var ok bool
	if plan.SecondaryPKValues, ok = node.At(UPDATE_LIST_OFFSET).execAnalyzeUpdateExpressions(tableInfo.Indexes[0]); !ok {
		plan.Reason = REASON_PK_CHANGE
		return plan
	}

	plan.PlanId = PLAN_DML_SUBQUERY
	plan.OuterQuery = node.GenerateUpdateOuterQuery(tableInfo.Indexes[0])
	plan.Subquery = node.GenerateUpdateSubquery(tableInfo)

	conditions := node.At(UPDATE_WHERE_OFFSET).execAnalyzeWhere()
	if conditions == nil {
		plan.Reason = REASON_WHERE
		return plan
	}

	if pkValues := getPKValues(conditions, tableInfo.Indexes[0]); pkValues != nil {
		plan.PlanId = PLAN_DML_PK
		plan.OuterQuery = plan.FullQuery
		plan.PKValues = pkValues
		return plan
	}

	return plan
}

func (node *Node) execAnalyzeDelete(getTable TableGetter) (plan *ExecPlan) {
	// Default plan
	plan = &ExecPlan{PlanId: PLAN_PASS_DML, FullQuery: node.GenerateFullQuery()}

	tableName := string(node.At(DELETE_TABLE_OFFSET).Value)
	tableInfo := plan.setTableInfo(tableName, getTable)

	if len(tableInfo.Indexes) == 0 || tableInfo.Indexes[0].Name != "PRIMARY" {
		log.Warningf("no primary key for table %s", tableName)
		plan.Reason = REASON_TABLE_NOINDEX
		return plan
	}

	plan.PlanId = PLAN_DML_SUBQUERY
	plan.OuterQuery = node.GenerateDeleteOuterQuery(tableInfo.Indexes[0])
	plan.Subquery = node.GenerateDeleteSubquery(tableInfo)

	conditions := node.At(DELETE_WHERE_OFFSET).execAnalyzeWhere()
	if conditions == nil {
		plan.Reason = REASON_WHERE
		return plan
	}

	if pkValues := getPKValues(conditions, tableInfo.Indexes[0]); pkValues != nil {
		plan.PlanId = PLAN_DML_PK
		plan.OuterQuery = plan.FullQuery
		plan.PKValues = pkValues
		return plan
	}

	return plan
}

func (node *Node) execAnalyzeSet() (plan *ExecPlan) {
	plan = &ExecPlan{PlanId: PLAN_SET, FullQuery: node.GenerateFullQuery()}
	update_list := node.At(1)  // NODE_LIST
	if update_list.Len() > 1 { // Multiple set values
		return
	}
	update_expression := update_list.At(0)              // '='
	plan.SetKey = string(update_expression.At(0).Value) // ID
	expression := update_expression.At(1)
	if expression.Type == NUMBER {
		// TODO: Try integer conversions first
		if val, err := strconv.ParseFloat(string(expression.Value), 64); err == nil {
			plan.SetValue = val
		}
	}
	return plan
}

func (node *ExecPlan) setTableInfo(tableName string, getTable TableGetter) *schema.Table {
	tableInfo, ok := getTable(tableName)
	if !ok {
		panic(NewParserError("Table %s not found in schema", tableName))
	}
	node.TableName = tableInfo.Name
	return tableInfo
}

//-----------------------------------------------
// Select

func (node *Node) execAnalyzeSelectStructure() bool {
	switch node.Type {
	case UNION, UNION_ALL, MINUS, EXCEPT, INTERSECT:
		return false
	}
	if node.At(SELECT_DISTINCT_OFFSET).Type == DISTINCT {
		return false
	}
	if node.At(SELECT_GROUP_OFFSET).Len() > 0 {
		return false
	}
	if node.At(SELECT_HAVING_OFFSET).Len() > 0 {
		return false
	}
	return true
}

//-----------------------------------------------
// Select Expressions

func (node *Node) execAnalyzeSelectExpressions(table *schema.Table) (selects []int) {
	selects = make([]int, 0, node.Len())
	for i := 0; i < node.Len(); i++ {
		if name := node.At(i).execAnalyzeSelectExpression(); name != "" {
			if name == "*" {
				for colIndex := range table.Columns {
					selects = append(selects, colIndex)
				}
			} else if colIndex := table.FindColumn(name); colIndex != -1 {
				selects = append(selects, colIndex)
			} else {
				panic(NewParserError("Column %s not found in table %s", name, table.Name))
			}
		} else {
			// Complex expression
			return nil
		}
	}
	return selects
}

func (node *Node) execAnalyzeSelectExpression() (name string) {
	switch node.Type {
	case ID, SELECT_STAR:
		return string(node.Value)
	case '.':
		return node.At(1).execAnalyzeSelectExpression()
	case AS:
		return node.At(0).execAnalyzeSelectExpression()
	}
	return ""
}

//-----------------------------------------------
// From

func (node *Node) execAnalyzeFrom() (tablename string, hasHints bool) {
	if node.Len() > 1 {
		return "", false
	}
	if node.At(0).Type != TABLE_EXPR {
		return "", false
	}
	hasHints = (node.At(0).At(2).Len() > 0)
	return node.At(0).At(0).collectTableName(), hasHints
}

func (node *Node) collectTableName() string {
	switch node.Type {
	case AS:
		return node.At(0).collectTableName()
	case ID:
		return string(node.Value)
	case '.':
		return string(node.At(1).Value)
	}
	// sub-select
	return ""
}

//-----------------------------------------------
// Where

func (node *Node) execAnalyzeWhere() (conditions []*Node) {
	if node.Len() == 0 {
		return nil
	}
	return node.At(0).execAnalyzeBoolean()
}

func (node *Node) execAnalyzeBoolean() (conditions []*Node) {
	switch node.Type {
	case AND:
		left := node.At(0).execAnalyzeBoolean()
		right := node.At(1).execAnalyzeBoolean()
		if left == nil || right == nil {
			return nil
		}
		if hasINClause(left) && hasINClause(right) {
			return nil
		}
		return append(left, right...)
	case '(':
		return node.At(0).execAnalyzeBoolean()
	case '=', '<', '>', LE, GE, NULL_SAFE_EQUAL, LIKE:
		left := node.At(0).execAnalyzeID()
		right := node.At(1).execAnalyzeValue()
		if left == nil || right == nil {
			return nil
		}
		n := NewParseNode(node.Type, node.Value)
		n.PushTwo(left, right)
		return []*Node{n}
	case IN:
		left := node.At(0).execAnalyzeID()
		right := node.At(1).execAnalyzeSimpleINList()
		if left == nil || right == nil {
			return nil
		}
		n := NewParseNode(node.Type, node.Value)
		n.PushTwo(left, right)
		return []*Node{n}
	case BETWEEN:
		left := node.At(0).execAnalyzeID()
		right1 := node.At(1).execAnalyzeValue()
		right2 := node.At(2).execAnalyzeValue()
		if left == nil || right1 == nil || right2 == nil {
			return nil
		}
		return []*Node{node}
	}
	return nil
}

func (node *Node) execAnalyzeSimpleINList() *Node {
	list := node.At(0) // '('->NODE_LIST
	for i := 0; i < list.Len(); i++ {
		if n := list.At(i).execAnalyzeValue(); n == nil {
			return nil
		}
	}
	return node
}

func (node *Node) execAnalyzeID() *Node {
	switch node.Type {
	case ID:
		return node
	case '.':
		return node.At(1).execAnalyzeID()
	}
	return nil
}

func (node *Node) execAnalyzeValue() *Node {
	switch node.Type {
	case STRING, NUMBER, VALUE_ARG:
		return node
	}
	return nil
}

func hasINClause(conditions []*Node) bool {
	for _, node := range conditions {
		if node.Type == IN {
			return true
		}
	}
	return false
}

func (node *Node) parseList() (values interface{}, isList bool) {
	vals := make([]interface{}, node.Len())
	for i := 0; i < node.Len(); i++ {
		vals[i] = asInterface(node.At(i))
	}
	return vals, true
}

//-----------------------------------------------
// Update expressions

func (node *Node) execAnalyzeUpdateExpressions(pkIndex *schema.Index) (pkValues []interface{}, ok bool) {
	for i := 0; i < node.Len(); i++ {
		columnName := string(node.At(i).At(0).Value)
		index := pkIndex.FindColumn(columnName)
		if index == -1 {
			continue
		}
		value := node.At(i).At(1).execAnalyzeValue()
		if value == nil {
			log.Warningf("expression is too complex %v", node.At(i).At(0))
			return nil, false
		}
		if pkValues == nil {
			pkValues = make([]interface{}, len(pkIndex.Columns))
		}
		pkValues[index] = asInterface(value)
	}
	return pkValues, true
}

//-----------------------------------------------
// Insert

<<<<<<< HEAD
func (node *Node) getInsertPKColumns(tableInfo *schema.Table) (pkColumnNumbers []int) {
	if node.Len() == 0 {
		return tableInfo.PKColumns
	}
	pkIndex := tableInfo.Indexes[0]
	fmt.Println("pkIndex: ", pkIndex)
	pkColumnNumbers = make([]int, len(pkIndex.Columns))
	for i := range pkColumnNumbers {
		pkColumnNumbers[i] = -1
	}
	for i, column := range node.Sub {
		fmt.Println("...", column)
		index := pkIndex.FindColumn(string(column.Value))
		if index == -1 {
			continue
		}
		pkColumnNumbers[index] = i
	}
	return pkColumnNumbers
}

func (node *Node) getInsertColumnValue(tableInfo *schema.Table, rowList *Node) (Columns []map[string]sqltypes.Value) {
	Columns = make([]map[string]sqltypes.Value, len(node.Sub))

func (node *Node) getInsertColumnValue(tableInfo *schema.Table, rowList *Node) (rowColumns []map[string]interface{}) {
	rowColumns = make([]map[string]interface{}, len(node.Sub))

	// pkIndex := tableInfo.Indexes[0]
	for i, column := range node.Sub {
		// index := pkIndex.FindColumn(string(column.Value))
		// if index != -1 {
		// 	continue
		// }
		values := make([]sqltypes.Value, rowList.Len())
		for j := 0; j < rowList.Len(); j++ {
			node := rowList.At(j).At(0).At(i) // NODE_LIST->'('->NODE_LIST->Value
			value := node.execAnalyzeValue()
			if value == nil {
				log.Warn("insert is too complex %v", node)
				return nil
			}
			// values[j] = asInterface(value)
			values[j] = asValue(value)
		}
		if len(values) == 1 {

			// m := map[string]sqltypes.Value
			// m[column.Value] = values[0]
			// Columns[i] = m
		// 	Columns[i] = map[string]sqltypes.Value{string(column.Value): values[0]}
		// } else {
		// 	fmt.Println("ValuesLen: ", len(values))
			// Columns[i] = map[string]sqltypes.Value{string(column.Value): values}

			rowColumns[i] = map[string]interface{}{"name": string(column.Value), "value": values[0]}
		} else {
			fmt.Println("ValuesLen: ", len(values))
			rowColumns[i] = map[string]interface{}{"name": string(column.Value), "value": values}

		}
	}
	return rowColumns
}

// func (node *Node) getInsertColumns(tableInfo *schema.Table) (ColumnNames []string) {

// 	ColumnNames = make([]string, len(tableInfo.Columns))
// 	for i := range ColumnNames {
// 		ColumnNames[i] = "null"
// 	}
// 	for i, column := range node.Sub {
// 		fmt.Println("...............", column.Value)
// 		ColumnNames[i] = string(column.Value)
// 	}
// 	return ColumnNames
// }

// func getInsertValues(ColumnNames []string, rowList *Node, tableInfo *schema.Table) (ColumnValues []interface{}) {

// 	ColumnValues = make([]interface{}, len( ColumnNames))
// 	for index, columnName := range ColumnNames {
// 		if columnName == "null" {
// 			ColumnValues[index] = "null"
// 			continue
// 		}
// 		values := make([]interface{}, rowList.Len())
// 		for j := 0; j < rowList.Len(); j++ {
// 			node := rowList.At(j).At(0).At(index) // NODE_LIST->'('->NODE_LIST->Value
// 			value := node.execAnalyzeValue()
// 			if value == nil {
// 				log.Warningf("insert is too complex %v", node)
// 				return nil
// 			}
// 			values[j] = asInterface(value)
// 		}
// 		if len(values) == 1 {
// 			ColumnValues[index] = values[0]
// 		} else {
// 			ColumnValues[index] = values
// 		}
// 	}

// 	return ColumnValues
// }

func getInsertPKValues(pkColumnNumbers []int, rowList *Node, tableInfo *schema.Table) (pkValues []interface{}) {
	pkValues = make([]interface{}, len(pkColumnNumbers))
	for index, columnNumber := range pkColumnNumbers {
		if columnNumber == -1 {
			pkValues[index] = tableInfo.GetPKColumn(index).Default
			continue
		}
		values := make([]interface{}, rowList.Len())
		for j := 0; j < rowList.Len(); j++ {
			if columnNumber >= rowList.At(j).At(0).Len() { // NODE_LIST->'('->NODE_LIST
				panic(NewParserError("Column count doesn't match value count"))
			}
			node := rowList.At(j).At(0).At(columnNumber) // NODE_LIST->'('->NODE_LIST->Value
			value := node.execAnalyzeValue()
			if value == nil {
				log.Warningf("insert is too complex %v", node)
				return nil
			}
			values[j] = asInterface(value)
		}
		if len(values) == 1 {
			pkValues[index] = values[0]
		} else {
			pkValues[index] = values
		}
	}
	return pkValues
}

//-----------------------------------------------
// Index Analysis

type IndexScore struct {
	Index       *schema.Index
	ColumnMatch []bool
	MatchFailed bool
}

type scoreValue int64

const (
	NO_MATCH      = scoreValue(-1)
	PERFECT_SCORE = scoreValue(0)
)

func NewIndexScore(index *schema.Index) *IndexScore {
	return &IndexScore{index, make([]bool, len(index.Columns)), false}
}

func (is *IndexScore) FindMatch(columnName string) int {
	if is.MatchFailed {
		return -1
	}
	if index := is.Index.FindColumn(columnName); index != -1 {
		is.ColumnMatch[index] = true
		fmt.Println("is.ColumnMatch: true")
		return index
	}
	// If the column is among the data columns, we can still use
	// the index without going to the main table
	if index := is.Index.FindDataColumn(columnName); index == -1 {
		fmt.Println("is.MatchFailed true")
		is.MatchFailed = true
	}
	return -1
}

func (is *IndexScore) GetScore() scoreValue {
	if is.MatchFailed {
		return NO_MATCH
	}
	score := NO_MATCH
	for i, indexColumn := range is.ColumnMatch {
		if indexColumn {
			score = scoreValue(is.Index.Cardinality[i])
			continue
		}
		return score
	}
	return PERFECT_SCORE
}

func NewIndexScoreList(indexes []*schema.Index) []*IndexScore {
	scoreList := make([]*IndexScore, len(indexes))
	for i, v := range indexes {
		scoreList[i] = NewIndexScore(v)
	}
	return scoreList
}

func getSelectPKValues(conditions []*Node, pkIndex *schema.Index) (planId PlanType, pkValues []interface{}) {
	pkValues = getPKValues(conditions, pkIndex)
	if pkValues == nil {
		return PLAN_PASS_SELECT, nil
	}
	for _, pkValue := range pkValues {
		inList, ok := pkValue.([]interface{})
		if !ok {
			continue
		}
		if len(pkValues) == 1 {
			return PLAN_PK_IN, inList
		}
		return PLAN_PASS_SELECT, nil
	}
	return PLAN_PK_EQUAL, pkValues
}

func getPKValues(conditions []*Node, pkIndex *schema.Index) (pkValues []interface{}) {
	pkIndexScore := NewIndexScore(pkIndex)
	pkValues = make([]interface{}, len(pkIndexScore.ColumnMatch))
	for _, condition := range conditions {
		if condition.Type != '=' && condition.Type != IN {
			return nil
		}
		index := pkIndexScore.FindMatch(string(condition.At(0).Value))
		if index == -1 {
			return nil
		}
		switch condition.Type {
		case '=':
			pkValues[index] = asInterface(condition.At(1))
		case IN:
			pkValues[index], _ = condition.At(1).At(0).parseList()
		}
	}
	if pkIndexScore.GetScore() == PERFECT_SCORE {
		return pkValues
	}
	return nil
}

func getIndexMatch(conditions []*Node, indexes []*schema.Index) string {
	indexScores := NewIndexScoreList(indexes)
	for _, condition := range conditions {
		for _, index := range indexScores {
			fmt.Println("...index ", indexScores)
			fmt.Println("...condition: ", condition.At(0).String())
			index.FindMatch(string(condition.At(0).Value))
		}
	}
	highScore := NO_MATCH
	highScorer := -1
	for i, index := range indexScores {
		curScore := index.GetScore()
		fmt.Println("...curScore ", curScore)
		if curScore == NO_MATCH {
			fmt.Println("...curScore ", curScore)
			continue
		}
		if curScore == PERFECT_SCORE {
			fmt.Println("...curScorse ", curScore)
			highScorer = i
			break
		}
		// Prefer secondary index over primary key
		if curScore >= highScore {
			highScore = curScore
			highScorer = i
		}
	}
	if highScorer == -1 {
		return ""
	}
	return indexes[highScorer].Name
}

//-----------------------------------------------
// Query Generation
func (node *Node) GenerateFullQuery() *ParsedQuery {
	buf := NewTrackedBuffer(nil)
	FormatNode(buf, node)
	return buf.ParsedQuery()
}

func (node *Node) GenerateFieldQuery() *ParsedQuery {
	buf := NewTrackedBuffer(FormatImpossible)
	FormatImpossible(buf, node)
	if len(buf.bindLocations) != 0 {
		return nil
	}
	return buf.ParsedQuery()
}

// FormatImpossible is a callback function used by TrackedBuffer
// to generate a modified version of the query where all selects
// have impossible where clauses. It overrides a few node types
// and passes the rest down to the default FormatNode.
func FormatImpossible(buf *TrackedBuffer, node *Node) {
	switch node.Type {
	case SELECT:
		buf.Fprintf("select %v from %v where 1 != 1",
			node.At(SELECT_EXPR_OFFSET),
			node.At(SELECT_FROM_OFFSET),
		)
	case JOIN, STRAIGHT_JOIN, CROSS, NATURAL:
		// We skip ON clauses (if any)
		buf.Fprintf("%v %s %v", node.At(0), node.Value, node.At(1))
	case LEFT, RIGHT:
		// ON clause is requried
		buf.Fprintf("%v %s %v on 1 != 1", node.At(0), node.Value, node.At(1))
	default:
		FormatNode(buf, node)
	}
}

func (node *Node) GenerateSelectLimitQuery() *ParsedQuery {
	buf := NewTrackedBuffer(nil)
	if node.Type == SELECT {
		limit := node.At(SELECT_LIMIT_OFFSET)
		if limit.Len() == 0 {
			limit.PushLimit()
			defer limit.Pop()
		}
	}
	FormatNode(buf, node)
	return buf.ParsedQuery()
}

func (node *Node) GenerateDefaultQuery(tableInfo *schema.Table) *ParsedQuery {
	buf := NewTrackedBuffer(nil)
	limit := node.At(SELECT_LIMIT_OFFSET)
	if limit.Len() == 0 {
		limit.PushLimit()
		defer limit.Pop()
	}
	fmt.Fprintf(buf, "select ")
	writeColumnList(buf, tableInfo.Columns)
	buf.Fprintf(" from %v%v%v%v",
		node.At(SELECT_FROM_OFFSET),
		node.At(SELECT_WHERE_OFFSET),
		node.At(SELECT_ORDER_OFFSET),
		limit)
	return buf.ParsedQuery()
}

func (node *Node) GenerateEqualOuterQuery(tableInfo *schema.Table) *ParsedQuery {
	buf := NewTrackedBuffer(nil)
	fmt.Fprintf(buf, "select ")
	writeColumnList(buf, tableInfo.Columns)
	buf.Fprintf(" from %v where ", node.At(SELECT_FROM_OFFSET))
	generatePKWhere(buf, tableInfo.Indexes[0])
	return buf.ParsedQuery()
}

func (node *Node) GenerateInOuterQuery(tableInfo *schema.Table) *ParsedQuery {
	buf := NewTrackedBuffer(nil)
	fmt.Fprintf(buf, "select ")
	writeColumnList(buf, tableInfo.Columns)
	// We assume there is one and only one PK column.
	// A '*' argument name means all variables of the list.
	buf.Fprintf(" from %v where %s in (%a)", node.At(SELECT_FROM_OFFSET), tableInfo.Indexes[0].Columns[0], "*")
	return buf.ParsedQuery()
}

func (node *Node) GenerateInsertOuterQuery() *ParsedQuery {
	buf := NewTrackedBuffer(nil)
	buf.Fprintf("insert %vinto %v%v values %a%v",
		node.At(INSERT_COMMENT_OFFSET),
		node.At(INSERT_TABLE_OFFSET),
		node.At(INSERT_COLUMN_LIST_OFFSET),
		"_rowValues",
		node.At(INSERT_ON_DUP_OFFSET),
	)
	return buf.ParsedQuery()
}

func (node *Node) GenerateUpdateOuterQuery(pkIndex *schema.Index) *ParsedQuery {
	buf := NewTrackedBuffer(nil)
	buf.Fprintf("update %v%v set %v where ",
		node.At(UPDATE_COMMENT_OFFSET), node.At(UPDATE_TABLE_OFFSET), node.At(UPDATE_LIST_OFFSET))
	generatePKWhere(buf, pkIndex)
	return buf.ParsedQuery()
}

func (node *Node) GenerateDeleteOuterQuery(pkIndex *schema.Index) *ParsedQuery {
	buf := NewTrackedBuffer(nil)
	buf.Fprintf("delete %vfrom %v where ", node.At(DELETE_COMMENT_OFFSET), node.At(DELETE_TABLE_OFFSET))
	generatePKWhere(buf, pkIndex)
	return buf.ParsedQuery()
}

func generatePKWhere(buf *TrackedBuffer, pkIndex *schema.Index) {
	for i := 0; i < len(pkIndex.Columns); i++ {
		if i != 0 {
			buf.WriteString(" and ")
		}
		buf.Fprintf("%s = %a", pkIndex.Columns[i], strconv.FormatInt(int64(i), 10))
	}
}

func (node *Node) GenerateSelectSubquery(tableInfo *schema.Table, index string) *ParsedQuery {
	hint := NewSimpleParseNode(USE, "use")
	hint.Push(NewSimpleParseNode(COLUMN_LIST, ""))
	hint.At(0).Push(NewSimpleParseNode(ID, index))
	table_expr := node.At(SELECT_FROM_OFFSET).At(0)
	savedHint := table_expr.Sub[2]
	table_expr.Sub[2] = hint
	defer func() {
		table_expr.Sub[2] = savedHint
	}()
	return GenerateSubquery(
		tableInfo.Indexes[0].Columns,
		node.At(SELECT_FROM_OFFSET),
		node.At(SELECT_WHERE_OFFSET),
		node.At(SELECT_ORDER_OFFSET),
		node.At(SELECT_LIMIT_OFFSET),
		false,
	)
}

func (node *Node) GenerateUpdateSubquery(tableInfo *schema.Table) *ParsedQuery {
	return GenerateSubquery(
		tableInfo.Indexes[0].Columns,
		node.At(UPDATE_TABLE_OFFSET),
		node.At(UPDATE_WHERE_OFFSET),
		node.At(UPDATE_ORDER_OFFSET),
		node.At(UPDATE_LIMIT_OFFSET),
		true,
	)
}

func (node *Node) GenerateDeleteSubquery(tableInfo *schema.Table) *ParsedQuery {
	return GenerateSubquery(
		tableInfo.Indexes[0].Columns,
		node.At(DELETE_TABLE_OFFSET),
		node.At(DELETE_WHERE_OFFSET),
		node.At(DELETE_ORDER_OFFSET),
		node.At(DELETE_LIMIT_OFFSET),
		true,
	)
}

func (node *Node) PushLimit() {
	node.Push(NewSimpleParseNode(VALUE_ARG, ":_vtMaxResultSize"))
}

func GenerateSubquery(columns []string, table *Node, where *Node, order *Node, limit *Node, for_update bool) *ParsedQuery {
	buf := NewTrackedBuffer(nil)
	if limit.Len() == 0 {
		limit.PushLimit()
		defer limit.Pop()
	}
	fmt.Fprintf(buf, "select ")
	i := 0
	for i = 0; i < len(columns)-1; i++ {
		fmt.Fprintf(buf, "%s, ", columns[i])
	}
	fmt.Fprintf(buf, "%s", columns[i])
	buf.Fprintf(" from %v%v%v%v", table, where, order, limit)
	if for_update {
		buf.Fprintf(" for update")
	}
	return buf.ParsedQuery()
}

func writeColumnList(buf *TrackedBuffer, columns []schema.TableColumn) {
	i := 0
	for i = 0; i < len(columns)-1; i++ {
		fmt.Fprintf(buf, "%s, ", columns[i].Name)
	}
	fmt.Fprintf(buf, "%s", columns[i].Name)
}

func asInterface(node *Node) interface{} {
	switch node.Type {
	case VALUE_ARG:
		return string(node.Value)
	case STRING:
		return sqltypes.MakeString(node.Value)
	case NUMBER:
		n, err := sqltypes.BuildNumeric(string(node.Value))
		if err != nil {
			panic(NewParserError("Type mismatch: %s", err))
		}
		return n
	}
	panic(NewParserError("Unexpected node %v", node))
}

func asValue(node *Node) sqltypes.Value {
	switch node.Type {
	case STRING:
		return sqltypes.MakeString(node.Value)
	case NUMBER:
		n, err := sqltypes.BuildNumeric(string(node.Value))
		if err != nil {
			panic(NewParserError("Type mismatch: %s", err))
		}
		return n
	}
	panic(NewParserError("Unexpected node %v", node))
}
