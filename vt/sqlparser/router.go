// Copyright 2012, Google Inc. All rights reserved.
// Use of this source code is governed by a BSD-style
// license that can be found in the LICENSE file.

package sqlparser

import (
	"strconv"

	"github.com/youtube/vitess/go/vt/key"
)

const (
	ROUTE_BY_CONDITION = iota
	ROUTE_BY_VALUE
)

const (
	EID_NODE = iota
	VALUE_NODE
	LIST_NODE
	OTHER_NODE
)

type RoutingPlan struct {
	routingType int
	criteria    *Node
}

func GetShardList(sql string, bindVariables map[string]interface{}, tabletKeys []key.KeyspaceId) (shardlist []int, err error) {
	defer handleError(&err)

	plan := buildPlan(sql)
	return shardListFromPlan(plan, bindVariables, tabletKeys), nil
}

func buildPlan(sql string) (plan *RoutingPlan) {
	tree, err := Parse(sql)
	if err != nil {
		panic(err)
	}
	return tree.getRoutingPlan()
}

func shardListFromPlan(plan *RoutingPlan, bindVariables map[string]interface{}, tabletKeys []key.KeyspaceId) (shardList []int) {
	if plan.routingType == ROUTE_BY_VALUE {
		index := plan.criteria.findInsertShard(bindVariables, tabletKeys)
		return []int{index}
	}

	if plan.criteria == nil {
		return makeList(0, len(tabletKeys))
	}

	switch plan.criteria.Type {
	case '=', NULL_SAFE_EQUAL:
		index := plan.criteria.At(1).findShard(bindVariables, tabletKeys)
		return []int{index}
	case '<', LE:
		index := plan.criteria.At(1).findShard(bindVariables, tabletKeys)
		return makeList(0, index+1)
	case '>', GE:
		index := plan.criteria.At(1).findShard(bindVariables, tabletKeys)
		return makeList(index, len(tabletKeys))
	case IN:
		return plan.criteria.At(1).findShardList(bindVariables, tabletKeys)
	case BETWEEN:
		start := plan.criteria.At(1).findShard(bindVariables, tabletKeys)
		last := plan.criteria.At(2).findShard(bindVariables, tabletKeys)
		if last < start {
			start, last = last, start
		}
		return makeList(start, last+1)
	}
	return makeList(0, len(tabletKeys))
}

func (node *Node) getRoutingPlan() (plan *RoutingPlan) {
	plan = &RoutingPlan{}
	if node.Type == INSERT {
		if node.At(INSERT_VALUES_OFFSET).Type == VALUES {
			plan.routingType = ROUTE_BY_VALUE
			plan.criteria = node.At(INSERT_VALUES_OFFSET).At(0).routingAnalyzeValues()
			return plan
		} else { // SELECT, let us recurse
			return node.At(INSERT_VALUES_OFFSET).getRoutingPlan()
		}
	}
	var where *Node
	plan.routingType = ROUTE_BY_CONDITION
	switch node.Type {
	case SELECT:
		where = node.At(SELECT_WHERE_OFFSET)
	case UPDATE:
		where = node.At(UPDATE_WHERE_OFFSET)
	case DELETE:
		where = node.At(DELETE_WHERE_OFFSET)
	}
	if where != nil && where.Len() > 0 {
		plan.criteria = where.At(0).routingAnalyzeBoolean()
	}
	return plan
}

func (node *Node) routingAnalyzeValues() *Node {
	// Analyze first value of every item in the list
	for i := 0; i < node.Len(); i++ {
		value_expression_list := node.At(i).At(0)
		result := value_expression_list.At(0).routingAnalyzeValue()
		if result != VALUE_NODE {
			panic(NewParserError("insert is too complex"))
		}
	}
	return node
}

func (node *Node) routingAnalyzeBoolean() *Node {
	switch node.Type {
	case AND:
		left := node.At(0).routingAnalyzeBoolean()
		right := node.At(1).routingAnalyzeBoolean()
		if left != nil && right != nil {
			return nil
		} else if left != nil {
			return left
		} else {
			return right
		}
	case '(':
		return node.At(0).routingAnalyzeBoolean()
	case '=', '<', '>', LE, GE, NULL_SAFE_EQUAL:
		left := node.At(0).routingAnalyzeValue()
		right := node.At(1).routingAnalyzeValue()
		if (left == EID_NODE && right == VALUE_NODE) || (left == VALUE_NODE && right == EID_NODE) {
			return node
		}
	case IN:
		left := node.At(0).routingAnalyzeValue()
		right := node.At(1).routingAnalyzeValue()
		if left == EID_NODE && right == LIST_NODE {
			return node
		}
	case BETWEEN:
		left := node.At(0).routingAnalyzeValue()
		right1 := node.At(1).routingAnalyzeValue()
		right2 := node.At(2).routingAnalyzeValue()
		if left == EID_NODE && right1 == VALUE_NODE && right2 == VALUE_NODE {
			return node
		}
	}
	return nil
}

func (node *Node) routingAnalyzeValue() int {
	switch node.Type {
	case ID:
		if string(node.Value) == "entity_id" {
			return EID_NODE
		}
	case '.':
		return node.At(1).routingAnalyzeValue()
	case '(':
		return node.At(0).routingAnalyzeValue()
	case NODE_LIST:
		for i := 0; i < node.Len(); i++ {
			if node.At(i).routingAnalyzeValue() != VALUE_NODE {
				return OTHER_NODE
			}
		}
		return LIST_NODE
	case STRING, NUMBER, VALUE_ARG:
		return VALUE_NODE
	}
	return OTHER_NODE
}

func (node *Node) findShardList(bindVariables map[string]interface{}, tabletKeys []key.KeyspaceId) []int {
	shardset := make(map[int]bool)
	switch node.Type {
	case '(':
		return node.At(0).findShardList(bindVariables, tabletKeys)
	case NODE_LIST:
		for i := 0; i < node.Len(); i++ {
			index := node.At(i).findShard(bindVariables, tabletKeys)
			shardset[index] = true
		}
	}
	shardlist := make([]int, len(shardset))
	index := 0
	for k := range shardset {
		shardlist[index] = k
		index++
	}
	return shardlist
}

func (node *Node) findInsertShard(bindVariables map[string]interface{}, tabletKeys []key.KeyspaceId) int {
	index := -1
	for i := 0; i < node.Len(); i++ {
		first_value_expression := node.At(i).At(0).At(0) // '('->value_expression_list->first_value
		newIndex := first_value_expression.findShard(bindVariables, tabletKeys)
		if index == -1 {
			index = newIndex
		} else if index != newIndex {
			panic(NewParserError("insert has multiple shard targets"))
		}
	}
	return index
}

func (node *Node) findShard(bindVariables map[string]interface{}, tabletKeys []key.KeyspaceId) int {
	value := node.getBoundValue(bindVariables)
	return key.FindShardForValue(value, tabletKeys)
}

func (node *Node) getBoundValue(bindVariables map[string]interface{}) string {
	switch node.Type {
	case '(':
		return node.At(0).getBoundValue(bindVariables)
	case STRING:
		return string(node.Value)
	case NUMBER:
		val, err := strconv.ParseInt(string(node.Value), 10, 64)
		if err != nil {
			panic(NewParserError("%s", err.Error()))
		}
		return key.Uint64Key(val).String()
	case VALUE_ARG:
		value := node.findBindValue(bindVariables)
		return key.EncodeValue(value)
	}
	panic("Unexpected token")
}

func (node *Node) findBindValue(bindVariables map[string]interface{}) interface{} {
	if bindVariables == nil {
		panic(NewParserError("No bind variable for " + string(node.Value)))
	}
	value, ok := bindVariables[string(node.Value[1:])]
	if !ok {
		panic(NewParserError("No bind variable for " + string(node.Value)))
	}
	return value
}

func makeList(start, end int) []int {
	list := make([]int, end-start)
	for i := start; i < end; i++ {
		list[i-start] = i
	}
	return list
}
