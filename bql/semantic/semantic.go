// Copyright 2015 Google Inc. All rights reserved.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

// Package semantic contains the semantic analysis required to have a
// senantically valid parser. It includes the data conversion required to
// turn tokens into valid BadWolf structures. It also provides the hooks
// implementations required for buliding an actionable execution plan.
package semantic

import (
	"fmt"
	"reflect"
	"sort"
	"time"

	"github.com/google/badwolf/bql/lexer"
	"github.com/google/badwolf/bql/table"
	"github.com/google/badwolf/triple"
	"github.com/google/badwolf/triple/node"
	"github.com/google/badwolf/triple/predicate"
)

// StatementType describes the type of statement being represented.
type StatementType int8

const (
	// Query statement.
	Query StatementType = iota
	// Insert statemrnt.
	Insert
	// Delete statement.
	Delete
	// Create statement.
	Create
	// Drop statement.
	Drop
)

// String provides a readable version of the StatementType.
func (t StatementType) String() string {
	switch t {
	case Query:
		return "QUERY"
	case Insert:
		return "INSERT"
	case Delete:
		return "DELETE"
	case Create:
		return "CREATE"
	case Drop:
		return "DROP"
	default:
		return "UNKNOWN"
	}
}

// Statement contains all the semantic information extract from the parsing
type Statement struct {
	sType                     StatementType
	graphs                    []string
	data                      []*triple.Triple
	pattern                   []*GraphClause
	workingClause             *GraphClause
	projection                []*Projection
	workingProjection         *Projection
	groupBy                   []string
	orderBy                   table.SortConfig
	havingExpression          []ConsumedElement
	havingExpressionEvaluator Evaluator
	limitSet                  bool
	limit                     int64
}

// GraphClause represents a clause of a graph pattern in a where clause.
type GraphClause struct {
	S          *node.Node
	SBinding   string
	SAlias     string
	STypeAlias string
	SIDAlias   string

	P                *predicate.Predicate
	PID              string
	PBinding         string
	PAlias           string
	PIDAlias         string
	PAnchorBinding   string
	PAnchorAlias     string
	PLowerBound      *time.Time
	PUpperBound      *time.Time
	PLowerBoundAlias string
	PUpperBoundAlias string
	PTemporal        bool

	O                *triple.Object
	OBinding         string
	OAlias           string
	OID              string
	OTypeAlias       string
	OIDAlias         string
	OAnchorBinding   string
	OAnchorAlias     string
	OLowerBound      *time.Time
	OUpperBound      *time.Time
	OLowerBoundAlias string
	OUpperBoundAlias string
	OTemporal        bool
}

// Specificity return
func (c *GraphClause) Specificity() int {
	s := 0
	if c.S != nil {
		s++
	}
	if c.P != nil {
		s++
	}
	if c.O != nil {
		s++
	}
	return s
}

// BindingsMap returns the binding map fo he graph clause.
func (c *GraphClause) BindingsMap() map[string]int {
	bm := make(map[string]int)

	addToBindings(bm, c.SBinding)
	addToBindings(bm, c.SAlias)
	addToBindings(bm, c.STypeAlias)
	addToBindings(bm, c.SIDAlias)
	addToBindings(bm, c.PAlias)
	addToBindings(bm, c.PAnchorBinding)
	addToBindings(bm, c.PBinding)
	addToBindings(bm, c.PLowerBoundAlias)
	addToBindings(bm, c.PUpperBoundAlias)
	addToBindings(bm, c.PIDAlias)
	addToBindings(bm, c.PAnchorAlias)
	addToBindings(bm, c.OBinding)
	addToBindings(bm, c.OAlias)
	addToBindings(bm, c.OTypeAlias)
	addToBindings(bm, c.OIDAlias)
	addToBindings(bm, c.OAnchorAlias)
	addToBindings(bm, c.OAnchorBinding)
	addToBindings(bm, c.OLowerBoundAlias)
	addToBindings(bm, c.OUpperBoundAlias)

	return bm
}

// Bindings returns the list of unique bindings listed int he graph clause.
func (c *GraphClause) Bindings() []string {
	var bs []string
	for k := range c.BindingsMap() {
		bs = append(bs, k)
	}
	return bs
}

// IsEmpty will return true if the are no set values in the clause.
func (c *GraphClause) IsEmpty() bool {
	return reflect.DeepEqual(c, &GraphClause{})
}

// BindType set he type of a statement.
func (s *Statement) BindType(st StatementType) {
	s.sType = st
}

// Type returns the type of the statement.
func (s *Statement) Type() StatementType {
	return s.sType
}

// AddGraph adds a graph to a given https://critique.corp.google.com/#review/101398527statement.
func (s *Statement) AddGraph(g string) {
	s.graphs = append(s.graphs, g)
}

// Graphs returns the list of graphs listed on the statement.
func (s *Statement) Graphs() []string {
	return s.graphs
}

// AddData adds a triple to a given statement's data.
func (s *Statement) AddData(d *triple.Triple) {
	s.data = append(s.data, d)
}

// Data returns the data available for the given statement.
func (s *Statement) Data() []*triple.Triple {
	return s.data
}

// GraphPatternClauses return the list of graph pattern clauses
func (s *Statement) GraphPatternClauses() []*GraphClause {
	return s.pattern
}

// ResetWorkingGraphClause resets the current working graph clause.
func (s *Statement) ResetWorkingGraphClause() {
	s.workingClause = &GraphClause{}
}

// WorkingClause returns the current working clause.
func (s *Statement) WorkingClause() *GraphClause {
	return s.workingClause
}

// AddWorkingGrpahClause add the current working graph clause to the set of
// clauses that form the graph pattern.
func (s *Statement) AddWorkingGrpahClause() {
	if s.workingClause != nil || !s.workingClause.IsEmpty() {
		s.pattern = append(s.pattern, s.workingClause)
	}
	s.ResetWorkingGraphClause()
}

// addtoBindings add the binding if not empty.
func addToBindings(bs map[string]int, b string) {
	if b != "" {
		bs[b]++
	}
}

// BindingsMap retuns the set of bindings available on the graph clauses for he
// statement.
func (s *Statement) BindingsMap() map[string]int {
	bm := make(map[string]int)

	for _, cls := range s.pattern {
		if cls != nil {
			addToBindings(bm, cls.SBinding)
			addToBindings(bm, cls.SAlias)
			addToBindings(bm, cls.STypeAlias)
			addToBindings(bm, cls.SIDAlias)
			addToBindings(bm, cls.PAlias)
			addToBindings(bm, cls.PAnchorBinding)
			addToBindings(bm, cls.PBinding)
			addToBindings(bm, cls.PLowerBoundAlias)
			addToBindings(bm, cls.PUpperBoundAlias)
			addToBindings(bm, cls.PIDAlias)
			addToBindings(bm, cls.PAnchorAlias)
			addToBindings(bm, cls.OBinding)
			addToBindings(bm, cls.OAlias)
			addToBindings(bm, cls.OTypeAlias)
			addToBindings(bm, cls.OIDAlias)
			addToBindings(bm, cls.OAnchorAlias)
			addToBindings(bm, cls.OAnchorBinding)
			addToBindings(bm, cls.OLowerBoundAlias)
			addToBindings(bm, cls.OUpperBoundAlias)
		}
	}
	return bm
}

// Bindings retuns the list of bindings available on the graph clauses for he
// statement.
func (s *Statement) Bindings() []string {
	var bs []string
	for k := range s.BindingsMap() {
		bs = append(bs, k)
	}
	return bs
}

// bySpecificity type helps sort clauses by Specificity.
type bySpecificity []*GraphClause

// Len returns the lenght of the clauses array.
func (s bySpecificity) Len() int {
	return len(s)
}

// Swap exchange the i and j elements in the clauses array.
func (s bySpecificity) Swap(i, j int) {
	s[i], s[j] = s[j], s[i]
}

// Less returns true if the i element is less specific than j one.
func (s bySpecificity) Less(i, j int) bool {
	return s[i].Specificity() > s[j].Specificity()
}

// SortedGraphPatternClauses return the list of graph pattern clauses
func (s *Statement) SortedGraphPatternClauses() []*GraphClause {
	var ptrns []*GraphClause
	// Filter empty clauses.
	for _, cls := range s.pattern {
		if cls != nil && !cls.IsEmpty() {
			ptrns = append(ptrns, cls)
		}
	}
	sort.Sort(bySpecificity(ptrns))
	return ptrns
}

// Projection contails the information required to project the outcome of
// querying with GraphClauses. It also contains the information of what
// aggregation function should be used.
type Projection struct {
	Binding  string
	Alias    string
	OP       lexer.TokenType // The information about what function to use.
	Modifier lexer.TokenType // The modifier for the selected op.
}

// String returns a readable form of the projection.
func (p *Projection) String() string {
	return fmt.Sprintf("%s as %s (%s, %s)", p.Binding, p.Alias, p.OP, p.Modifier)
}

// IsEmpty check if the given projection is empty.
func (p *Projection) IsEmpty() bool {
	return p.Binding == "" && p.Alias == "" && p.OP == lexer.ItemError && p.Modifier == lexer.ItemError
}

// ResetProjection resets the current working variable projection.
func (s *Statement) ResetProjection() {
	s.workingProjection = &Projection{}
}

// WorkingProjection returns the current working variable projection.
func (s *Statement) WorkingProjection() *Projection {
	if s.workingProjection == nil {
		s.ResetProjection()
	}
	return s.workingProjection
}

// AddWorkingProjection add the current projection variableto the set of
// projects that this statement.
func (s *Statement) AddWorkingProjection() {
	if s.workingProjection != nil && !s.workingProjection.IsEmpty() {
		s.projection = append(s.projection, s.workingProjection)
	}
	s.ResetProjection()
}

// Projections returns all the available projections.
func (s *Statement) Projections() []*Projection {
	return s.projection
}

// InputBindings returns the list of incomming binding feed from a where clause.
func (s *Statement) InputBindings() []string {
	var res []string
	for _, p := range s.projection {
		if p.Binding != "" {
			res = append(res, p.Binding)
		}
	}
	return res
}

// OutputBindings returns the list of binding that a query will return.
func (s *Statement) OutputBindings() []string {
	var res []string
	for _, p := range s.projection {
		if p.Alias != "" {
			res = append(res, p.Alias)
			continue
		}
		if p.Binding != "" {
			res = append(res, p.Binding)
		}
	}
	return res
}

// GroupByBindings returns the bindings used on the group by statement.
func (s *Statement) GroupByBindings() []string {
	return s.groupBy
}

// OrderByConfig returns the sort configuration specified by the order by
// statement.
func (s *Statement) OrderByConfig() table.SortConfig {
	return s.orderBy
}

// HasHavingClause returns true if there is a having clause.
func (s *Statement) HasHavingClause() bool {
	return len(s.havingExpression) > 0
}

// HavingEvaluator returns the evaluator constructed for the provided having
// clause.
func (s *Statement) HavingEvaluator() Evaluator {
	return s.havingExpressionEvaluator
}

// IsLimitSet returns true if the limit is set.
func (s *Statement) IsLimitSet() bool {
	return s.limitSet
}

// Limit returns the limit value set in the limit clause.
func (s *Statement) Limit() int64 {
	return s.limit
}
