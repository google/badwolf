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
// semantically valid parser. It includes the data conversion required to
// turn tokens into valid BadWolf structures. It also provides the hooks
// implementations required for building an actionable execution plan.
package semantic

import (
	"bytes"
	"context"
	"reflect"
	"sort"
	"time"

	"github.com/google/badwolf/bql/lexer"
	"github.com/google/badwolf/bql/table"
	"github.com/google/badwolf/storage"
	"github.com/google/badwolf/triple"
	"github.com/google/badwolf/triple/node"
	"github.com/google/badwolf/triple/predicate"
)

// StatementType describes the type of statement being represented.
type StatementType int8

const (
	// Query statement.
	Query StatementType = iota
	// Insert statement.
	Insert
	// Delete statement.
	Delete
	// Create statement.
	Create
	// Drop statement.
	Drop
	// Construct statement.
	Construct
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
	case Construct:
		return "CONSTRUCT"
	default:
		return "UNKNOWN"
	}
}

// Statement contains all the semantic information extract from the parsing
type Statement struct {
	sType                     StatementType
	graphNames                []string
	graphs                    []storage.Graph
	inputGraphNames           []string
	inputGraphs               []storage.Graph
	outputGraphNames          []string
	outputGraphs              []storage.Graph
	data                      []*triple.Triple
	pattern                   []*GraphClause
	workingClause             *GraphClause
	constructClauses          []*ConstructClause
	workingConstructClause    *ConstructClause
	projection                []*Projection
	workingProjection         *Projection
	groupBy                   []string
	orderBy                   table.SortConfig
	havingExpression          []ConsumedElement
	havingExpressionEvaluator Evaluator
	limitSet                  bool
	limit                     int64
	lookupOptions             storage.LookupOptions
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

// ConstructClause represents a singular clause within a construct statement.
type ConstructClause struct {
	S        *node.Node
	SBinding string

	P              *predicate.Predicate
	PBinding       string
	PID            string
	PAnchorBinding string
	PTemporal      bool

	O              *triple.Object
	OBinding       string
	OID            string
	OAnchorBinding string
	OTemporal      bool

	reificationClauses        []*ReificationClause
	workingReificationClause  *ReificationClause
}

// ReificationClause represents a clause used to reify a triple.
type ReificationClause struct {
	P              *predicate.Predicate
	PBinding       string
	PID            string
	PAnchorBinding string
	PTemporal      bool

	O              *triple.Object
	OBinding       string
	OID            string
	OAnchorBinding string
	OTemporal      bool
}

// String returns a readable representation of a graph clause.
func (c *GraphClause) String() string {
	b := bytes.NewBufferString("{ ")

	// Subject section.
	if c.S != nil {
		b.WriteString(c.S.String())
	} else {
		b.WriteString(c.SBinding)
	}
	if c.SAlias != "" {
		b.WriteString(" AS ")
		b.WriteString(c.SAlias)
	}
	if c.STypeAlias != "" {
		b.WriteString(" TYPE ")
		b.WriteString(c.STypeAlias)
	}
	if c.SIDAlias != "" {
		b.WriteString(" ID ")
		b.WriteString(c.SIDAlias)
	}

	// Predicate section.
	predicate := false
	if c.P != nil {
		b.WriteString(" ")
		b.WriteString(c.P.String())
		predicate = true
	}
	if c.PBinding != "" {
		b.WriteString(" ")
		b.WriteString(c.PBinding)
	}
	if c.PID != "" {
		b.WriteString(" \"")
		b.WriteString(c.PID)
		b.WriteString("\"")
	}
	if !predicate {
		if !c.PTemporal {
			b.WriteString("@[]")
		} else {
			b.WriteString("@[")
			if c.PAnchorBinding != "" {
				b.WriteString(c.PAnchorBinding)
				if c.PAnchorAlias != "" {
					b.WriteString(" at ")
					b.WriteString(c.PAnchorAlias)
				}
			} else {
				if c.PLowerBound != nil {
					b.WriteString(c.PLowerBound.String())
				} else {
					if c.PLowerBoundAlias != "" {
						b.WriteString(c.PLowerBoundAlias)
					}
				}
				b.WriteString(",")
				if c.PUpperBound != nil {
					b.WriteString(c.PUpperBound.String())
				} else {
					if c.PUpperBoundAlias != "" {
						b.WriteString(c.PUpperBoundAlias)
					}
				}
			}
			b.WriteString("]")
		}
	}

	if c.PAlias != "" {
		b.WriteString(" AS ")
		b.WriteString(c.PAlias)
	}
	if c.PIDAlias != "" {
		b.WriteString(" ID ")
		b.WriteString(c.PIDAlias)
	}

	// Object section.
	// Node portion.
	object := false
	if c.O != nil {
		b.WriteString(" ")
		b.WriteString(c.O.String())
		object = true
	} else {
		b.WriteString(" ")
		b.WriteString(c.OBinding)
		object = true
	}
	if c.OAlias != "" {
		b.WriteString(" AS ")
		b.WriteString(c.OAlias)
	}
	if c.OTypeAlias != "" {
		b.WriteString(" TYPE ")
		b.WriteString(c.OTypeAlias)
	}
	if c.OIDAlias != "" {
		b.WriteString(" ID ")
		b.WriteString(c.OIDAlias)
	}
	// Predicate portion.
	if !object {
		if c.OBinding != "" {
			b.WriteString(" ")
			b.WriteString(c.OBinding)
		}
		if c.OID != "" {
			b.WriteString(" \"")
			b.WriteString(c.OID)
			b.WriteString("\"")
		}
		if !c.OTemporal {
			b.WriteString("[]")
		} else {
			b.WriteString("[")
			if c.OAnchorBinding != "" {
				b.WriteString(c.OAnchorBinding)
				if c.OAnchorAlias != "" {
					b.WriteString(" at ")
					b.WriteString(c.OAnchorAlias)
				}
			} else {
				if c.OLowerBound != nil {
					b.WriteString(c.OLowerBound.String())
				} else {
					if c.OLowerBoundAlias != "" {
						b.WriteString(c.OLowerBoundAlias)
					}
				}
				b.WriteString(",")
				if c.OUpperBound != nil {
					b.WriteString(c.OUpperBound.String())
				} else {
					if c.OUpperBoundAlias != "" {
						b.WriteString(c.OUpperBoundAlias)
					}
				}
			}
			b.WriteString("]")
		}
	}

	if c.OAlias != "" {
		b.WriteString(" AS ")
		b.WriteString(c.OAlias)
	}
	if c.OIDAlias != "" {
		b.WriteString(" ID ")
		b.WriteString(c.OIDAlias)
	}

	b.WriteString(" }")
	return b.String()
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

// IsEmpty will return true if there are no set values in the construct clause.
func (c *ConstructClause) IsEmpty() bool {
	return reflect.DeepEqual(c, &ConstructClause{})
}

// IsEmpty will return true if there are no set values in the reification clause.
func (c *ReificationClause) IsEmpty() bool {
	return reflect.DeepEqual(c, &ReificationClause{})
}

// BindType sets the type of a statement.
func (s *Statement) BindType(st StatementType) {
	s.sType = st
}

// Type returns the type of the statement.
func (s *Statement) Type() StatementType {
	return s.sType
}

// AddGraph adds a graph to a given statement.
func (s *Statement) AddGraph(g string) {
	s.graphNames = append(s.graphNames, g)
}

// Graphs returns the list of graphs listed on the statement.
func (s *Statement) Graphs() []storage.Graph {
	return s.graphs
}

// InputGraphNames returns the list of input graphs listed on the statement.
func (s *Statement) InputGraphNames() []string {
	return s.inputGraphNames
}

// AddInputGraph adds an input graph to a given statement.
func (s *Statement) AddInputGraph(g string) {
	s.inputGraphNames = append(s.inputGraphNames, g)
}

// InputGraphs returns the list of input graphs listed on the statement.
func (s *Statement) InputGraphs() []storage.Graph {
	return s.inputGraphs
}

// OutputGraphNames returns the list of output graphs listed on the statement.
func (s *Statement) OutputGraphNames() []string {
	return s.outputGraphNames
}

// AddOutputGraph adds an output graph to a given statement.
func (s *Statement) AddOutputGraph(g string) {
	s.outputGraphNames = append(s.outputGraphNames, g)
}

// OutputGraphs returns the list of output graphs listed on the statement.
func (s *Statement) OutputGraphs() []storage.Graph {
	return s.outputGraphs
}

// Init initializes all graphs given the graph names.
func (s *Statement) Init(ctx context.Context, st storage.Store) error {
	for _, gn := range s.graphNames {
		g, err := st.Graph(ctx, gn)
		if err != nil {
			return err
		}
		s.graphs = append(s.graphs, g)
	}
	for _, ign := range s.inputGraphNames {
		ig, err := st.Graph(ctx, ign)
		if err != nil {
			return err
		}
		s.inputGraphs = append(s.inputGraphs, ig)
	}
	for _, ogn := range s.outputGraphNames {
		og, err := st.Graph(ctx, ogn)
		if err != nil {
			return err
		}
		s.outputGraphs = append(s.outputGraphs, og)
	}
	return nil
}

// GraphNames returns the list of graphs listed on the statement.
func (s *Statement) GraphNames() []string {
	return s.graphNames
}

// AddData adds a triple to a given statement's data.
func (s *Statement) AddData(d *triple.Triple) {
	s.data = append(s.data, d)
}

// Data returns the data available for the given statement.
func (s *Statement) Data() []*triple.Triple {
	return s.data
}

// GraphPatternClauses returns the list of graph pattern clauses
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

// AddWorkingGraphClause adds the current working graph clause to the set of
// clauses that form the graph pattern.
func (s *Statement) AddWorkingGraphClause() {
	if s.workingClause != nil && !s.workingClause.IsEmpty() {
		s.pattern = append(s.pattern, s.workingClause)
	}
	s.ResetWorkingGraphClause()
}

// Projection returns the available projections in the statement.
func (s *Statement) Projection() []*Projection {
	return s.projection
}

// GroupBy returns the available group by binding in the statement.
func (s *Statement) GroupBy() []string {
	return s.groupBy
}

// OrderBy returns the available order by binding in the statement.
func (s *Statement) OrderBy() table.SortConfig {
	return s.orderBy
}

// HavingExpression returns the avaible tokens in the haaving expression.
func (s *Statement) HavingExpression() []ConsumedElement {
	return s.havingExpression
}

// HasLimit returns true if there is valid limit.
func (s *Statement) HasLimit() bool {
	return s.limitSet
}

// addToBindings adds the binding if not empty.
func addToBindings(bs map[string]int, b string) {
	if b != "" {
		bs[b]++
	}
}

// BindingsMap returns the set of bindings available on the graph clauses for the
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

// Bindings returns the list of bindings available on the graph clauses for he
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

// Len returns the length of the clauses array.
func (s bySpecificity) Len() int {
	return len(s)
}

// Swap exchanges the i and j elements in the clauses array.
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

// Projection contains the information required to project the outcome of
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
	b := bytes.NewBufferString(p.Binding)
	b.WriteString(" as ")
	b.WriteString(p.Binding)
	if p.OP != lexer.ItemError {
		b.WriteString(" via ")
		b.WriteString(p.OP.String())
		if p.Modifier != lexer.ItemError {
			b.WriteString(" ")
			b.WriteString(p.Modifier.String())
		}
	}
	return b.String()
}

// IsEmpty checks if the given projection is empty.
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

// AddWorkingProjection adds the current projection variable to the set of
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

// InputBindings returns the list of incoming bindings feed from a where clause.
func (s *Statement) InputBindings() []string {
	var res []string
	for _, p := range s.projection {
		if p.Binding != "" {
			res = append(res, p.Binding)
		}
	}
	for _, c := range s.constructClauses {
		if c.SBinding != "" {
			res = append(res, c.SBinding)
		}
		if c.PBinding != "" {
			res = append(res, c.PBinding)
		}
		if c.PAnchorBinding != "" {
			res = append(res, c.PAnchorBinding)
		}
		if c.OBinding != "" {
			res = append(res, c.OBinding)
		}
		if c.OAnchorBinding != "" {
			res = append(res, c.OAnchorBinding)
		}
		for _, r := range c.reificationClauses {
			if r.PBinding != "" {
				res = append(res, r.PBinding)
			}
			if r.PAnchorBinding != "" {
				res = append(res, r.PAnchorBinding)
			}
			if r.OBinding != "" {
				res = append(res, r.OBinding)
			}
			if r.OAnchorBinding != "" {
				res = append(res, r.OAnchorBinding)
			}
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

// GlobalLookupOptions returns the global lookup options available in the
// statement.
func (s *Statement) GlobalLookupOptions() *storage.LookupOptions {
	lo := s.lookupOptions
	return &lo
}

// ConstructClauses returns the list of construct clauses in the statement.
func (s *Statement) ConstructClauses() []*ConstructClause {
	return s.constructClauses
}

// ResetWorkingConstructClause resets the current working construct clause.
func (s *Statement) ResetWorkingConstructClause() {
	s.workingConstructClause = &ConstructClause{}
}

// WorkingConstructClause returns the current working construct clause.
func (s *Statement) WorkingConstructClause() *ConstructClause {
	return s.workingConstructClause
}

// AddWorkingConstructClause adds the current working construct clause to the set
// of construct clauses that form the construct statement.
func (s *Statement) AddWorkingConstructClause() {
	if s.workingConstructClause != nil && !s.workingConstructClause.IsEmpty() {
		s.constructClauses = append(s.constructClauses, s.workingConstructClause)
	}
	s.ResetWorkingConstructClause()
}

// ReificationClauses returns the list of reification clauses within the construct
// clause.
func (c *ConstructClause) ReificationClauses() []*ReificationClause {
	return c.reificationClauses
}

// ResetWorkingReificationClause resets the working reification clause in the
// construct clause.
func (c *ConstructClause) ResetWorkingReificationClause() {
	c.workingReificationClause = &ReificationClause{}
}

// WorkingReificationClause returns the working reification clause in the
// construct clause.
func (c *ConstructClause) WorkingReificationClause() *ReificationClause {
	return c.workingReificationClause
}

// AddWorkingReificationClause adds the working  reification clause to the set
// of reification clauses belonging to the construct clause.
func (c *ConstructClause) AddWorkingReificationClause() {
	if c.workingReificationClause != nil && !c.workingReificationClause.IsEmpty(){
		c.reificationClauses = append(c.reificationClauses, c.workingReificationClause)
	}
	c.ResetWorkingReificationClause()
}
