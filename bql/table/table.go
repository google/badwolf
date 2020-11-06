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

// Package table export the table that contains the results of a BQL query.
package table

import (
	"bytes"
	"errors"
	"fmt"
	"io"
	"log"
	"reflect"
	"sort"
	"strings"
	"sync"
	"time"

	"github.com/google/badwolf/triple/literal"
	"github.com/google/badwolf/triple/node"
	"github.com/google/badwolf/triple/predicate"
)

// Table contains the results of a BQL query. This table implementation is not
// safe for concurrency. You should take appropriate precautions if you want to
// access it concurrently and wrap to properly control concurrent operations.
type Table struct {
	// AvailableBindings in order contained on the table
	AvailableBindings []string `json:"bindings,omitempty"`
	// Data that form the table.
	Data []Row `json:"rows,omitempty"`
	// mbs is an internal map for bindings existence.
	mbs map[string]bool
	// mu provides a RW mutex for safe table manipulation operations.
	mu sync.RWMutex
}

// New returns a new table that can hold data for the given bindings. The,
// table creation will fail if there are repeated bindings.
func New(bs []string) (*Table, error) {
	m := make(map[string]bool)
	for _, b := range bs {
		m[b] = true
	}
	if len(m) != len(bs) {
		return nil, fmt.Errorf("table.New does not allow duplicated bindings in %s", bs)
	}
	return &Table{
		AvailableBindings: bs,
		mbs:               m,
	}, nil
}

// Cell contains one of the possible values that form rows.
type Cell struct {
	S *string              `json:"s,omitempty"`
	N *node.Node           `json:"node,omitempty"`
	P *predicate.Predicate `json:"pred,omitempty"`
	L *literal.Literal     `json:"lit,omitempty"`
	T *time.Time           `json:"time,omitempty"`
}

// String returns a readable representation of a cell.
func (c *Cell) String() string {
	if c.S != nil {
		return *c.S
	}
	if c.N != nil {
		return c.N.String()
	}
	if c.P != nil {
		return c.P.String()
	}
	if c.L != nil {
		return c.L.String()
	}
	if c.T != nil {
		return c.T.Format(time.RFC3339Nano)
	}
	return "<NULL>"
}

// Row represents a collection of cells.
type Row map[string]*Cell

// ToTextLine converts a row into line of text. To do so, it requires the list
// of bindings of the table, and the separator you want to use. If the separator
// is empty tabs will be used.
func (r Row) ToTextLine(res *bytes.Buffer, bs []string, sep string) error {
	cnt := len(bs)
	if sep == "" {
		sep = "\t"
	}
	for _, b := range bs {
		cnt--
		v := "<NULL>"
		if c, ok := r[b]; ok {
			v = c.String()
		}
		if _, err := res.WriteString(v); err != nil {
			return err
		}
		if cnt > 0 {
			res.WriteString(sep)
		}
	}
	return nil
}

// AddRow adds a row to the end of a table. For performance reasons, it does not
// check that all bindings are set, nor that they are declared on table
// creation. BQL builds valid tables, if you plan to create tables on your own
// you should be careful to provide valid rows.
func (t *Table) AddRow(r Row) {
	t.mu.Lock()
	if len(r) > 0 {
		delete(r, "")
		t.Data = append(t.Data, r)
	}
	t.mu.Unlock()
}

// NumRows returns the number of rows currently available on the table.
func (t *Table) NumRows() int {
	t.mu.RLock()
	defer t.mu.RUnlock()
	return len(t.Data)
}

// Row returns the requested row. Rows start at 0. Also, if you request a row
// beyond it will return nil, and the ok boolean will be false.
func (t *Table) Row(i int) (Row, bool) {
	t.mu.RLock()
	defer t.mu.RUnlock()
	if i < 0 || i >= len(t.Data) {
		return nil, false
	}
	return t.Data[i], true
}

// Rows returns all the available rows.
func (t *Table) Rows() []Row {
	t.mu.RLock()
	defer t.mu.RUnlock()
	return t.Data
}

// unsafeAddBindings add the new bindings provided to the table bypassing the lock.
func (t *Table) unsafeAddBindings(bs []string) {
	for _, b := range bs {
		if !t.mbs[b] {
			t.mbs[b] = true
			t.AvailableBindings = append(t.AvailableBindings, b)
		}
	}
}

// AddBindings add the new bindings provided to the table.
func (t *Table) AddBindings(bs []string) {
	t.mu.Lock()
	t.unsafeAddBindings(bs)
	t.mu.Unlock()
}

// ProjectBindings replaces the current bindings with the projected one. The
// provided bindings needs to be a subset of the original bindings. If the
// provided bindings are not a subset of the original ones, the projection will
// fail, leave the table unmodified, and return an error. The projection only
// modify the bindings, but does not drop non projected data.
func (t *Table) ProjectBindings(bs []string) error {
	t.mu.Lock()
	defer t.mu.Unlock()
	if len(t.Data) == 0 || len(t.mbs) == 0 {
		return nil
	}
	for _, b := range bs {
		if !t.mbs[b] {
			return fmt.Errorf("cannot project against unknown binding %s; known bindinds are %v", b, t.AvailableBindings)
		}
	}
	t.AvailableBindings = []string{}
	t.mbs = make(map[string]bool)
	t.unsafeAddBindings(bs)
	return nil
}

// HasBinding returns true if the binding currently exist on the table.
func (t *Table) HasBinding(b string) bool {
	t.mu.RLock()
	defer t.mu.RUnlock()
	return t.mbs[b]
}

// Bindings returns the bindings contained on the tables.
func (t *Table) Bindings() []string {
	t.mu.RLock()
	defer t.mu.RUnlock()
	return t.AvailableBindings
}

// equalBindings returns true if the bindings are the same, false otherwise.
func equalBindings(b1, b2 map[string]bool) bool {
	if len(b1) != len(b2) {
		return false
	}
	for k := range b1 {
		if !b2[k] {
			return false
		}
	}
	return true
}

// AppendTable appends the content of the provided table. It will fail it the
// target table is not empty and the bindings do not match.
func (t *Table) AppendTable(t2 *Table) error {
	t.mu.Lock()
	defer t.mu.Unlock()
	if t2 == nil {
		return nil
	}
	if len(t.AvailableBindings) > 0 && !equalBindings(t.mbs, t2.mbs) {
		return fmt.Errorf("AppendTable can only append to an empty table or equally binded table; instead got %v and %v", t.AvailableBindings, t2.AvailableBindings)
	}
	if len(t.AvailableBindings) == 0 {
		t.AvailableBindings, t.mbs = t2.AvailableBindings, t2.mbs
	}
	t.Data = append(t.Data, t2.Data...)
	return nil
}

// disjointBinding returns true if they are not overlapping bindings, false
// otherwise.
func disjointBindings(b1, b2 map[string]bool) bool {
	for k := range b1 {
		if b2[k] {
			return false
		}
	}
	return true
}

// MergeRows takes a list of rows and returns a new map containing both.
func MergeRows(ms []Row) Row {
	res := make(map[string]*Cell)
	for _, om := range ms {
		for k, v := range om {
			if _, ok := res[k]; !ok {
				res[k] = v
			}
		}
	}
	return res
}

// DotProduct does the dot product with the provided table
func (t *Table) DotProduct(t2 *Table) error {
	t.mu.Lock()
	defer t.mu.Unlock()
	if !disjointBindings(t.mbs, t2.mbs) {
		return fmt.Errorf("DotProduct operations requires disjoint bindings; instead got %v and %v", t.mbs, t2.mbs)
	}
	// Update the table metadata.
	m := make(map[string]bool)
	for k := range t.mbs {
		m[k] = true
	}
	for k := range t2.mbs {
		m[k] = true
	}
	t.mbs = m
	t.AvailableBindings = []string{}
	for k := range t.mbs {
		t.AvailableBindings = append(t.AvailableBindings, k)
	}
	// Update the data.
	td := t.Data
	cnt, size := 0, len(td)*len(t2.Data)
	t.Data = make([]Row, size, size) // Preallocate resulting table.
	for _, r1 := range td {
		for _, r2 := range t2.Data {
			t.Data[cnt] = MergeRows([]Row{r1, r2})
			cnt++
		}
	}
	return nil
}

// LeftOptionalJoin does a left join using the provided right table.
func (t *Table) LeftOptionalJoin(t2 *Table) error {
	if equalBindings(t.mbs, t2.mbs) || len(t2.mbs) == 0 {
		// Both tables have the same bindings. Hence, the optinal results of
		// the second table can be ignored and keep the left originol table
		// untouched.
		return nil
	}
	if disjointBindings(t.mbs, t2.mbs) {
		// The tables has nothing in commnon. Hence, we are going to treat it
		// as a regular cross product.
		return t.DotProduct(t2)
	}
	// There are some overlapping bindings. That requires to sort both tables
	// by the overlapping bindings and and then create the new rows merging
	// both row ranges.
	joinWithRange(t, t2)
	return nil
}

// joinWithRange joins the two tables with overlapping bindings triggering
// range expansions if needed.
func joinWithRange(t, t2 *Table) {
	ibs := intersectBindings(t.mbs, t2.mbs)
	ubs := unionBindings(t.mbs, t2.mbs)

	var sbs []string
	for k := range ibs {
		sbs = append(sbs, k)
	}
	sortTablesData(t, t2, sbs)

	// Create the comparison for row order.
	var scfg SortConfig
	for _, k := range sbs {
		scfg = append(scfg, sortConfig{Binding: k})
	}

	t.mu.Lock()
	defer t.mu.Unlock()
	t2.mu.Lock()
	defer t2.mu.Unlock()
	var res []Row
	t2d := t2.Data
	lj, j := 0, 0
	for _, t1r := range t.Data {
		extended := false
		for j < len(t2d) && (joinable(t1r, t2d[j], ibs) || rowLess(t2d[j], t1r, scfg)) {
			if joinable(t1r, t2d[j], ibs) {
				res = append(res, extendRowWith(t1r, t2d[j]))
				extended = true
				j++
				continue
			}
			// Advante the row index for the right table while the rows are
			// smaller than the current one.
			for j < len(t2d) && rowLess(t2d[j], t1r, scfg) {
				j++
				lj = j
			}
		}
		if !extended {
			res = append(res, extendRow(t1r, ubs))
		}
		j = lj
	}

	// Udate the table.
	t.mbs = ubs
	t.AvailableBindings = nil
	for k := range ubs {
		t.AvailableBindings = append(t.AvailableBindings, k)
	}
	t.Data = res
}

// extendRow extends the row with the missing bindings.
func extendRow(r Row, bs map[string]bool) Row {
	nr := make(Row)
	for k, v := range r {
		nr[k] = v
	}
	for k := range bs {
		if _, ok := nr[k]; ok {
			continue
		}
		nr[k] = &Cell{}
	}
	return nr
}

// extendRowWith extends the row with the missing bindings.
func extendRowWith(r, r2 Row) Row {
	nr := make(Row)
	for k, v := range r {
		nr[k] = v
	}
	for k, v := range r2 {
		if _, ok := nr[k]; ok {
			continue
		}
		nr[k] = v
	}
	return nr
}

// intersecBindings returns a map with the intersection of bindings
func intersectBindings(bs1, bs2 map[string]bool) map[string]bool {
	res := make(map[string]bool)
	for k1 := range bs1 {
		if _, ok := bs2[k1]; ok {
			res[k1] = true
		}
	}
	return res
}

// unionBindings returns a map with the intersection of bindings
func unionBindings(bs1, bs2 map[string]bool) map[string]bool {
	res := make(map[string]bool)
	for k := range bs1 {
		res[k] = true
	}
	for k := range bs2 {
		res[k] = true
	}
	return res
}

// sortTablesData sorts the two provided row slices based on the provided
// bindings.
func sortTablesData(t, t2 *Table, bs []string) {
	t.mu.Lock()
	defer t.mu.Unlock()
	t2.mu.Lock()
	defer t2.mu.Unlock()
	d, d2 := t.Data, t2.Data
	var scfg SortConfig
	for _, k := range bs {
		scfg = append(scfg, sortConfig{Binding: k})
	}
	sortIt := func(dt []Row) {
		sort.Slice(dt, func(i, j int) bool {
			return rowLess(dt[i], dt[j], scfg)
		})
	}
	sortIt(d)
	sortIt(d2)
}

// joinable return true if the values of the provided bindings are equal
// for both provided rows.
func joinable(r1, r2 Row, bs map[string]bool) bool {
	for k := range bs {
		if !reflect.DeepEqual(r1[k], r2[k]) {
			return false
		}
	}
	return true
}

// DeleteRow removes the row at position i from the table. This should be used
// carefully. If you are planning to delete a large volume of rows consider
// creating a new table and just copy the rows you need. This operation relies
// on slices and it *will* *not* release the underlying deleted row. Please,
// see https://blog.golang.org/go-slices-usage-and-internals for a detailed
// explanation.
func (t *Table) DeleteRow(i int) error {
	t.mu.Lock()
	defer t.mu.Unlock()
	if i < 0 || i >= len(t.Data) {
		return fmt.Errorf("cannot delete row %d from a table with %d rows", i, len(t.Data))
	}
	t.Data = append(t.Data[:i], t.Data[i+1:]...)
	return nil
}

// Truncate flushes all the data away. It still retains all set bindings.
func (t *Table) Truncate() {
	t.mu.Lock()
	defer t.mu.Unlock()
	t.Data = nil
}

// Limit keeps the initial ith rows.
func (t *Table) Limit(i int64) {
	t.mu.Lock()
	defer t.mu.Unlock()
	if int64(len(t.Data)) > i {
		td := make([]Row, i, i) // Preallocate resulting table.
		copy(td, t.Data[:i])
		t.Data = td
	}
}

// SortConfig contains the sorting information. Contains the binding order
// to use while sorting as well as the direction for each of them to use.
type SortConfig []sortConfig
type sortConfig struct {
	Binding string
	Desc    bool
}

func (s SortConfig) String() string {
	b := bytes.NewBufferString("[ ")
	for _, sc := range s {
		b.WriteString(sc.Binding)
		b.WriteString("->")
		if sc.Desc {
			b.WriteString("DESC ")
		} else {
			b.WriteString("ASC ")
		}
	}
	b.WriteString("]")
	return b.String()
}

type bySortConfig struct {
	rows []Row
	cfg  SortConfig
}

// Len returns the length of the table.
func (c bySortConfig) Len() int {
	return len(c.rows)
}

// Swap exchange the i and j rows in the table.
func (c bySortConfig) Swap(i, j int) {
	c.rows[i], c.rows[j] = c.rows[j], c.rows[i]
}

func stringLess(rsi, rsj string, desc bool) int {
	si, sj := strings.TrimSpace(rsi), strings.TrimSpace(rsj)
	if (si == "" && sj == "") || si == sj {
		return 0
	}
	b := 1
	if si < sj {
		b = -1
	}
	if desc {
		b *= -1
	}
	return b
}

// CellString create a pointer for the provided string.
func CellString(s string) *string {
	return &s
}
func rowLess(ri, rj Row, c SortConfig) bool {
	if c == nil {
		return false
	}
	cfg, last := c[0], len(c) == 1
	ci, ok := ri[cfg.Binding]
	if !ok {
		log.Fatalf("Could not retrieve binding %q! %v %v", cfg.Binding, ri, rj)
	}
	cj, ok := rj[cfg.Binding]
	if !ok {
		log.Fatalf("Could not retrieve binding %q! %v %v", cfg.Binding, ri, rj)
	}
	si, sj := "", ""
	// Check if it has a string.
	if ci.S != nil && cj.S != nil {
		si, sj = *ci.S, *cj.S
	}
	// Check if it has a nodes.
	if ci.N != nil && cj.N != nil {
		si, sj = ci.N.String(), cj.N.String()
	}
	// Check if it has a predicates.
	if ci.P != nil && cj.P != nil {
		si, sj = ci.P.String(), cj.P.String()
	}
	// Check if it has a literal.
	if ci.L != nil && cj.L != nil {
		si, sj = ci.L.ToComparableString(), cj.L.ToComparableString()
	}
	// Check if it has a time anchor.
	if ci.T != nil && cj.T != nil {
		si, sj = ci.T.Format(time.RFC3339Nano), cj.T.Format(time.RFC3339Nano)
	}
	l := stringLess(si, sj, cfg.Desc)
	if l < 0 {
		return true
	}
	if l > 0 || last {
		return false
	}
	return rowLess(ri, rj, c[1:])
}

// Less returns true if the i row is less than j one.
func (c bySortConfig) Less(i, j int) bool {
	ri, rj, cfg := c.rows[i], c.rows[j], c.cfg
	return rowLess(ri, rj, cfg)
}

// unsafeSort sorts the table given a sort configuration bypassing the lock.
func (t *Table) unsafeSort(cfg SortConfig) {
	if cfg == nil {
		return
	}
	sort.Sort(bySortConfig{t.Data, cfg})
}

// Sort sorts the table given a sort configuration.
func (t *Table) Sort(cfg SortConfig) {
	t.mu.Lock()
	t.unsafeSort(cfg)
	t.mu.Unlock()
}

// Accumulator type represents a generic accumulator for independent values
// expressed as the element of the array slice. Returns the values after being
// accumulated. If the wrong type is passed in, it will crash casting the
// interface.
type Accumulator interface {
	// Accumulate takes the given value and accumulates it to the current state.
	Accumulate(interface{}) (interface{}, error)

	// Resets the current state back to the original one.
	Reset()
}

// sumInt64 implements an accumulator that sum int64 values.
type sumInt64 struct {
	initialState int64
	state        int64
}

// Accumulate takes the given value and accumulates it to the current state.
func (s *sumInt64) Accumulate(v interface{}) (interface{}, error) {
	c := v.(*Cell)
	l := c.L
	if l == nil {
		return nil, fmt.Errorf("not a valid literal it cell %v", c)
	}
	iv, err := l.Int64()
	if err != nil {
		return s.state, err
	}
	s.state += iv
	return s.state, nil
}

// Resets the current state back to the original one.
func (s *sumInt64) Reset() {
	s.state = s.initialState
}

// NewSumInt64LiteralAccumulator accumulates the int64 types of a literal.
func NewSumInt64LiteralAccumulator(s int64) Accumulator {
	return &sumInt64{s, s}
}

// sumFloat64 implements an accumulator that sum float64 values.
type sumFloat64 struct {
	initialState float64
	state        float64
}

// Accumulate takes the given value and accumulates it to the current state.
func (s *sumFloat64) Accumulate(v interface{}) (interface{}, error) {
	c := v.(*Cell)
	l := c.L
	if l == nil {
		return nil, fmt.Errorf("not a valid literal it cell %v", c)
	}
	iv, err := l.Float64()
	if err != nil {
		return s.state, err
	}
	s.state += iv
	return s.state, nil
}

// Resets the current state back to the original one.
func (s *sumFloat64) Reset() {
	s.state = s.initialState
}

// NewSumFloat64LiteralAccumulator accumulates the int64 types of a literal.
func NewSumFloat64LiteralAccumulator(s float64) Accumulator {
	return &sumFloat64{s, s}
}

// countAcc implements an accumulator that count accumulation occurrences.
type countAcc struct {
	state int64
}

// Accumulate takes the given value and accumulates it to the current state.
func (c *countAcc) Accumulate(v interface{}) (interface{}, error) {
	c.state++
	return c.state, nil
}

// Resets the current state back to the original one.
func (c *countAcc) Reset() {
	c.state = 0
}

// NewCountAccumulator accumulates the int64 types of a literal.
func NewCountAccumulator() Accumulator {
	return &countAcc{0}
}

// countDistinctAcc implements an accumulator that count accumulation occurrences.
type countDistinctAcc struct {
	state map[string]int64
}

// Accumulate takes the given value and accumulates it to the current state.
func (c *countDistinctAcc) Accumulate(v interface{}) (interface{}, error) {
	vs := fmt.Sprintf("%v", v)
	c.state[vs]++
	return int64(len(c.state)), nil
}

// Resets the current state back to the original one.
func (c *countDistinctAcc) Reset() {
	c.state = make(map[string]int64)
}

// NewCountDistinctAccumulator counts calls by incrementing the internal state
// only if the value has not been seen before.
func NewCountDistinctAccumulator() Accumulator {
	return &countDistinctAcc{make(map[string]int64)}
}

// groupRangeReduce takes a sorted range and generates a new row containing
// the aggregated columns and the non aggregated ones.
func (t *Table) groupRangeReduce(i, j int, alias map[string]string, acc map[string]Accumulator) (Row, error) {
	t.mu.Lock()
	defer t.mu.Unlock()
	if i > j {
		return nil, fmt.Errorf("cannot aggregate empty ranges [%d, %d)", i, j)
	}
	// Initialize the range and accumulator results.
	rng := t.Data[i:j]
	vaccs := make(map[string]interface{})
	// Reset the accumulators.
	for _, a := range acc {
		a.Reset()
	}
	// Aggregate the range using the provided aggregators.
	for _, r := range rng {
		for b, a := range acc {
			av, err := a.Accumulate(r[b])
			if err != nil {
				return nil, err
			}
			vaccs[b] = av
		}
	}
	// Create a new row based on the resulting aggregations with the proper
	// binding aliasing and the non aggregated values.
	newRow := Row{}
	for b, v := range rng[0] {
		acc, ok := vaccs[b]
		if !ok {
			if a, ok := alias[b]; ok {
				newRow[a] = v
			} else {
				newRow[b] = v
			}
		} else {
			a, ok := alias[b]
			if !ok {
				return nil, fmt.Errorf("aggregated bindings require and alias; binding %s missing alias", b)
			}
			// Accumulators currently only can return numeric literals.
			switch acc.(type) {
			case int64:
				l, err := literal.DefaultBuilder().Build(literal.Int64, acc)
				if err != nil {
					return nil, err
				}
				newRow[a] = &Cell{L: l}
			case float64:
				l, err := literal.DefaultBuilder().Build(literal.Float64, acc)
				if err != nil {
					return nil, err
				}
				newRow[a] = &Cell{L: l}
			default:
				return nil, fmt.Errorf("aggregation of binding %s returned unknown value %v or type", b, acc)
			}
		}
	}
	return newRow, nil
}

// AliasAccPair contains the in, out alias, and the optional accumulator to use.
type AliasAccPair struct {
	InAlias  string
	OutAlias string
	Acc      Accumulator
}

// unsafeFullGroupRangeReduce takes a sorted range and generates a new row containing
// the aggregated columns and the non aggregated ones. This call bypasses the lock.
func (t *Table) unsafeFullGroupRangeReduce(i, j int, acc map[string]map[string]AliasAccPair) (Row, error) {
	if i > j {
		return nil, fmt.Errorf("cannot aggregate empty ranges [%d, %d)", i, j)
	}
	// Initialize the range and accumulator results.
	rng := t.Data[i:j]
	// Reset the accumulators.
	for _, aap := range acc {
		for _, a := range aap {
			if a.Acc != nil {
				a.Acc.Reset()
			}
		}
	}
	// Aggregate the range using the provided aggregators.
	vaccs := make(map[string]map[string]interface{})
	for _, r := range rng {
		for _, aap := range acc {
			for _, a := range aap {
				if a.Acc == nil {
					continue
				}
				av, err := a.Acc.Accumulate(r[a.InAlias])
				if err != nil {
					return nil, err
				}
				if _, ok := vaccs[a.InAlias]; !ok {
					vaccs[a.InAlias] = make(map[string]interface{})
				}
				vaccs[a.InAlias][a.OutAlias] = av
			}
		}
	}
	// Create a new row based on the resulting aggregations with the proper
	// binding aliasing and the non aggregated values.
	newRow := Row{}
	for b, v := range rng[0] {
		for _, app := range acc[b] { //macc {
			if app.Acc == nil {
				newRow[app.OutAlias] = v
			} else {
				// Accumulators currently only can return numeric literals.
				switch vaccs[app.InAlias][app.OutAlias].(type) {
				case int64:
					l, err := literal.DefaultBuilder().Build(literal.Int64, vaccs[app.InAlias][app.OutAlias])
					if err != nil {
						return nil, err
					}
					newRow[app.OutAlias] = &Cell{L: l}
				case float64:
					l, err := literal.DefaultBuilder().Build(literal.Float64, vaccs[app.InAlias][app.OutAlias])
					if err != nil {
						return nil, err
					}
					newRow[app.OutAlias] = &Cell{L: l}
				default:
					return nil, fmt.Errorf("aggregation of binding %s returned unknown value %v or type", b, acc)
				}
			}
		}
	}
	if len(newRow) == 0 {
		return nil, errors.New("failed to reduced row range returning an empty one")
	}
	return newRow, nil
}

// toMap converts a list of alias and acc pairs into a nested map. The first
// key is the input binding, the second one is the output binding.
func toMap(aaps []AliasAccPair) map[string]map[string]AliasAccPair {
	resMap := make(map[string]map[string]AliasAccPair)
	for _, aap := range aaps {
		m, ok := resMap[aap.InAlias]
		if !ok {
			m = make(map[string]AliasAccPair)
			resMap[aap.InAlias] = m
		}
		m[aap.OutAlias] = aap
	}
	return resMap
}

// Reduce alters the table by sorting and then range grouping the table data.
// In order to group reduce the table, we sort the table and then apply the
// accumulator functions to each group. Finally, the table metadata gets
// updated to reflect the reduce operation.
func (t *Table) Reduce(cfg SortConfig, aaps []AliasAccPair) error {
	maaps := toMap(aaps)
	t.mu.Lock()
	defer t.mu.Unlock()
	// Input validation tests.
	if len(t.AvailableBindings) != len(maaps) {
		return fmt.Errorf("table.Reduce cannot project bindings; current %v, requested %v", t.AvailableBindings, aaps)
	}
	for _, b := range t.AvailableBindings {
		if _, ok := maaps[b]; !ok {
			return fmt.Errorf("table.Reduce missing binding alias for %q", b)
		}
	}
	cnt := 0
	for b := range maaps {
		if _, ok := t.mbs[b]; !ok {
			return fmt.Errorf("table.Reduce unknown reducer binding %q; available bindings %v", b, t.AvailableBindings)
		}
		cnt++
	}
	if cnt != len(t.AvailableBindings) {
		return fmt.Errorf("table.Reduce invalid reduce configuration in cfg=%v, aap=%v for table with binding %v", cfg, aaps, t.AvailableBindings)
	}
	// Valid reduce configuration. Reduce sorts the table and then reduces
	// contiguous groups row groups.
	if len(t.Data) == 0 {
		return nil
	}
	t.unsafeSort(cfg)
	last, lastIdx, current, newData := "", 0, "", []Row{}
	id := func(r Row) string {
		res := bytes.NewBufferString("")
		for _, c := range cfg {
			res.WriteString(r[c.Binding].String())
			res.WriteString(";")
		}
		return res.String()
	}
	for idx, r := range t.Data {
		current = id(r)
		// First time.
		if last == "" {
			last, lastIdx = current, idx
			continue
		}
		// Still in the same group.
		if last == current {
			continue
		}
		// A group reduce operation is needed.
		nr, err := t.unsafeFullGroupRangeReduce(lastIdx, idx, maaps)
		if err != nil {
			return err
		}
		newData = append(newData, nr)
		last, lastIdx = current, idx
	}
	nr, err := t.unsafeFullGroupRangeReduce(lastIdx, len(t.Data), maaps)
	if err != nil {
		return err
	}
	newData = append(newData, nr)
	// Update the table.
	t.AvailableBindings, t.mbs = []string{}, make(map[string]bool)
	for _, aap := range aaps {
		if !t.mbs[aap.OutAlias] {
			t.AvailableBindings = append(t.AvailableBindings, aap.OutAlias)
		}
		t.mbs[aap.OutAlias] = true
	}
	t.Data = newData
	return nil
}

// Filter removes all the rows where the provided function returns true, returning
// by the end the final number of rows removed.
func (t *Table) Filter(f func(Row) bool) int {
	t.mu.Lock()
	defer t.mu.Unlock()
	var newData []Row
	for _, r := range t.Data {
		if !f(r) {
			newData = append(newData, r)
		}
	}
	nRowsRemoved := len(t.Data) - len(newData)
	t.Data = newData
	return nRowsRemoved
}

// ToText convert the table into a readable text versions. It requires the
// separator to be used between cells.
func (t *Table) ToText(sep string) (*bytes.Buffer, error) {
	t.mu.RLock()
	defer t.mu.RUnlock()
	res, row := &bytes.Buffer{}, &bytes.Buffer{}
	res.WriteString(strings.Join(t.AvailableBindings, sep))
	res.WriteString("\n")
	for _, r := range t.Data {
		err := r.ToTextLine(row, t.AvailableBindings, sep)
		if err != nil {
			return nil, err
		}
		if _, err := res.Write(row.Bytes()); err != nil {
			return nil, err
		}
		if _, err := res.WriteString("\n"); err != nil {
			return nil, err
		}
		row.Reset()
	}
	return res, nil
}

// String attempts to force serialize the table into a string.
func (t *Table) String() string {
	t.mu.RLock()
	defer t.mu.RUnlock()
	b, err := t.ToText("\t")
	if err != nil {
		return fmt.Sprintf("Failed to serialize to text! Error: %s", err)
	}
	return b.String()
}

// ToJSON convert the table intotext versions. It requires the
// separator to be used between cells JSON.
func (t *Table) ToJSON(w io.Writer) {
	t.mu.RLock()
	defer t.mu.RUnlock()
	w.Write([]byte(`{ "bindings": [`))

	if len(t.AvailableBindings) > 0 {
		w.Write([]byte(`"`))
		w.Write([]byte(strings.Join(t.AvailableBindings, `", "`)))
		w.Write([]byte(`"`))
	}

	w.Write([]byte(`], "rows": [`))

	rc := len(t.Data)
	for _, r := range t.Data {
		if len(r) > 0 {
			w.Write([]byte(`{ `))

			cc := len(t.AvailableBindings)
			for _, k := range t.AvailableBindings {
				if k != "" {
					c := r[k]
					w.Write([]byte(`"`))
					w.Write([]byte(k))
					w.Write([]byte(`": {"`))

					if c.S != nil {
						w.Write([]byte(`string": "`))
						w.Write([]byte(strings.Replace(*c.S, `"`, `\"`, -1)))
					} else if c.N != nil {
						w.Write([]byte(`node": "`))
						w.Write([]byte(strings.Replace(c.N.String(), `"`, `\"`, -1)))
					} else if c.P != nil {
						w.Write([]byte(`pred": "`))
						w.Write([]byte(strings.Replace(c.P.String(), `"`, `\"`, -1)))

					} else if c.L != nil {
						w.Write([]byte(`lit": "`))
						w.Write([]byte(strings.Replace(c.L.String(), `"`, `\"`, -1)))

					} else if c.T != nil {
						w.Write([]byte(`anchor": "`))
						w.Write([]byte(strings.Replace(c.T.Format(time.RFC3339Nano), `"`, `\"`, -1)))
					}

					w.Write([]byte(`"}`))
					if cc > 1 {
						w.Write([]byte(`,`))
					}
					w.Write([]byte(` `))
				}
				cc--
			}
			w.Write([]byte(` }`))
			if rc > 1 {
				w.Write([]byte(`, `))
			}
		}
		rc--
	}

	w.Write([]byte(`] }`))
}
