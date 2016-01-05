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
	"fmt"
	"sort"
	"strings"
	"time"

	"github.com/google/badwolf/triple/literal"
	"github.com/google/badwolf/triple/node"
	"github.com/google/badwolf/triple/predicate"
)

// Table contains the results of a BQL query. This table implementation is not
// safe for concurrency. You should take appropiate precautions if you want to
// access it concurrently and wrap to properly control concurrent operations.
type Table struct {
	bs   []string
	mbs  map[string]bool
	data []Row
}

// New returns a new table that can hold data for the the given bindings. The,
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
		bs:  bs,
		mbs: m,
	}, nil
}

// Cell contains one of the possible values that form rows.
type Cell struct {
	S string
	N *node.Node
	P *predicate.Predicate
	L *literal.Literal
	T *time.Time
}

// String returns a readable representation of a cell.
func (c *Cell) String() string {
	if c.S != "" {
		return c.S
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

// AddRow adds a row to the end of a table. For preformance reasons, it does not
// check that all bindindgs are set, nor that they are declared on table
// creation. BQL builds valid tables, if you plan to create tables on your own
// you should be carful to provide valid rows.
func (t *Table) AddRow(r Row) {
	t.data = append(t.data, r)
}

// NumRows returns the number of rows currently available on the table.
func (t *Table) NumRows() int {
	return len(t.data)
}

// Row returns the requested row. Rows start at 0. Also, if you request a row
// beyond it will return nil, and the ok boolean will be false.
func (t *Table) Row(i int) (Row, bool) {
	if i >= len(t.data) {
		return nil, false
	}
	return t.data[i], true
}

// Rows returns all the available rows.
func (t *Table) Rows() []Row {
	return t.data
}

// AddBindings add the new binings provided to the table.
func (t *Table) AddBindings(bs []string) {
	for _, b := range bs {
		if _, ok := t.mbs[b]; !ok {
			t.mbs[b] = true
			t.bs = append(t.bs, b)
		}
	}
}

// ProjectBindings replaces the current bidings with the projected one. The
// provided bindings needs to be a subsed of the original bindings. If the
// provided bindings are not a subset of the original ones, the projection will
// fail, leave the table unmodified, and return an error. The projection only
// modify the bindings, but does not drop non projected data.
func (t *Table) ProjectBindings(bs []string) error {
	for _, b := range bs {
		if !t.mbs[b] {
			return fmt.Errorf("cannot project against unknow binding %s; known bindinds are %v", b, t.bs)
		}
	}
	t.bs = []string{}
	t.mbs = make(map[string]bool)
	t.AddBindings(bs)
	return nil
}

// HasBinding returns true if the binding currently exist on the teable.
func (t *Table) HasBinding(b string) bool {
	return t.mbs[b]
}

// Bindings returns the bindings contained on the tables.
func (t *Table) Bindings() []string {
	return t.bs
}

// ToText convert the table into a readable text versions. It requires the
// separator to be used between cells.
func (t *Table) ToText(sep string) (*bytes.Buffer, error) {
	res, row := &bytes.Buffer{}, &bytes.Buffer{}
	res.WriteString(strings.Join(t.bs, sep))
	res.WriteString("\n")
	for _, r := range t.data {
		err := r.ToTextLine(row, t.bs, sep)
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
	b, err := t.ToText("\t")
	if err != nil {
		return fmt.Sprintf("Failed to serialize to text! Error: %s", err)
	}
	return b.String()
}

// equalBindings returns true if the bindings are the same, false otherwise.
func equalBindings(b1, b2 map[string]bool) bool {
	if len(b1) != len(b2) {
		return false
	}
	for k := range b1 {
		if _, ok := b2[k]; !ok {
			return false
		}
	}
	return true
}

// AppendTable appends the content of the provided table. It will fail it the
// target table is not empty and the binidngs do not match.
func (t *Table) AppendTable(t2 *Table) error {
	if len(t.Bindings()) > 0 && !equalBindings(t.mbs, t2.mbs) {
		return fmt.Errorf("AppendTable can only append to an empty table or equally binded table; intead got %v and %v", t.bs, t2.bs)
	}
	if len(t.Bindings()) == 0 {
		t.bs, t.mbs = t2.bs, t2.mbs
	}
	t.data = append(t.data, t2.data...)
	return nil
}

// disjointBinding returns true if they are not overlapping bindings, false
// otherwise.
func disjointBinding(b1, b2 map[string]bool) bool {
	m := make(map[string]int)
	for k := range b1 {
		m[k]++
	}
	for k := range b2 {
		m[k]++
	}
	for _, cnt := range m {
		if cnt != 1 {
			return false
		}
	}
	return true
}

// MergeRows takes a list of rors and returns a new map containing both.
func MergeRows(ms []Row) Row {
	res := make(map[string]*Cell)
	for _, om := range ms {
		for k, v := range om {
			res[k] = v
		}
	}
	return res
}

// DotProduct does the doot product with the provided tatble
func (t *Table) DotProduct(t2 *Table) error {
	if !disjointBinding(t.mbs, t2.mbs) {
		return fmt.Errorf("DotProduct operations requires disjoint bindingts; instead got %v and %v", t.mbs, t2.mbs)
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
	t.bs = []string{}
	for k := range t.mbs {
		t.bs = append(t.bs, k)
	}
	// Update the data.
	td := t.data
	t.data = []Row{}
	for _, r1 := range td {
		for _, r2 := range t2.data {
			t.data = append(t.data, MergeRows([]Row{r1, r2}))
		}
	}
	return nil
}

// DeleteRow removes the row at position i from the table.
func (t *Table) DeleteRow(i int) error {
	if i < 0 || i >= len(t.data) {
		return fmt.Errorf("cannot delete row %d from a table with %d rows", i, len(t.data))
	}
	t.data = append(t.data[:i], t.data[i+1:]...)
	return nil
}

// Truncate flushes all the data away. It still retains all set bindings.
func (t *Table) Truncate() {
	t.data = []Row{}
}

// SortConfig contains the sorting information. Contains the binding order
// to use wile sorting as well as the deriction for each of them to use.
type SortConfig []struct {
	Binding string
	Desc    bool
}

type bySortConfig struct {
	rows []Row
	cfg  SortConfig
}

// Len returns the lenght of the table.
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

func rowLess(ri, rj Row, c SortConfig) bool {
	if c == nil {
		return false
	}
	cfg, last := c[0], len(c) == 1
	ci, cj := ri[cfg.Binding], rj[cfg.Binding]
	si, sj := "", ""
	// Check if it has a string.
	if ci.S != "" && cj.S != "" {
		si, sj = ci.S, cj.S
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

// Sort sorts the table given a sort configuration.
func (t *Table) Sort(cfg SortConfig) {
	if cfg == nil {
		return
	}
	sort.Sort(bySortConfig{t.data, cfg})
}
