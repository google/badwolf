// Copyright 2016 Google Inc. All rights reserved.
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

// Package compliance provides the tools to validate the compliance of driver
// implementations and BQL behavior testing. The compliance package is built
// around stories. A story is a collection of graphs and a sequence of
// assertions against the provided data. An assertion is defined by a tuple
// containing a BQL, the execution status, and the expected result table.
package compliance

import (
	"encoding/json"
	"fmt"
	"time"

	"github.com/google/badwolf/bql/table"
	"github.com/google/badwolf/triple/literal"
	"github.com/google/badwolf/triple/node"
	"github.com/google/badwolf/triple/predicate"
)

// Graph contains the graph binding name and the list of parseable triples
// that define it.
type Graph struct {
	// ID of the binding name to use for the graph.
	ID string

	// Facts contains the parseable triples which define the graph.
	Facts []string
}

// Assertion contains a BQL, the expected status of the BQL query execution,
// and the returned results table.
type Assertion struct {
	// Requires of the assertion.
	Requires string

	// Statement contains the BQL query to assert.
	Statement string

	// WillFail indicates if the query should fail with and error.
	WillFail bool

	// MustReturn contains the table  containing the expected results provided
	// by the BQL statement execution.
	MustReturn []map[string]string

	// The equivalent table representation of the MustReturn information.
	table *table.Table
}

// AssertionOutcome contains the result of running one assertion of a given
// story.
type AssertionOutcome struct {
	Equal bool
	Got   *table.Table
	Want  *table.Table
}

// Story contains the available graphs and the collection of assertions to
// validate.
type Story struct {
	// Name of the story.
	Name string

	// Sources contains the list of graphs used in the story.
	Sources []*Graph

	// Assertions that need to be validated against the provided sources.
	Assertions []*Assertion
}

// Marshal serializes the story into a JSON readable string.
func (s *Story) Marshal() (string, error) {
	b, err := json.MarshalIndent(s, "", "  ")
	if err != nil {
		return "", err
	}
	return string(b), nil
}

// Unmarshal rebuilds a story from a JSON readable string.
func (s *Story) Unmarshal(ss string) error {
	return json.Unmarshal([]byte(ss), s)
}

// inferCell builds a Cell out of the provided string.
func inferCell(s string) *table.Cell {
	if n, err := node.Parse(s); err == nil {
		return &table.Cell{N: n}
	}
	if p, err := predicate.Parse(s); err == nil {
		return &table.Cell{P: p}
	}
	if l, err := literal.DefaultBuilder().Parse(s); err == nil {
		return &table.Cell{L: l}
	}
	t, err := time.Parse(time.RFC3339Nano, s)
	if err == nil {
		return &table.Cell{T: &t}
	}
	return &table.Cell{S: table.CellString(s)}
}

// OutputTable returns the expected result table for the must result table
// provided by the story.
func (a *Assertion) OutputTable(bo []string) (*table.Table, error) {
	// Return the already computed output table.
	if a.table != nil {
		return a.table, nil
	}
	// Compute the output table.
	var (
		first  bool
		mBdngs map[string]bool
		data   []table.Row
		bs     []string
	)
	mBdngs, first = make(map[string]bool), true
	for _, row := range a.MustReturn {
		nr := table.Row{}
		for k, v := range row {
			_, ok := mBdngs[k]
			if first && !ok {
				bs = append(bs, k)
			}
			if !first && !ok {
				return nil, fmt.Errorf("unknow binding %q; available ones are %v", k, mBdngs)
			}
			mBdngs[k], nr[k] = true, inferCell(v)
		}
		data = append(data, nr)
		first = false
	}
	if first {
		// No data was provided. This will create the empty table with the right
		// bindings.
		bs = bo
	}
	// Build the table.
	if len(bo) != len(bs) {
		return nil, fmt.Errorf("incompatible bindings; got %v, want %v", bs, bo)
	}
	for _, b := range bo {
		if _, ok := mBdngs[b]; !first && !ok {
			return nil, fmt.Errorf("missing binding %q; want bining in %v", b, bo)
		}
	}
	t, err := table.New(bo)
	if err != nil {
		return nil, err
	}
	for _, r := range data {
		t.AddRow(r)
	}
	return t, nil
}
