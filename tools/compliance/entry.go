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
// containing a BQL, the execution status, and the expeted result table.
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

	// Facts contains the parseable tribles which define the graph.
	Facts []string
}

// Assertion contains a BQL, the expecte status of the BQL query execution,
// and the returned results table.
type Assertion struct {
	// Name of the assertion.
	Name string

	// Statement contains the BQL query to assert.
	Statement string

	// WillFail indicates if the query should fail with and error.
	WillFail bool

	// MustReturn contains the table  containing the expected results provided
	// by the BQL statemnet execution.
	MustReturn []map[string]string

	// The equivalent table representation of the MustReturn inforamtion.
	table *table.Table
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
	if err := json.Unmarshal([]byte(ss), s); err != nil {
		return err
	}
	return nil
}

// inferCell build a Cell out of the provided string.
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
	return &table.Cell{S: s}
}

// OutputTable returns the expected result table for the must result table
// provided by the story.
func (a *Assertion) OutputTable() (*table.Table, error) {
	if a.table != nil {
		return a.table, nil
	}

	var (
		first  bool
		mBdngs map[string]bool
		data   []table.Row
	)
	mBdngs = make(map[string]bool)
	for _, row := range a.MustReturn {
		nr := table.Row{}
		for k, v := range row {
			if _, ok := mBdngs[k]; first && !ok {
				return nil, fmt.Errorf("unknow binding %q; available ones are %v", k, mBdngs)
			}
			mBdngs[k], nr[k] = true, inferCell(v)
		}
		data = append(data, nr)
		first = true
	}
	// Build the table.
	var bs []string
	for k := range mBdngs {
		bs = append(bs, k)
	}
	t, err := table.New(bs)
	if err != nil {
		return nil, err
	}
	for _, r := range data {
		t.AddRow(r)
	}
	return t, nil
}
