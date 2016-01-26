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

package compliance

import (
	"fmt"
	"reflect"
	"strings"

	"github.com/google/badwolf/bql/grammar"
	"github.com/google/badwolf/bql/planner"
	"github.com/google/badwolf/bql/semantic"
	"github.com/google/badwolf/bql/table"
	"github.com/google/badwolf/storage"
	"github.com/google/badwolf/triple"
	"github.com/google/badwolf/triple/literal"
)

// getGraphFromStore returns a Graph. Will create it if it does not exist.
func getGraphFromStore(st storage.Store, id string) (storage.Graph, error) {
	g, err := st.Graph(id)
	if err == nil {
		return g, nil
	}
	return st.NewGraph(id)
}

// populateSources create all the graph required for the story and
// populates it with the provided data.
func (s *Story) populateSources(st storage.Store, b literal.Builder) error {
	for _, src := range s.Sources {
		g, err := getGraphFromStore(st, src.ID)
		if err != nil {
			return err
		}
		var trps []*triple.Triple
		for _, trp := range src.Facts {
			t, err := triple.Parse(trp, b)
			if err != nil {
				return err
			}
			trps = append(trps, t)
		}
		if err := g.AddTriples(trps); err != nil {
			return err
		}
	}
	return nil
}

// runAssertion runs the assertion and compares the outcome. Returns the outcome
// of comparing the obtained result table with the assertion table if there is
// no error during the assertion.
func (a *Assertion) runAssertion(st storage.Store) (bool, *table.Table, *table.Table, error) {
	errorizer := func(e error) (bool, *table.Table, *table.Table, error) {
		if a.WillFail && e != nil {
			return true, nil, nil, nil
		}
		return false, nil, nil, e
	}

	// Run the query.
	p, err := grammar.NewParser(grammar.SemanticBQL())
	if err != nil {
		return errorizer(fmt.Errorf("Failed to initilize a valid BQL parser"))
	}
	stm := &semantic.Statement{}
	if err := p.Parse(grammar.NewLLk(a.Statement, 1), stm); err != nil {
		return errorizer(fmt.Errorf("Failed to parse BQL statement with error %v", err))
	}
	pln, err := planner.New(st, stm)
	if err != nil {
		return errorizer(fmt.Errorf("Should have not failed to create a plan using memory.DefaultStorage for statement %v with error %v", stm, err))
	}
	tbl, err := pln.Excecute()
	if err != nil {
		return errorizer(fmt.Errorf("planner.Execute: failed to execute insert plan with error %v", err))
	}

	// Check the output.
	want, err := a.OutputTable()
	if err != nil {
		return errorizer(err)
	}
	return reflect.DeepEqual(tbl, want), tbl, want, nil
}

// Run evaluates a story. Returns if the story is true or not. It will also
// return an error if something wrong happen along the way. It is worth
// mentioning that Run does not clear any data avaiable in the provided
// storage.
func (s *Story) Run(st storage.Store, b literal.Builder) (map[string]*AssertionOutcome, error) {
	// Populate the sources.
	if err := s.populateSources(st, b); err != nil {
		return nil, err
	}
	// Run assertions.
	m := make(map[string]*AssertionOutcome)
	for _, a := range s.Assertions {
		b, got, want, err := a.runAssertion(st)
		if err != nil {
			return nil, err
		}
		aName := fmt.Sprintf("%s ==> %s", strings.TrimSpace(s.Name), strings.TrimSpace(a.Name))
		m[aName] = &AssertionOutcome{
			Equal: b,
			Got:   got,
			Want:  want,
		}
	}
	return m, nil
}
