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
	"strings"

	"golang.org/x/net/context"

	"github.com/google/badwolf/bql/grammar"
	"github.com/google/badwolf/bql/planner"
	"github.com/google/badwolf/bql/semantic"
	"github.com/google/badwolf/bql/table"
	"github.com/google/badwolf/storage"
	"github.com/google/badwolf/triple"
	"github.com/google/badwolf/triple/literal"
)

// getGraphFromStore returns a Graph. Will create it if it does not exist.
func getGraphFromStore(ctx context.Context, st storage.Store, id string) (storage.Graph, error) {
	g, err := st.Graph(ctx, id)
	if err == nil {
		return g, nil
	}
	return st.NewGraph(ctx, id)
}

// populateSources create all the graph required for the story and
// populates it with the provided data.
func (s *Story) populateSources(ctx context.Context, st storage.Store, b literal.Builder) error {
	for _, src := range s.Sources {
		g, err := getGraphFromStore(ctx, st, src.ID)
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
		if err := g.AddTriples(ctx, trps); err != nil {
			return err
		}
	}
	return nil
}

// cleanSources create all the graph required for the story and
// populates it with the provided data.
func (s *Story) cleanSources(ctx context.Context, st storage.Store) error {
	for _, src := range s.Sources {
		if err := st.DeleteGraph(ctx, src.ID); err != nil {
			return err
		}
	}
	return nil
}

// runAssertion runs the assertion and compares the outcome. Returns the outcome
// of comparing the obtained result table with the assertion table if there is
// no error during the assertion.
func (a *Assertion) runAssertion(ctx context.Context, st storage.Store, chanSize int) (bool, *table.Table, *table.Table, error) {
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
	pln, err := planner.New(ctx, st, stm, chanSize, nil)
	if err != nil {
		return errorizer(fmt.Errorf("Should have not failed to create a plan using memory.DefaultStorage for statement %v with error %v", stm, err))
	}
	tbl, err := pln.Execute(ctx)
	if err != nil {
		return errorizer(fmt.Errorf("planner.Execute: failed to execute assertion %q with error %v", a.Requires, err))
	}

	// Check the output.
	want, err := a.OutputTable(stm.OutputBindings())
	if err != nil {
		return errorizer(err)
	}
	// Cannot use reflect.DeepEqual, since projections only remove bindings from
	// the table but not the actual data. However, the serialized text version
	// of the tables will be equal regardless of the internal representation.
	return tbl.String() == want.String(), tbl, want, nil
}

// Run evaluates a story. Returns if the story is true or not. It will also
// return an error if something wrong happen along the way. It is worth
// mentioning that Run does not clear any data avaiable in the provided
// storage.
func (s *Story) Run(ctx context.Context, st storage.Store, b literal.Builder, chanSize int) (map[string]*AssertionOutcome, error) {
	// Populate the sources.
	if err := s.populateSources(ctx, st, b); err != nil {
		return nil, err
	}
	// Run assertions.
	m := make(map[string]*AssertionOutcome)
	for _, a := range s.Assertions {
		b, got, want, err := a.runAssertion(ctx, st, chanSize)
		if err != nil {
			return nil, err
		}
		aName := fmt.Sprintf("requires %s", strings.TrimSpace(a.Requires))
		m[aName] = &AssertionOutcome{
			Equal: b,
			Got:   got,
			Want:  want,
		}
	}
	// Clean the sources.
	if err := s.cleanSources(ctx, st); err != nil {
		return nil, err
	}
	return m, nil
}

// AssertionBattery contains the result of running a collection of stories.
type AssertionBattery struct {
	Entries []*AssertionBatteryEntry
}

//AssertionBatteryEntry contains teh result of running a story.
type AssertionBatteryEntry struct {
	Story   *Story
	Outcome map[string]*AssertionOutcome
	Err     error
}

// RunStories runs a the provided stories and returns the outcome of each of
// them.
func RunStories(ctx context.Context, st storage.Store, b literal.Builder, stories []*Story, chanSize int) *AssertionBattery {
	results := &AssertionBattery{}
	for _, s := range stories {
		o, err := s.Run(ctx, st, b, chanSize)
		results.Entries = append(results.Entries, &AssertionBatteryEntry{
			Story:   s,
			Outcome: o,
			Err:     err,
		})
	}
	return results
}
