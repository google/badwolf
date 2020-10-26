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

package semantic

import (
	"fmt"
	"reflect"
	"testing"
	"time"

	"github.com/google/badwolf/bql/lexer"
	"github.com/google/badwolf/bql/planner/filter"
	"github.com/google/badwolf/bql/table"
	"github.com/google/badwolf/storage"
	"github.com/google/badwolf/triple"
	"github.com/google/badwolf/triple/literal"
	"github.com/google/badwolf/triple/node"
	"github.com/google/badwolf/triple/predicate"
)

func TestDataAccumulatorHook(t *testing.T) {
	st := &Statement{}
	ces := []ConsumedElement{
		NewConsumedSymbol("FOO"),
		NewConsumedToken(&lexer.Token{
			Type: lexer.ItemNode,
			Text: "/_<s>",
		}),
		NewConsumedSymbol("FOO"),
		NewConsumedToken(&lexer.Token{
			Type: lexer.ItemPredicate,
			Text: `"p"@[]`,
		}),
		NewConsumedSymbol("FOO"),
		NewConsumedToken(&lexer.Token{
			Type: lexer.ItemNode,
			Text: "/_<o>",
		}),
		NewConsumedSymbol("FOO"),
		NewConsumedToken(&lexer.Token{
			Type: lexer.ItemNode,
			Text: "/_<s>",
		}),
		NewConsumedSymbol("FOO"),
		NewConsumedToken(&lexer.Token{
			Type: lexer.ItemPredicate,
			Text: `"p"@[]`,
		}),
		NewConsumedSymbol("FOO"),
		NewConsumedToken(&lexer.Token{
			Type: lexer.ItemNode,
			Text: "/_<o>",
		}),
	}
	var (
		hook ElementHook
		err  error
	)
	hook = dataAccumulator(literal.DefaultBuilder())
	for _, ce := range ces {
		hook, err = hook(st, ce)
		if err != nil {
			t.Errorf("semantic.DataAccumulator hook should have never failed for %v with error %v", ce, err)
		}
	}
	data := st.Data()
	if len(data) != 2 {
		t.Errorf("semantic.DataAccumulator hook should have produced 2 triples; instead produced %v", st.Data())
	}
	for _, trpl := range data {
		if got, want := trpl.Subject().String(), "/_<s>"; got != want {
			t.Errorf("semantic.DataAccumulator hook failed to parse subject correctly; got %v, want %v", got, want)
		}
		if got, want := trpl.Predicate().String(), `"p"@[]`; got != want {
			t.Errorf("semantic.DataAccumulator hook failed to parse prdicate correctly; got %v, want %v", got, want)
		}
		if got, want := trpl.Object().String(), "/_<o>"; got != want {
			t.Errorf("semantic.DataAccumulator hook failed to parse object correctly; got %v, want %v", got, want)
		}
	}
}

func TestGraphAccumulatorElementHooks(t *testing.T) {
	st := &Statement{}
	ces := []ConsumedElement{
		NewConsumedSymbol("FOO"),
		NewConsumedToken(&lexer.Token{
			Type: lexer.ItemBinding,
			Text: "?foo",
		}),
		NewConsumedToken(&lexer.Token{
			Type: lexer.ItemComma,
			Text: ",",
		}),
		NewConsumedSymbol("FOO"),
		NewConsumedToken(&lexer.Token{
			Type: lexer.ItemBinding,
			Text: "?bar",
		}),
	}
	var (
		hook ElementHook
		data []string
		err  error
	)
	hook = graphAccumulator()
	for _, ce := range ces {
		hook, err = hook(st, ce)
		if err != nil {
			t.Errorf("semantic.GraphAccumulator hook should have never failed for %v with error %v", ce, err)
		}
	}
	data = st.GraphNames()
	if len(data) != 2 {
		t.Errorf("semantic.GraphAccumulator hook should have produced 2 graph bindings; instead produced %v", st.Graphs())
	}
	for _, g := range data {
		if g != "?foo" && g != "?bar" {
			t.Errorf("semantic.GraphAccumulator hook failed to provied either ?foo or ?bar; got %v instead", g)
		}
	}

	hook = inputGraphAccumulator()
	for _, ce := range ces {
		hook, err = hook(st, ce)
		if err != nil {
			t.Errorf("semantic.InputGraphAccumulator hook should have never failed for %v with error %v", ce, err)
		}
	}
	data = st.InputGraphNames()
	if len(data) != 2 {
		t.Errorf("semantic.InputGraphAccumulator hook should have produced 2 graph bindings; instead produced %v", st.Graphs())
	}
	for _, g := range data {
		if g != "?foo" && g != "?bar" {
			t.Errorf("semantic.InputGraphAccumulator hook failed to provied either ?foo or ?bar; got %v instead", g)
		}
	}

	hook = outputGraphAccumulator()
	for _, ce := range ces {
		hook, err = hook(st, ce)
		if err != nil {
			t.Errorf("semantic.OutputGraphAccumulator hook should have never failed for %v with error %v", ce, err)
		}
	}
	data = st.OutputGraphNames()
	if len(data) != 2 {
		t.Errorf("semantic.OutputGraphAccumulator hook should have produced 2 graph bindings; instead produced %v", st.Graphs())
	}
	for _, g := range data {
		if g != "?foo" && g != "?bar" {
			t.Errorf("semantic.OutputGraphAccumulator hook failed to provied either ?foo or ?bar; got %v instead", g)
		}
	}
}

func TestTypeBindingClauseHook(t *testing.T) {
	f := TypeBindingClauseHook(Insert)
	st := &Statement{}
	f(st, Symbol("FOO"))
	if got, want := st.Type(), Insert; got != want {
		t.Errorf("semantic.TypeBindingHook failed to set the right type; got %s, want %s", got, want)
	}
}

func TestWhereInitClauseHook(t *testing.T) {
	f := whereInitWorkingClause()
	st := &Statement{}
	f(st, Symbol("FOO"))
	if wc := st.WorkingClause(); wc == nil || !wc.IsEmpty() {
		t.Errorf(`semantic.Statement.WorkingClause() = %q for statement "%v" after call to semantic.WhereInitWorkingClause; want empty GraphClause`, wc, st)
	}
	if wf := st.WorkingFilter(); wf == nil || !wf.IsEmpty() {
		t.Errorf(`semantic.Statement.WorkingFilter() = %q for statement "%v" after call to semantic.WhereInitWorkingClause; want empty FilterClause`, wf, st)
	}
}

func TestWhereFilterClauseHook(t *testing.T) {
	st := &Statement{}
	f := whereFilterClause()
	st.ResetWorkingFilterClause()

	testTable := []struct {
		id   string
		ces  []ConsumedElement
		want *FilterClause
	}{
		{
			id: "FILTER latest(?p)",
			ces: []ConsumedElement{
				NewConsumedToken(&lexer.Token{
					Type: lexer.ItemFilter,
					Text: "FILTER",
				}),
				NewConsumedToken(&lexer.Token{
					Type: lexer.ItemFilterFunction,
					Text: "latest",
				}),
				NewConsumedToken(&lexer.Token{
					Type: lexer.ItemLPar,
					Text: "(",
				}),
				NewConsumedToken(&lexer.Token{
					Type: lexer.ItemBinding,
					Text: "?p",
				}),
				NewConsumedToken(&lexer.Token{
					Type: lexer.ItemRPar,
					Text: ")",
				}),
				NewConsumedSymbol("FOO"),
			},
			want: &FilterClause{
				Operation: filter.Latest,
				Binding:   "?p",
			},
		},
		{
			id: "FILTER latest(?o)",
			ces: []ConsumedElement{
				NewConsumedToken(&lexer.Token{
					Type: lexer.ItemFilter,
					Text: "FILTER",
				}),
				NewConsumedToken(&lexer.Token{
					Type: lexer.ItemFilterFunction,
					Text: "latest",
				}),
				NewConsumedToken(&lexer.Token{
					Type: lexer.ItemLPar,
					Text: "(",
				}),
				NewConsumedToken(&lexer.Token{
					Type: lexer.ItemBinding,
					Text: "?o",
				}),
				NewConsumedToken(&lexer.Token{
					Type: lexer.ItemRPar,
					Text: ")",
				}),
				NewConsumedSymbol("FOO"),
			},
			want: &FilterClause{
				Operation: filter.Latest,
				Binding:   "?o",
			},
		},
	}
	for i, entry := range testTable {
		t.Run(entry.id, func(t *testing.T) {
			for _, ce := range entry.ces {
				if _, err := f(st, ce); err != nil {
					t.Errorf("%q: semantic.WhereFilterClauseHook(%s) = _, %v; want _, nil", entry.id, ce, err)
				}
			}
			if got, want := st.FilterClauses()[i], entry.want; !reflect.DeepEqual(got, want) {
				t.Errorf("%q: semantic.Statement.FilterClauses()[%d] = %s; want %s", entry.id, i, got, want)
			}
		})
	}

	t.Run(fmt.Sprintf("final length filters list expects %d", len(testTable)), func(t *testing.T) {
		if got, want := len(st.FilterClauses()), len(testTable); got != want {
			t.Errorf("len(semantic.Statement.FilterClauses()) = %d after consuming %d valid FILTER clauses; want %d", got, want, want)
		}
	})
}

func TestWhereFilterClauseHookError(t *testing.T) {
	st := &Statement{}
	f := whereFilterClause()
	st.ResetWorkingFilterClause()

	testTable := []struct {
		id           string
		ces          []ConsumedElement
		ceIndexError int
	}{
		{
			id: "FILTER notSupportedFilterFunction(?p)",
			ces: []ConsumedElement{
				NewConsumedToken(&lexer.Token{
					Type: lexer.ItemFilter,
					Text: "FILTER",
				}),
				NewConsumedToken(&lexer.Token{
					Type: lexer.ItemFilterFunction,
					Text: "notSupportedFilterFunction",
				}),
				NewConsumedToken(&lexer.Token{
					Type: lexer.ItemLPar,
					Text: "(",
				}),
				NewConsumedToken(&lexer.Token{
					Type: lexer.ItemBinding,
					Text: "?p",
				}),
				NewConsumedToken(&lexer.Token{
					Type: lexer.ItemRPar,
					Text: ")",
				}),
			},
			ceIndexError: 1,
		},
		{
			id: "FILTER latest latest(?p)",
			ces: []ConsumedElement{
				NewConsumedToken(&lexer.Token{
					Type: lexer.ItemFilter,
					Text: "FILTER",
				}),
				NewConsumedToken(&lexer.Token{
					Type: lexer.ItemFilterFunction,
					Text: "latest",
				}),
				NewConsumedToken(&lexer.Token{
					Type: lexer.ItemFilterFunction,
					Text: "latest",
				}),
				NewConsumedToken(&lexer.Token{
					Type: lexer.ItemLPar,
					Text: "(",
				}),
				NewConsumedToken(&lexer.Token{
					Type: lexer.ItemBinding,
					Text: "?p",
				}),
				NewConsumedToken(&lexer.Token{
					Type: lexer.ItemRPar,
					Text: ")",
				}),
			},
			ceIndexError: 2,
		},
		{
			id: "FILTER latest(?p, ?o)",
			ces: []ConsumedElement{
				NewConsumedToken(&lexer.Token{
					Type: lexer.ItemFilter,
					Text: "FILTER",
				}),
				NewConsumedToken(&lexer.Token{
					Type: lexer.ItemFilterFunction,
					Text: "latest",
				}),
				NewConsumedToken(&lexer.Token{
					Type: lexer.ItemLPar,
					Text: "(",
				}),
				NewConsumedToken(&lexer.Token{
					Type: lexer.ItemBinding,
					Text: "?p",
				}),
				NewConsumedToken(&lexer.Token{
					Type: lexer.ItemComma,
					Text: ",",
				}),
				NewConsumedToken(&lexer.Token{
					Type: lexer.ItemBinding,
					Text: "?o",
				}),
				NewConsumedToken(&lexer.Token{
					Type: lexer.ItemRPar,
					Text: ")",
				}),
			},
			ceIndexError: 5,
		},
		{
			id: `FILTER latest(?p, "37"^^type:int64)`,
			ces: []ConsumedElement{
				NewConsumedToken(&lexer.Token{
					Type: lexer.ItemFilter,
					Text: "FILTER",
				}),
				NewConsumedToken(&lexer.Token{
					Type: lexer.ItemFilterFunction,
					Text: "latest",
				}),
				NewConsumedToken(&lexer.Token{
					Type: lexer.ItemLPar,
					Text: "(",
				}),
				NewConsumedToken(&lexer.Token{
					Type: lexer.ItemBinding,
					Text: "?p",
				}),
				NewConsumedToken(&lexer.Token{
					Type: lexer.ItemComma,
					Text: ",",
				}),
				NewConsumedToken(&lexer.Token{
					Type: lexer.ItemLiteral,
					Text: `"37"^^type:int64)`,
				}),
				NewConsumedToken(&lexer.Token{
					Type: lexer.ItemRPar,
					Text: ")",
				}),
			},
			ceIndexError: 6,
		},
	}
	for _, entry := range testTable {
		t.Run(entry.id, func(t *testing.T) {
			var err error
			for i, ce := range entry.ces {
				if _, err = f(st, ce); err != nil {
					if i != entry.ceIndexError {
						t.Errorf("%q: semantic.WhereFilterClauseHook(%v) = _, %v; want _, nil since the expected error should be when consuming %v", entry.id, ce, err, entry.ces[entry.ceIndexError])
					}
					break
				}
			}
			if err == nil {
				t.Errorf("%q: semantic.WhereFilterClauseHook(%v) = _, nil; want _, error", entry.id, entry.ces[entry.ceIndexError])
			}
			st.ResetWorkingFilterClause()
		})
	}

	t.Run("final length filters list expects 0", func(t *testing.T) {
		if got, want := len(st.FilterClauses()), 0; got != want {
			t.Errorf("len(semantic.Statement.FilterClauses()) = %d; want %d", got, want)
		}
	})
}

func TestWhereWorkingClauseHook(t *testing.T) {
	f := whereNextWorkingClause()
	st := &Statement{}
	st.ResetWorkingGraphClause()
	wcs := st.WorkingClause()
	wcs.SBinding = "?a"
	f(st, Symbol("FOO"))
	wcs = st.WorkingClause()
	wcs.SBinding = "?b"
	f(st, Symbol("FOO"))

	if got, want := len(st.GraphPatternClauses()), 2; got != want {
		t.Errorf("semantic.whereNextWorkingClause should have returned two clauses for statement %v; got %d, want %d", st, got, want)
	}
}

type testClauseTable struct {
	valid bool
	id    string
	ces   []ConsumedElement
	want  *GraphClause
}

func runTabulatedClauseHookTest(t *testing.T, testName string, f ElementHook, table []testClauseTable) {
	st := &Statement{}
	st.ResetWorkingGraphClause()
	failed := false
	for _, entry := range table {
		for _, ce := range entry.ces {
			if _, err := f(st, ce); err != nil {
				if entry.valid {
					t.Errorf("%s case %q should have never failed with error: %v", testName, entry.id, err)
				} else {
					failed = true
				}
			}
		}
		if entry.valid {
			if got, want := st.WorkingClause(), entry.want; !reflect.DeepEqual(got, want) {
				t.Errorf("%s case %q should have populated all required fields; got %+v, want %+v", testName, entry.id, got, want)
			}
		} else {
			if !failed {
				t.Errorf("%s failed to reject invalid case %q", testName, entry.id)
			}
		}
		st.ResetWorkingGraphClause()
	}
}

func TestWhereSubjectClauseHook(t *testing.T) {
	st := &Statement{}
	f := whereSubjectClause()
	st.ResetWorkingGraphClause()
	n, err := node.Parse("/_<foo>")
	if err != nil {
		t.Fatalf("node.Parse failed with error %v", err)
	}
	runTabulatedClauseHookTest(t, "semantic.whereSubjectClause", f, []testClauseTable{
		{
			valid: true,
			id:    "node_example",
			ces: []ConsumedElement{
				NewConsumedSymbol("FOO"),
				NewConsumedToken(&lexer.Token{
					Type: lexer.ItemNode,
					Text: "/_<foo>",
				}),
				NewConsumedSymbol("FOO"),
				NewConsumedToken(&lexer.Token{
					Type: lexer.ItemAs,
					Text: "as",
				}),
				NewConsumedSymbol("FOO"),
				NewConsumedToken(&lexer.Token{
					Type: lexer.ItemBinding,
					Text: "?bar",
				}),
				NewConsumedToken(&lexer.Token{
					Type: lexer.ItemType,
					Text: "type",
				}),
				NewConsumedSymbol("FOO"),
				NewConsumedToken(&lexer.Token{
					Type: lexer.ItemBinding,
					Text: "?bar2",
				}),
				NewConsumedSymbol("FOO"),
				NewConsumedToken(&lexer.Token{
					Type: lexer.ItemID,
					Text: "id",
				}),
				NewConsumedSymbol("FOO"),
				NewConsumedToken(&lexer.Token{
					Type: lexer.ItemBinding,
					Text: "?bar3",
				}),
				NewConsumedSymbol("FOO"),
			},
			want: &GraphClause{
				S:          n,
				SAlias:     "?bar",
				STypeAlias: "?bar2",
				SIDAlias:   "?bar3",
			},
		},
		{
			valid: true,
			id:    "binding_example",
			ces: []ConsumedElement{
				NewConsumedSymbol("FOO"),
				NewConsumedToken(&lexer.Token{
					Type: lexer.ItemBinding,
					Text: "?foo",
				}),
				NewConsumedSymbol("FOO"),
				NewConsumedToken(&lexer.Token{
					Type: lexer.ItemAs,
					Text: "as",
				}),
				NewConsumedSymbol("FOO"),
				NewConsumedToken(&lexer.Token{
					Type: lexer.ItemBinding,
					Text: "?bar",
				}),
				NewConsumedToken(&lexer.Token{
					Type: lexer.ItemType,
					Text: "type",
				}),
				NewConsumedSymbol("FOO"),
				NewConsumedToken(&lexer.Token{
					Type: lexer.ItemBinding,
					Text: "?bar2",
				}),
				NewConsumedSymbol("FOO"),
				NewConsumedToken(&lexer.Token{
					Type: lexer.ItemID,
					Text: "id",
				}),
				NewConsumedSymbol("FOO"),
				NewConsumedToken(&lexer.Token{
					Type: lexer.ItemBinding,
					Text: "?bar3",
				}),
				NewConsumedSymbol("FOO"),
			},
			want: &GraphClause{
				SBinding:   "?foo",
				SAlias:     "?bar",
				STypeAlias: "?bar2",
				SIDAlias:   "?bar3",
			},
		},
	})
}

func TestWherePredicateClauseHook(t *testing.T) {
	st := &Statement{}
	f := wherePredicateClause()
	st.ResetWorkingGraphClause()
	p, err := predicate.Parse(`"foo"@[2015-07-19T13:12:04.669618843-07:00]`)
	if err != nil {
		t.Fatalf("predicate.Parse failed with error %v", err)
	}
	tlb, err := time.Parse(time.RFC3339Nano, `2015-07-19T13:12:04.669618843-07:00`)
	if err != nil {
		t.Fatalf("time.Parse failed to parse valid lower time bound with error %v", err)
	}
	tub, err := time.Parse(time.RFC3339Nano, `2016-07-19T13:12:04.669618843-07:00`)
	if err != nil {
		t.Fatalf("time.Parse failed to parse valid upper time bound with error %v", err)
	}
	runTabulatedClauseHookTest(t, "semantic.wherePredicateClause", f, []testClauseTable{
		{
			valid: true,
			id:    "valid predicate",
			ces: []ConsumedElement{
				NewConsumedSymbol("FOO"),
				NewConsumedToken(&lexer.Token{
					Type: lexer.ItemPredicate,
					Text: `"foo"@[2015-07-19T13:12:04.669618843-07:00]`,
				}),
				NewConsumedSymbol("FOO"),
				NewConsumedToken(&lexer.Token{
					Type: lexer.ItemAs,
					Text: "as",
				}),
				NewConsumedSymbol("FOO"),
				NewConsumedToken(&lexer.Token{
					Type: lexer.ItemBinding,
					Text: "?bar",
				}),
				NewConsumedToken(&lexer.Token{
					Type: lexer.ItemID,
					Text: "id",
				}),
				NewConsumedSymbol("FOO"),
				NewConsumedToken(&lexer.Token{
					Type: lexer.ItemBinding,
					Text: "?bar2",
				}),
				NewConsumedSymbol("FOO"),
				NewConsumedToken(&lexer.Token{
					Type: lexer.ItemAt,
					Text: "at",
				}),
				NewConsumedSymbol("FOO"),
				NewConsumedToken(&lexer.Token{
					Type: lexer.ItemBinding,
					Text: "?bar3",
				}),
				NewConsumedSymbol("FOO"),
			},
			want: &GraphClause{
				P:            p,
				PAlias:       "?bar",
				PIDAlias:     "?bar2",
				PAnchorAlias: "?bar3",
				PTemporal:    true,
			},
		},
		{
			valid: true,
			id:    "valid predicate with binding",
			ces: []ConsumedElement{
				NewConsumedSymbol("FOO"),
				NewConsumedToken(&lexer.Token{
					Type: lexer.ItemPredicate,
					Text: `"foo"@[?foo]`,
				}),
				NewConsumedSymbol("FOO"),
				NewConsumedToken(&lexer.Token{
					Type: lexer.ItemAs,
					Text: "as",
				}),
				NewConsumedSymbol("FOO"),
				NewConsumedToken(&lexer.Token{
					Type: lexer.ItemBinding,
					Text: "?bar",
				}),
				NewConsumedToken(&lexer.Token{
					Type: lexer.ItemID,
					Text: "id",
				}),
				NewConsumedSymbol("FOO"),
				NewConsumedToken(&lexer.Token{
					Type: lexer.ItemBinding,
					Text: "?bar2",
				}),
				NewConsumedSymbol("FOO"),
				NewConsumedToken(&lexer.Token{
					Type: lexer.ItemAt,
					Text: "at",
				}),
				NewConsumedSymbol("FOO"),
				NewConsumedToken(&lexer.Token{
					Type: lexer.ItemBinding,
					Text: "?bar3",
				}),
				NewConsumedSymbol("FOO"),
			},
			want: &GraphClause{
				PID:            "foo",
				PAnchorBinding: "?foo",
				PAlias:         "?bar",
				PIDAlias:       "?bar2",
				PAnchorAlias:   "?bar3",
				PTemporal:      true,
			},
		},
		{
			valid: true,
			id:    "valid bound with bindings",
			ces: []ConsumedElement{
				NewConsumedSymbol("FOO"),
				NewConsumedToken(&lexer.Token{
					Type: lexer.ItemPredicateBound,
					Text: `"foo"@[?fooLower,?fooUpper]`,
				}),
				NewConsumedSymbol("FOO"),
				NewConsumedToken(&lexer.Token{
					Type: lexer.ItemAs,
					Text: "as",
				}),
				NewConsumedSymbol("FOO"),
				NewConsumedToken(&lexer.Token{
					Type: lexer.ItemBinding,
					Text: "?bar",
				}),
				NewConsumedToken(&lexer.Token{
					Type: lexer.ItemID,
					Text: "id",
				}),
				NewConsumedSymbol("FOO"),
				NewConsumedToken(&lexer.Token{
					Type: lexer.ItemBinding,
					Text: "?bar2",
				}),
				NewConsumedSymbol("FOO"),
				NewConsumedToken(&lexer.Token{
					Type: lexer.ItemAt,
					Text: "at",
				}),
				NewConsumedSymbol("FOO"),
				NewConsumedToken(&lexer.Token{
					Type: lexer.ItemBinding,
					Text: "?bar3",
				}),
				NewConsumedSymbol("FOO"),
			},
			want: &GraphClause{
				PID:              "foo",
				PLowerBoundAlias: "?fooLower",
				PUpperBoundAlias: "?fooUpper",
				PAlias:           "?bar",
				PIDAlias:         "?bar2",
				PAnchorAlias:     "?bar3",
				PTemporal:        true,
			},
		},
		{
			valid: true,
			id:    "valid bound with dates",
			ces: []ConsumedElement{
				NewConsumedSymbol("FOO"),
				NewConsumedToken(&lexer.Token{
					Type: lexer.ItemPredicateBound,
					Text: `"foo"@[2015-07-19T13:12:04.669618843-07:00,2016-07-19T13:12:04.669618843-07:00]`,
				}),
				NewConsumedSymbol("FOO"),
				NewConsumedToken(&lexer.Token{
					Type: lexer.ItemAs,
					Text: "as",
				}),
				NewConsumedSymbol("FOO"),
				NewConsumedToken(&lexer.Token{
					Type: lexer.ItemBinding,
					Text: "?bar",
				}),
				NewConsumedToken(&lexer.Token{
					Type: lexer.ItemID,
					Text: "id",
				}),
				NewConsumedSymbol("FOO"),
				NewConsumedToken(&lexer.Token{
					Type: lexer.ItemBinding,
					Text: "?bar2",
				}),
				NewConsumedSymbol("FOO"),
				NewConsumedToken(&lexer.Token{
					Type: lexer.ItemAt,
					Text: "at",
				}),
				NewConsumedSymbol("FOO"),
				NewConsumedToken(&lexer.Token{
					Type: lexer.ItemBinding,
					Text: "?bar3",
				}),
				NewConsumedSymbol("FOO"),
			},
			want: &GraphClause{
				PID:          "foo",
				PLowerBound:  &tlb,
				PUpperBound:  &tub,
				PAlias:       "?bar",
				PIDAlias:     "?bar2",
				PAnchorAlias: "?bar3",
				PTemporal:    true,
			},
		},
		{
			valid: false,
			id:    "invalid bound with dates",
			ces: []ConsumedElement{
				NewConsumedSymbol("FOO"),
				NewConsumedToken(&lexer.Token{
					Type: lexer.ItemPredicateBound,
					Text: `"foo"@[2016-07-19T13:12:04.669618843-07:00,2015-07-19T13:12:04.669618843-07:00]`,
				}),
				NewConsumedSymbol("FOO"),
				NewConsumedToken(&lexer.Token{
					Type: lexer.ItemAs,
					Text: "as",
				}),
				NewConsumedSymbol("FOO"),
				NewConsumedToken(&lexer.Token{
					Type: lexer.ItemBinding,
					Text: "?bar",
				}),
				NewConsumedToken(&lexer.Token{
					Type: lexer.ItemID,
					Text: "id",
				}),
				NewConsumedSymbol("FOO"),
				NewConsumedToken(&lexer.Token{
					Type: lexer.ItemBinding,
					Text: "?bar2",
				}),
				NewConsumedSymbol("FOO"),
				NewConsumedToken(&lexer.Token{
					Type: lexer.ItemAt,
					Text: "at",
				}),
				NewConsumedSymbol("FOO"),
				NewConsumedToken(&lexer.Token{
					Type: lexer.ItemBinding,
					Text: "?bar3",
				}),
				NewConsumedSymbol("FOO"),
			},
			want: &GraphClause{},
		},
	})
}

func TestWhereObjectClauseHook(t *testing.T) {
	st := &Statement{}
	f := whereObjectClause()
	st.ResetWorkingGraphClause()
	node, err := node.Parse("/_<foo>")
	if err != nil {
		t.Fatalf("node.Parse failed with error %v", err)
	}
	n := triple.NewNodeObject(node)
	pred, err := predicate.Parse(`"foo"@[2015-07-19T13:12:04.669618843-07:00]`)
	if err != nil {
		t.Fatalf("predicate.Parse failed with error %v", err)
	}
	p := triple.NewPredicateObject(pred)
	tlb, err := time.Parse(time.RFC3339Nano, `2015-07-19T13:12:04.669618843-07:00`)
	if err != nil {
		t.Fatalf("time.Parse failed to parse valid lower time bound with error %v", err)
	}
	tub, err := time.Parse(time.RFC3339Nano, `2016-07-19T13:12:04.669618843-07:00`)
	if err != nil {
		t.Fatalf("time.Parse failed to parse valid upper time bound with error %v", err)
	}
	l, err := triple.ParseObject(`"1"^^type:int64`, literal.DefaultBuilder())
	if err != nil {
		t.Fatalf("literal.Parse should never fail to parse %s with error %v", `"1"^^type:int64`, err)
	}

	runTabulatedClauseHookTest(t, "semantic.whereObjectClause", f, []testClauseTable{
		{
			valid: true,
			id:    "node_example",
			ces: []ConsumedElement{
				NewConsumedSymbol("FOO"),
				NewConsumedToken(&lexer.Token{
					Type: lexer.ItemNode,
					Text: "/_<foo>",
				}),
				NewConsumedSymbol("FOO"),
				NewConsumedToken(&lexer.Token{
					Type: lexer.ItemAs,
					Text: "as",
				}),
				NewConsumedSymbol("FOO"),
				NewConsumedToken(&lexer.Token{
					Type: lexer.ItemBinding,
					Text: "?bar",
				}),
				NewConsumedToken(&lexer.Token{
					Type: lexer.ItemType,
					Text: "type",
				}),
				NewConsumedSymbol("FOO"),
				NewConsumedToken(&lexer.Token{
					Type: lexer.ItemBinding,
					Text: "?bar2",
				}),
				NewConsumedSymbol("FOO"),
				NewConsumedToken(&lexer.Token{
					Type: lexer.ItemID,
					Text: "id",
				}),
				NewConsumedSymbol("FOO"),
				NewConsumedToken(&lexer.Token{
					Type: lexer.ItemBinding,
					Text: "?bar3",
				}),
				NewConsumedSymbol("FOO"),
			},
			want: &GraphClause{
				O:          n,
				OAlias:     "?bar",
				OTypeAlias: "?bar2",
				OIDAlias:   "?bar3",
			},
		},
		{
			valid: true,
			id:    "binding_example",
			ces: []ConsumedElement{
				NewConsumedSymbol("FOO"),
				NewConsumedToken(&lexer.Token{
					Type: lexer.ItemBinding,
					Text: "?foo",
				}),
				NewConsumedSymbol("FOO"),
				NewConsumedToken(&lexer.Token{
					Type: lexer.ItemAs,
					Text: "as",
				}),
				NewConsumedSymbol("FOO"),
				NewConsumedToken(&lexer.Token{
					Type: lexer.ItemBinding,
					Text: "?bar",
				}),
				NewConsumedToken(&lexer.Token{
					Type: lexer.ItemType,
					Text: "type",
				}),
				NewConsumedSymbol("FOO"),
				NewConsumedToken(&lexer.Token{
					Type: lexer.ItemBinding,
					Text: "?bar2",
				}),
				NewConsumedSymbol("FOO"),
				NewConsumedToken(&lexer.Token{
					Type: lexer.ItemID,
					Text: "id",
				}),
				NewConsumedSymbol("FOO"),
				NewConsumedToken(&lexer.Token{
					Type: lexer.ItemBinding,
					Text: "?bar3",
				}),
				NewConsumedSymbol("FOO"),
			},
			want: &GraphClause{
				OBinding:   "?foo",
				OAlias:     "?bar",
				OTypeAlias: "?bar2",
				OIDAlias:   "?bar3",
			},
		},
		{
			valid: true,
			id:    "valid predicate",
			ces: []ConsumedElement{
				NewConsumedSymbol("FOO"),
				NewConsumedToken(&lexer.Token{
					Type: lexer.ItemPredicate,
					Text: `"foo"@[2015-07-19T13:12:04.669618843-07:00]`,
				}),
				NewConsumedSymbol("FOO"),
				NewConsumedToken(&lexer.Token{
					Type: lexer.ItemAs,
					Text: "as",
				}),
				NewConsumedSymbol("FOO"),
				NewConsumedToken(&lexer.Token{
					Type: lexer.ItemBinding,
					Text: "?bar",
				}),
				NewConsumedToken(&lexer.Token{
					Type: lexer.ItemID,
					Text: "id",
				}),
				NewConsumedSymbol("FOO"),
				NewConsumedToken(&lexer.Token{
					Type: lexer.ItemBinding,
					Text: "?bar2",
				}),
				NewConsumedSymbol("FOO"),
				NewConsumedToken(&lexer.Token{
					Type: lexer.ItemAt,
					Text: "at",
				}),
				NewConsumedSymbol("FOO"),
				NewConsumedToken(&lexer.Token{
					Type: lexer.ItemBinding,
					Text: "?bar3",
				}),
				NewConsumedSymbol("FOO"),
			},
			want: &GraphClause{
				O:            p,
				OAlias:       "?bar",
				OIDAlias:     "?bar2",
				OAnchorAlias: "?bar3",
				OTemporal:    true,
			},
		},
		{
			valid: true,
			id:    "valid predicate with binding",
			ces: []ConsumedElement{
				NewConsumedSymbol("FOO"),
				NewConsumedToken(&lexer.Token{
					Type: lexer.ItemPredicate,
					Text: `"foo"@[?foo]`,
				}),
				NewConsumedSymbol("FOO"),
				NewConsumedToken(&lexer.Token{
					Type: lexer.ItemAs,
					Text: "as",
				}),
				NewConsumedSymbol("FOO"),
				NewConsumedToken(&lexer.Token{
					Type: lexer.ItemBinding,
					Text: "?bar",
				}),
				NewConsumedToken(&lexer.Token{
					Type: lexer.ItemID,
					Text: "id",
				}),
				NewConsumedSymbol("FOO"),
				NewConsumedToken(&lexer.Token{
					Type: lexer.ItemBinding,
					Text: "?bar2",
				}),
				NewConsumedSymbol("FOO"),
				NewConsumedToken(&lexer.Token{
					Type: lexer.ItemAt,
					Text: "at",
				}),
				NewConsumedSymbol("FOO"),
				NewConsumedToken(&lexer.Token{
					Type: lexer.ItemBinding,
					Text: "?bar3",
				}),
				NewConsumedSymbol("FOO"),
			},
			want: &GraphClause{
				OID:            "foo",
				OAnchorBinding: "?foo",
				OAlias:         "?bar",
				OIDAlias:       "?bar2",
				OAnchorAlias:   "?bar3",
				OTemporal:      true,
			},
		},
		{
			valid: true,
			id:    "valid bound with bindings",
			ces: []ConsumedElement{
				NewConsumedSymbol("FOO"),
				NewConsumedToken(&lexer.Token{
					Type: lexer.ItemPredicateBound,
					Text: `"foo"@[?fooLower,?fooUpper]`,
				}),
				NewConsumedSymbol("FOO"),
				NewConsumedToken(&lexer.Token{
					Type: lexer.ItemAs,
					Text: "as",
				}),
				NewConsumedSymbol("FOO"),
				NewConsumedToken(&lexer.Token{
					Type: lexer.ItemBinding,
					Text: "?bar",
				}),
				NewConsumedToken(&lexer.Token{
					Type: lexer.ItemID,
					Text: "id",
				}),
				NewConsumedSymbol("FOO"),
				NewConsumedToken(&lexer.Token{
					Type: lexer.ItemBinding,
					Text: "?bar2",
				}),
				NewConsumedSymbol("FOO"),
				NewConsumedToken(&lexer.Token{
					Type: lexer.ItemAt,
					Text: "at",
				}),
				NewConsumedSymbol("FOO"),
				NewConsumedToken(&lexer.Token{
					Type: lexer.ItemBinding,
					Text: "?bar3",
				}),
				NewConsumedSymbol("FOO"),
			},
			want: &GraphClause{
				OID:              "foo",
				OLowerBoundAlias: "?fooLower",
				OUpperBoundAlias: "?fooUpper",
				OAlias:           "?bar",
				OIDAlias:         "?bar2",
				OAnchorAlias:     "?bar3",
				OTemporal:        true,
			},
		},
		{
			valid: true,
			id:    "valid bound with dates",
			ces: []ConsumedElement{
				NewConsumedSymbol("FOO"),
				NewConsumedToken(&lexer.Token{
					Type: lexer.ItemPredicateBound,
					Text: `"foo"@[2015-07-19T13:12:04.669618843-07:00,2016-07-19T13:12:04.669618843-07:00]`,
				}),
				NewConsumedSymbol("FOO"),
				NewConsumedToken(&lexer.Token{
					Type: lexer.ItemAs,
					Text: "as",
				}),
				NewConsumedSymbol("FOO"),
				NewConsumedToken(&lexer.Token{
					Type: lexer.ItemBinding,
					Text: "?bar",
				}),
				NewConsumedToken(&lexer.Token{
					Type: lexer.ItemID,
					Text: "id",
				}),
				NewConsumedSymbol("FOO"),
				NewConsumedToken(&lexer.Token{
					Type: lexer.ItemBinding,
					Text: "?bar2",
				}),
				NewConsumedSymbol("FOO"),
				NewConsumedToken(&lexer.Token{
					Type: lexer.ItemAt,
					Text: "at",
				}),
				NewConsumedSymbol("FOO"),
				NewConsumedToken(&lexer.Token{
					Type: lexer.ItemBinding,
					Text: "?bar3",
				}),
				NewConsumedSymbol("FOO"),
			},
			want: &GraphClause{
				OID:          "foo",
				OLowerBound:  &tlb,
				OUpperBound:  &tub,
				OAlias:       "?bar",
				OIDAlias:     "?bar2",
				OAnchorAlias: "?bar3",
				OTemporal:    true,
			},
		},
		{
			valid: false,
			id:    "invalid bound with dates",
			ces: []ConsumedElement{
				NewConsumedSymbol("FOO"),
				NewConsumedToken(&lexer.Token{
					Type: lexer.ItemPredicateBound,
					Text: `"foo"@[2016-07-19T13:12:04.669618843-07:00,2015-07-19T13:12:04.669618843-07:00]`,
				}),
				NewConsumedSymbol("FOO"),
				NewConsumedToken(&lexer.Token{
					Type: lexer.ItemAs,
					Text: "as",
				}),
				NewConsumedSymbol("FOO"),
				NewConsumedToken(&lexer.Token{
					Type: lexer.ItemBinding,
					Text: "?bar",
				}),
				NewConsumedToken(&lexer.Token{
					Type: lexer.ItemID,
					Text: "id",
				}),
				NewConsumedSymbol("FOO"),
				NewConsumedToken(&lexer.Token{
					Type: lexer.ItemBinding,
					Text: "?bar2",
				}),
				NewConsumedSymbol("FOO"),
				NewConsumedToken(&lexer.Token{
					Type: lexer.ItemAt,
					Text: "at",
				}),
				NewConsumedSymbol("FOO"),
				NewConsumedToken(&lexer.Token{
					Type: lexer.ItemBinding,
					Text: "?bar3",
				}),
				NewConsumedSymbol("FOO"),
			},
			want: &GraphClause{},
		},
		{
			valid: true,
			id:    "literal with alias",
			ces: []ConsumedElement{
				NewConsumedSymbol("FOO"),
				NewConsumedToken(&lexer.Token{
					Type: lexer.ItemLiteral,
					Text: `"1"^^type:int64`,
				}),
				NewConsumedSymbol("FOO"),
				NewConsumedToken(&lexer.Token{
					Type: lexer.ItemAs,
					Text: "as",
				}),
				NewConsumedSymbol("FOO"),
				NewConsumedToken(&lexer.Token{
					Type: lexer.ItemBinding,
					Text: "?bar",
				}),
				NewConsumedSymbol("FOO"),
			},
			want: &GraphClause{
				O:      l,
				OAlias: "?bar"},
		},
	})
}

type testProjectionTable struct {
	valid bool
	id    string
	ces   []ConsumedElement
	want  *Projection
}

func runTabulatedProjectionHookTest(t *testing.T, testName string, f ElementHook, table []testProjectionTable) {
	var st *Statement
	failed := false
	for _, entry := range table {
		st = &Statement{}
		for _, ce := range entry.ces {
			if _, err := f(st, ce); err != nil {
				if entry.valid {
					t.Errorf("%s case %q should have never failed with error: %v", testName, entry.id, err)
				} else {
					failed = true
				}
			}
		}
		if entry.valid {
			p := st.WorkingProjection()
			if p.IsEmpty() && len(st.Projections()) > 0 {
				p = st.Projections()[0]
			}
			if got, want := p, entry.want; !reflect.DeepEqual(got, want) {
				t.Errorf("%s case %q should have populated all required fields; got %+v, want %+v", testName, entry.id, got, want)
			}
		} else {
			if !failed {
				t.Errorf("%s failed to reject invalid case %q", testName, entry.id)
			}
		}
	}
}

func TestVarAccumulatorHook(t *testing.T) {
	st := &Statement{}
	f := varAccumulator()
	st.ResetProjection()

	runTabulatedProjectionHookTest(t, "semantic.varAccumulator", f, []testProjectionTable{
		{
			valid: true,
			id:    "simple var",
			ces: []ConsumedElement{
				NewConsumedSymbol("FOO"),
				NewConsumedToken(&lexer.Token{
					Type: lexer.ItemBinding,
					Text: "?foo",
				}),
				NewConsumedSymbol("FOO"),
			},
			want: &Projection{
				Binding: "?foo",
			},
		},
		{
			valid: true,
			id:    "simple var with alias",
			ces: []ConsumedElement{
				NewConsumedSymbol("FOO"),
				NewConsumedToken(&lexer.Token{
					Type: lexer.ItemBinding,
					Text: "?foo",
				}),
				NewConsumedSymbol("FOO"),
				NewConsumedSymbol("FOO"),
				NewConsumedToken(&lexer.Token{
					Type: lexer.ItemAs,
				}),
				NewConsumedSymbol("FOO"),
				NewConsumedSymbol("FOO"),
				NewConsumedToken(&lexer.Token{
					Type: lexer.ItemBinding,
					Text: "?bar",
				}),
				NewConsumedSymbol("FOO"),
			},
			want: &Projection{
				Binding: "?foo",
				Alias:   "?bar",
			},
		},
		{
			valid: true,
			id:    "sum var with alias",
			ces: []ConsumedElement{
				NewConsumedSymbol("FOO"),
				NewConsumedToken(&lexer.Token{
					Type: lexer.ItemSum,
				}),
				NewConsumedSymbol("FOO"),
				NewConsumedToken(&lexer.Token{
					Type: lexer.ItemLPar,
				}),
				NewConsumedSymbol("FOO"),
				NewConsumedToken(&lexer.Token{
					Type: lexer.ItemBinding,
					Text: "?foo",
				}),
				NewConsumedSymbol("FOO"),
				NewConsumedToken(&lexer.Token{
					Type: lexer.ItemRPar,
				}),
				NewConsumedSymbol("FOO"),
				NewConsumedSymbol("FOO"),
				NewConsumedToken(&lexer.Token{
					Type: lexer.ItemAs,
				}),
				NewConsumedSymbol("FOO"),
				NewConsumedSymbol("FOO"),
				NewConsumedToken(&lexer.Token{
					Type: lexer.ItemBinding,
					Text: "?bar",
				}),
				NewConsumedSymbol("FOO"),
			},
			want: &Projection{
				Binding: "?foo",
				Alias:   "?bar",
				OP:      lexer.ItemSum,
			},
		},
		{
			valid: true,
			id:    "count var with alias",
			ces: []ConsumedElement{
				NewConsumedSymbol("FOO"),
				NewConsumedToken(&lexer.Token{
					Type: lexer.ItemCount,
				}),
				NewConsumedSymbol("FOO"),
				NewConsumedToken(&lexer.Token{
					Type: lexer.ItemLPar,
				}),
				NewConsumedSymbol("FOO"),
				NewConsumedToken(&lexer.Token{
					Type: lexer.ItemBinding,
					Text: "?foo",
				}),
				NewConsumedSymbol("FOO"),
				NewConsumedToken(&lexer.Token{
					Type: lexer.ItemRPar,
				}),
				NewConsumedSymbol("FOO"),
				NewConsumedSymbol("FOO"),
				NewConsumedToken(&lexer.Token{
					Type: lexer.ItemAs,
				}),
				NewConsumedSymbol("FOO"),
				NewConsumedSymbol("FOO"),
				NewConsumedToken(&lexer.Token{
					Type: lexer.ItemBinding,
					Text: "?bar",
				}),
				NewConsumedSymbol("FOO"),
			},
			want: &Projection{
				Binding: "?foo",
				Alias:   "?bar",
				OP:      lexer.ItemCount,
			},
		},
		{
			valid: true,
			id:    "count distinct var with alias",
			ces: []ConsumedElement{
				NewConsumedSymbol("FOO"),
				NewConsumedToken(&lexer.Token{
					Type: lexer.ItemCount,
				}),
				NewConsumedSymbol("FOO"),
				NewConsumedToken(&lexer.Token{
					Type: lexer.ItemLPar,
				}),
				NewConsumedSymbol("FOO"),
				NewConsumedToken(&lexer.Token{
					Type: lexer.ItemDistinct,
				}),
				NewConsumedSymbol("FOO"),
				NewConsumedToken(&lexer.Token{
					Type: lexer.ItemBinding,
					Text: "?foo",
				}),
				NewConsumedSymbol("FOO"),
				NewConsumedToken(&lexer.Token{
					Type: lexer.ItemRPar,
				}),
				NewConsumedSymbol("FOO"),
				NewConsumedSymbol("FOO"),
				NewConsumedToken(&lexer.Token{
					Type: lexer.ItemAs,
				}),
				NewConsumedSymbol("FOO"),
				NewConsumedSymbol("FOO"),
				NewConsumedToken(&lexer.Token{
					Type: lexer.ItemBinding,
					Text: "?bar",
				}),
				NewConsumedSymbol("FOO"),
			},
			want: &Projection{
				Binding:  "?foo",
				Alias:    "?bar",
				OP:       lexer.ItemCount,
				Modifier: lexer.ItemDistinct,
			},
		},
	})
}

func TestBindingsGraphChecker(t *testing.T) {
	f := bindingsGraphChecker()
	testTable := []struct {
		id   string
		s    *Statement
		want bool
	}{
		{
			id: "missing binding",
			s: &Statement{
				pattern: []*GraphClause{
					{},
					{SAlias: "?foo"},
					{OAlias: "?foo_bar"},
				},
				projection: []*Projection{
					{Binding: "?foo"},
					{Binding: "?bar"},
				},
			},
			want: false,
		},
		{
			id: "unknown binding",
			s: &Statement{
				pattern: []*GraphClause{
					{},
					{SAlias: "?foo"},
					{PAlias: "?bar"},
					{OAlias: "?foo_bar"},
				},
				projection: []*Projection{
					{Binding: "?unknown"},
				},
			},
			want: false,
		},
		{
			id: "all bindings available",
			s: &Statement{
				pattern: []*GraphClause{
					{},
					{SAlias: "?foo"},
					{PAlias: "?bar"},
					{OAlias: "?foo_bar"},
				},
				projection: []*Projection{
					{Binding: "?foo"},
					{Binding: "?bar"},
					{Binding: "?foo_bar"},
				},
			},
			want: true,
		},
	}
	for _, entry := range testTable {
		if _, err := f(entry.s, Symbol("FOO")); (err == nil) != entry.want {
			t.Errorf("semantic.bindingsGraphChecker invalid statement %#v for case %q; %v", entry.s, entry.id, err)
		}
	}
}

func TestGroupByBindings(t *testing.T) {
	f := groupByBindings()
	testTable := []struct {
		ces  []ConsumedElement
		want []string
	}{
		{
			ces: []ConsumedElement{
				NewConsumedSymbol("FOO"),
				NewConsumedToken(&lexer.Token{
					Type: lexer.ItemBinding,
					Text: "?foo",
				}),
				NewConsumedSymbol("FOO"),
				NewConsumedToken(&lexer.Token{
					Type: lexer.ItemBinding,
					Text: "?bar",
				}),
				NewConsumedSymbol("FOO"),
			},
			want: []string{"?foo", "?bar"},
		},
	}
	st := &Statement{}
	for _, entry := range testTable {
		// Run all tokens.
		for _, ce := range entry.ces {
			if _, err := f(st, ce); err != nil {
				t.Errorf("semantic.groupByBindings should never fail with error %v", err)
			}
		}
		// Check collected output.
		if got, want := st.groupBy, entry.want; !reflect.DeepEqual(got, want) {
			t.Errorf("semantic.groupByBindings failed to collect the expected group by bindings; got %v, want %v", got, want)
		}
	}
}

func TestGroupByBindingsChecker(t *testing.T) {
	f := groupByBindingsChecker()
	testTable := []struct {
		id   string
		s    *Statement
		want bool
	}{
		{
			id:   "empty statement",
			s:    &Statement{},
			want: true,
		},
		{
			id: "one binding",
			s: &Statement{
				projection: []*Projection{
					{Binding: "?foo"},
				},
				groupBy: []string{"?foo"},
			},
			want: true,
		},
		{
			id: "one binding missing aggregation target",
			s: &Statement{
				projection: []*Projection{
					{Binding: "?foo", OP: lexer.ItemSum},
				},
				groupBy: []string{"?foo"},
			},
			want: false,
		},
		{
			id: "two binding",
			s: &Statement{
				projection: []*Projection{
					{Binding: "?foo"},
					{Binding: "?bar", OP: lexer.ItemSum},
				},
				groupBy: []string{"?foo"},
			},
			want: true,
		},
		{
			id: "two binding missing aggregation function",
			s: &Statement{
				projection: []*Projection{
					{Binding: "?foo"},
					{Binding: "?bar"},
				},
				groupBy: []string{"?foo"},
			},
			want: false,
		},
		{
			id: "invalid binding",
			s: &Statement{
				projection: []*Projection{
					{Binding: "?foo"},
					{Binding: "?bar"},
				},
				groupBy: []string{"?invalid_binding"},
			},
			want: false,
		},
	}
	for _, entry := range testTable {
		if _, err := f(entry.s, Symbol("FOO")); (err == nil) != entry.want {
			t.Errorf("semantic.groupByBindingsChecker invalid  group by statement %#v for case %q; %v", entry.s, entry.id, err)
		}
	}
}

func TestOrderByBindings(t *testing.T) {
	f := orderByBindings()
	testTable := []struct {
		ces  []ConsumedElement
		want table.SortConfig
	}{
		{
			ces: []ConsumedElement{
				NewConsumedSymbol("FOO"),
				NewConsumedToken(&lexer.Token{
					Type: lexer.ItemBinding,
					Text: "?foo",
				}),
				NewConsumedSymbol("FOO"),
				NewConsumedToken(&lexer.Token{
					Type: lexer.ItemBinding,
					Text: "?bar",
				}),
				NewConsumedSymbol("FOO"),
				NewConsumedToken(&lexer.Token{
					Type: lexer.ItemBinding,
					Text: "?asc",
				}),
				NewConsumedSymbol("FOO"),
				NewConsumedToken(&lexer.Token{
					Type: lexer.ItemAsc,
				}),
				NewConsumedSymbol("FOO"),
				NewConsumedToken(&lexer.Token{
					Type: lexer.ItemBinding,
					Text: "?desc",
				}),
				NewConsumedSymbol("FOO"),
				NewConsumedToken(&lexer.Token{
					Type: lexer.ItemDesc,
				}),
				NewConsumedSymbol("FOO"),
			},
			want: table.SortConfig{
				{Binding: "?foo", Desc: false},
				{Binding: "?bar", Desc: false},
				{Binding: "?asc", Desc: false},
				{Binding: "?desc", Desc: true},
			},
		},
	}
	st := &Statement{}
	for _, entry := range testTable {
		// Run all tokens.
		for _, ce := range entry.ces {
			if _, err := f(st, ce); err != nil {
				t.Errorf("semantic.orderByBindings should never fail with error %v", err)
			}
		}
		// Check collected output.
		if got, want := st.orderBy, entry.want; !reflect.DeepEqual(got, want) {
			t.Errorf("semantic.orderByBindings failed to collect the expected group by bindings; got %v, want %v", got, want)
		}
	}
}

func TestOrderByBindingsChecker(t *testing.T) {
	f := orderByBindingsChecker()
	testTable := []struct {
		id   string
		s    *Statement
		want bool
	}{
		{
			id:   "empty statement",
			s:    &Statement{},
			want: true,
		},
		{
			id: "one binding",
			s: &Statement{
				projection: []*Projection{
					{Binding: "?foo"},
				},
				orderBy: table.SortConfig{{Binding: "?foo"}},
			},
			want: true,
		},
		{
			id: "two binding",
			s: &Statement{
				projection: []*Projection{
					{Binding: "?foo"},
					{Binding: "?bar"},
				},
				orderBy: table.SortConfig{{Binding: "?foo"}},
			},
			want: true,
		},
		{
			id: "invalid binding",
			s: &Statement{
				projection: []*Projection{
					{Binding: "?foo"},
					{Binding: "?bar"},
				},
				orderBy: table.SortConfig{{Binding: "?invalid_binding"}},
			},
			want: false,
		},
	}
	for _, entry := range testTable {
		if _, err := f(entry.s, Symbol("FOO")); (err == nil) != entry.want {
			t.Errorf("semantic.orderByBindingsChecker invalid order by statement %#v for case %q; %v", entry.s, entry.id, err)
		}
	}
}

func TestHavingExpression(t *testing.T) {
	f := havingExpression()
	testTable := []struct {
		ces  []ConsumedElement
		want []ConsumedElement
	}{
		{
			ces: []ConsumedElement{
				NewConsumedSymbol("FOO"),
				NewConsumedToken(&lexer.Token{
					Type: lexer.ItemLPar,
				}),
				NewConsumedSymbol("FOO"),
				NewConsumedToken(&lexer.Token{
					Type: lexer.ItemBinding,
					Text: "?foo",
				}),
				NewConsumedSymbol("FOO"),
				NewConsumedToken(&lexer.Token{
					Type: lexer.ItemGT,
				}),
				NewConsumedSymbol("FOO"),
				NewConsumedToken(&lexer.Token{
					Type: lexer.ItemBinding,
					Text: "?bar",
				}),
				NewConsumedSymbol("FOO"),
				NewConsumedToken(&lexer.Token{
					Type: lexer.ItemRPar,
				}),

				NewConsumedSymbol("FOO"),
				NewConsumedToken(&lexer.Token{
					Type: lexer.ItemAnd,
				}),

				NewConsumedSymbol("FOO"),
				NewConsumedToken(&lexer.Token{
					Type: lexer.ItemLPar,
				}),
				NewConsumedSymbol("FOO"),
				NewConsumedToken(&lexer.Token{
					Type: lexer.ItemBinding,
					Text: "?foo",
				}),
				NewConsumedSymbol("FOO"),
				NewConsumedToken(&lexer.Token{
					Type: lexer.ItemGT,
				}),
				NewConsumedSymbol("FOO"),
				NewConsumedToken(&lexer.Token{
					Type: lexer.ItemBinding,
					Text: "?bar",
				}),
				NewConsumedSymbol("FOO"),
				NewConsumedToken(&lexer.Token{
					Type: lexer.ItemRPar,
				}),
				NewConsumedSymbol("FOO"),
				NewConsumedToken(&lexer.Token{
					Type: lexer.ItemAsc,
				}),
				NewConsumedSymbol("FOO"),
				NewConsumedToken(&lexer.Token{
					Type: lexer.ItemBinding,
					Text: "?desc",
				}),
				NewConsumedSymbol("FOO"),
				NewConsumedToken(&lexer.Token{
					Type: lexer.ItemDesc,
				}),
				NewConsumedSymbol("FOO"),
			},
			want: []ConsumedElement{
				NewConsumedToken(&lexer.Token{
					Type: lexer.ItemLPar,
				}),
				NewConsumedToken(&lexer.Token{
					Type: lexer.ItemBinding,
					Text: "?foo",
				}),
				NewConsumedToken(&lexer.Token{
					Type: lexer.ItemGT,
				}),
				NewConsumedToken(&lexer.Token{
					Type: lexer.ItemBinding,
					Text: "?bar",
				}),
				NewConsumedToken(&lexer.Token{
					Type: lexer.ItemRPar,
				}),
				NewConsumedToken(&lexer.Token{
					Type: lexer.ItemAnd,
				}),
				NewConsumedToken(&lexer.Token{
					Type: lexer.ItemLPar,
				}),
				NewConsumedToken(&lexer.Token{
					Type: lexer.ItemBinding,
					Text: "?foo",
				}),
				NewConsumedToken(&lexer.Token{
					Type: lexer.ItemGT,
				}),
				NewConsumedToken(&lexer.Token{
					Type: lexer.ItemBinding,
					Text: "?bar",
				}),
				NewConsumedToken(&lexer.Token{
					Type: lexer.ItemRPar,
				}),
				NewConsumedToken(&lexer.Token{
					Type: lexer.ItemAsc,
				}),
				NewConsumedToken(&lexer.Token{
					Type: lexer.ItemBinding,
					Text: "?desc",
				}),
				NewConsumedToken(&lexer.Token{
					Type: lexer.ItemDesc,
				}),
			},
		},
	}
	st := &Statement{}
	for _, entry := range testTable {
		// Run all tokens.
		for _, ce := range entry.ces {
			if _, err := f(st, ce); err != nil {
				t.Errorf("semantic.havingExpression should never fail with error %v", err)
			}
		}
		// Check collected output.
		if got, want := st.havingExpression, entry.want; !reflect.DeepEqual(got, want) {
			t.Errorf("semantic.havingExpression failed to collect the expected tokens; got %v, want %v", got, want)
		}
	}
}

func TestHavingExpressionBuilder(t *testing.T) {
	f := havingExpressionBuilder()
	testTable := []struct {
		id   string
		s    *Statement
		r    table.Row
		want bool
	}{
		{
			id:   "empty statement",
			s:    &Statement{},
			want: true,
		},
		{
			id: "(?foo < ?bar) or (?foo > ?bar)",
			s: &Statement{
				havingExpression: []ConsumedElement{
					NewConsumedToken(&lexer.Token{
						Type: lexer.ItemLPar,
					}),
					NewConsumedToken(&lexer.Token{
						Type: lexer.ItemBinding,
						Text: "?foo",
					}),
					NewConsumedToken(&lexer.Token{
						Type: lexer.ItemLT,
					}),
					NewConsumedToken(&lexer.Token{
						Type: lexer.ItemBinding,
						Text: "?bar",
					}),
					NewConsumedToken(&lexer.Token{
						Type: lexer.ItemRPar,
					}),
					NewConsumedToken(&lexer.Token{
						Type: lexer.ItemOr,
					}),
					NewConsumedToken(&lexer.Token{
						Type: lexer.ItemLPar,
					}),
					NewConsumedToken(&lexer.Token{
						Type: lexer.ItemBinding,
						Text: "?foo",
					}),
					NewConsumedToken(&lexer.Token{
						Type: lexer.ItemGT,
					}),
					NewConsumedToken(&lexer.Token{
						Type: lexer.ItemBinding,
						Text: "?bar",
					}),
					NewConsumedToken(&lexer.Token{
						Type: lexer.ItemRPar,
					}),
				},
			},
			r: table.Row{
				"?foo": &table.Cell{S: table.CellString("foo")},
				"?bar": &table.Cell{S: table.CellString("bar")},
			},
			want: true,
		},
		{
			id: "not((?foo < ?bar) or (?foo > ?bar))",
			s: &Statement{
				havingExpression: []ConsumedElement{
					NewConsumedToken(&lexer.Token{
						Type: lexer.ItemNot,
					}),
					NewConsumedToken(&lexer.Token{
						Type: lexer.ItemLPar,
					}),
					NewConsumedToken(&lexer.Token{
						Type: lexer.ItemLPar,
					}),
					NewConsumedToken(&lexer.Token{
						Type: lexer.ItemBinding,
						Text: "?foo",
					}),
					NewConsumedToken(&lexer.Token{
						Type: lexer.ItemLT,
					}),
					NewConsumedToken(&lexer.Token{
						Type: lexer.ItemBinding,
						Text: "?bar",
					}),
					NewConsumedToken(&lexer.Token{
						Type: lexer.ItemRPar,
					}),
					NewConsumedToken(&lexer.Token{
						Type: lexer.ItemOr,
					}),
					NewConsumedToken(&lexer.Token{
						Type: lexer.ItemLPar,
					}),
					NewConsumedToken(&lexer.Token{
						Type: lexer.ItemBinding,
						Text: "?foo",
					}),
					NewConsumedToken(&lexer.Token{
						Type: lexer.ItemGT,
					}),
					NewConsumedToken(&lexer.Token{
						Type: lexer.ItemBinding,
						Text: "?bar",
					}),
					NewConsumedToken(&lexer.Token{
						Type: lexer.ItemRPar,
					}),
					NewConsumedToken(&lexer.Token{
						Type: lexer.ItemRPar,
					}),
				},
			},
			r: table.Row{
				"?foo": &table.Cell{S: table.CellString("foo")},
				"?bar": &table.Cell{S: table.CellString("bar")},
			},
			want: false,
		},
	}
	for _, entry := range testTable {
		if _, err := f(entry.s, Symbol("FOO")); err != nil {
			t.Errorf("semantic.havingExpressionBuilder faile to build statement %#v for case %q with error %v", entry.s, entry.id, err)
		}
		got, err := entry.s.havingExpressionEvaluator.Evaluate(entry.r)
		if err != nil {
			t.Errorf("expression evaluator should have not fail with errorf %v for case %q", err, entry.id)
		}
		if want := entry.want; got != want {
			t.Errorf("expression evaluator returned the wrong value for case %q; got %v, want %v", entry.id, got, want)
		}
	}
}

func TestLimitCollection(t *testing.T) {
	f := limitCollection()
	testTable := []struct {
		in   []ConsumedElement
		want int64
	}{
		{
			in: []ConsumedElement{
				NewConsumedSymbol("FOO"),
				NewConsumedToken(&lexer.Token{
					Type: lexer.ItemLiteral,
					Text: `"1234"^^type:int64`,
				}),
				NewConsumedSymbol("FOO"),
			},
			want: 1234,
		},
	}
	st := &Statement{}
	for _, entry := range testTable {
		// Run all tokens.
		for _, ce := range entry.in {
			if _, err := f(st, ce); err != nil {
				t.Errorf("semantic.limitCollection should never fail with error %v", err)
			}
		}
		// Check collected output.
		if got, want := st.limit, entry.want; !st.limitSet || got != want {
			t.Errorf("semantic.limitClause failed to collect the expected value; got %v, want %v (%v)", got, want, st.limitSet)
		}
	}
}

func TestCollectGlobalBounds(t *testing.T) {
	f := collectGlobalBounds()
	date := "2015-07-19T13:12:04.669618843-07:00"
	pd, err := time.Parse(time.RFC3339Nano, date)
	if err != nil {
		t.Fatalf("time.Parse failed to parse valid time %s with error %v", date, err)
	}
	pretty, invalid := date, fmt.Sprintf("\"INVALID\"@[%s]", date)
	testTable := []struct {
		id   string
		in   []ConsumedElement
		want storage.LookupOptions
		fail bool
	}{
		{
			id: "before X",
			in: []ConsumedElement{
				NewConsumedSymbol("FOO"),
				NewConsumedToken(&lexer.Token{
					Type: lexer.ItemBefore,
				}),
				NewConsumedSymbol("FOO"),
				NewConsumedToken(&lexer.Token{
					Type: lexer.ItemTime,
					Text: pretty,
				}),
				NewConsumedSymbol("FOO"),
			},
			want: storage.LookupOptions{
				UpperAnchor: &pd,
			},
			fail: false,
		},
		{
			id: "after X",
			in: []ConsumedElement{
				NewConsumedSymbol("FOO"),
				NewConsumedToken(&lexer.Token{
					Type: lexer.ItemAfter,
				}),
				NewConsumedSymbol("FOO"),
				NewConsumedToken(&lexer.Token{
					Type: lexer.ItemTime,
					Text: pretty,
				}),
				NewConsumedSymbol("FOO"),
			},
			want: storage.LookupOptions{
				LowerAnchor: &pd,
			},
			fail: false,
		},
		{
			id: "between X, Y",
			in: []ConsumedElement{
				NewConsumedSymbol("FOO"),
				NewConsumedToken(&lexer.Token{
					Type: lexer.ItemBetween,
				}),
				NewConsumedSymbol("FOO"),
				NewConsumedToken(&lexer.Token{
					Type: lexer.ItemPredicateBound,
					Text: fmt.Sprintf("%s, %s", date, date),
				}),
				NewConsumedSymbol("FOO"),
			},
			want: storage.LookupOptions{
				LowerAnchor: &pd,
				UpperAnchor: &pd,
			},
			fail: false,
		},
		{
			id: "before INVALID_X",
			in: []ConsumedElement{
				NewConsumedSymbol("FOO"),
				NewConsumedToken(&lexer.Token{
					Type: lexer.ItemBefore,
				}),
				NewConsumedSymbol("FOO"),
				NewConsumedToken(&lexer.Token{
					Type: lexer.ItemTime,
					Text: invalid,
				}),
				NewConsumedSymbol("FOO"),
			},
			fail: true,
		},
		{
			id: "after INVALID_X",
			in: []ConsumedElement{
				NewConsumedSymbol("FOO"),
				NewConsumedToken(&lexer.Token{
					Type: lexer.ItemAfter,
				}),
				NewConsumedSymbol("FOO"),
				NewConsumedToken(&lexer.Token{
					Type: lexer.ItemTime,
					Text: invalid,
				}),
				NewConsumedSymbol("FOO"),
			},
			fail: true,
		},
		{
			id: "between X, INVALID_Y",
			in: []ConsumedElement{
				NewConsumedSymbol("FOO"),
				NewConsumedToken(&lexer.Token{
					Type: lexer.ItemBetween,
				}),
				NewConsumedSymbol("FOO"),
				NewConsumedToken(&lexer.Token{
					Type: lexer.ItemPredicateBound,
					Text: fmt.Sprintf("%s, notADate", date),
				}),
				NewConsumedSymbol("FOO"),
			},
			fail: true,
		},
		{
			id: "between X, NO_UPPER_BOUND",
			in: []ConsumedElement{
				NewConsumedSymbol("FOO"),
				NewConsumedToken(&lexer.Token{
					Type: lexer.ItemBetween,
				}),
				NewConsumedSymbol("FOO"),
				NewConsumedToken(&lexer.Token{
					Type: lexer.ItemPredicateBound,
					Text: fmt.Sprintf("%s, ", date),
				}),
				NewConsumedSymbol("FOO"),
			},
			fail: true,
		},
	}
	for _, entry := range testTable {
		st := &Statement{}
		// Run all tokens.
		fail := false
		for _, ce := range entry.in {
			if _, err := f(st, ce); (err != nil) && !entry.fail {
				t.Errorf("semantic.CollectGlobalBounds should never fail with error %v for case %q", err, entry.id)
				fail = true
				break
			}
		}
		if fail {
			continue
		}
		// Check collected output.
		if got, want := st.lookupOptions, entry.want; !entry.fail && !reflect.DeepEqual(got, want) {
			t.Errorf("semantic.CollectGlobalBounds failed to collect the expected value for case %q; got %v, want %v (%v)", entry.id, got, want, st.limitSet)
		}
	}
}

func TestInitWorkingConstructClauseHook(t *testing.T) {
	f := InitWorkingConstructClause()
	st := &Statement{}
	f(st, Symbol("FOO"))
	if st.WorkingConstructClause() == nil {
		t.Errorf("semantic.InitConstructWorkingClause should have returned a valid working clause for statement %v", st)
	}
}

func TestNextWorkingConstructClauseHook(t *testing.T) {
	f := NextWorkingConstructClause()
	st := &Statement{}
	st.ResetWorkingConstructClause()
	wcs := st.WorkingConstructClause()
	wcs.SBinding = "?a"
	f(st, Symbol("FOO"))
	wcs = st.WorkingConstructClause()
	wcs.SBinding = "?b"
	f(st, Symbol("FOO"))
	if got, want := len(st.ConstructClauses()), 2; got != want {
		t.Errorf("semantic.NextWorkingConstructClause should have returned two clauses for statement %v; got %d, want %d", st, got, want)
	}
}

func TestNextWorkingConstructPredicateObjectPairClauseHook(t *testing.T) {
	f := NextWorkingConstructPredicateObjectPair()
	st := &Statement{}
	st.ResetWorkingConstructClause()
	wcc := st.WorkingConstructClause()
	wcc.ResetWorkingPredicateObjectPair()
	wrs := wcc.WorkingPredicateObjectPair()
	wrs.PBinding = "?a"
	f(st, Symbol("FOO"))
	wrs = wcc.WorkingPredicateObjectPair()
	wrs.PBinding = "?b"
	f(st, Symbol("FOO"))
	if got, want := len(wcc.PredicateObjectPairs()), 2; got != want {
		t.Errorf("semantic.NextWorkingConstructPredicateObjectPair should have returned two clauses for statement %v; got %d, want %d", st, got, want)
	}
}

type testConstructSubjectHookTable struct {
	valid bool
	id    string
	ces   []ConsumedElement
	want  *ConstructClause
}

func runTabulatedConstructSubjectHookTest(t *testing.T, testName string, f ElementHook, table []testConstructSubjectHookTable) {
	st := &Statement{}
	st.ResetWorkingConstructClause()
	failed := false
	for _, entry := range table {
		for _, ce := range entry.ces {
			if _, err := f(st, ce); err != nil {
				if entry.valid {
					t.Errorf("%s case %q should have never failed with error: %v", testName, entry.id, err)
				} else {
					failed = true
				}
			}
		}
		if entry.valid {
			if got, want := st.WorkingConstructClause(), entry.want; !reflect.DeepEqual(got, want) {
				t.Errorf("%s case %q should have populated all required fields; got %+v, want %+v", testName, entry.id, got, want)
			}
		} else {
			if !failed {
				t.Errorf("%s failed to reject invalid case %q", testName, entry.id)
			}
		}
		st.ResetWorkingConstructClause()
	}
}

func TestConstructSubjectHook(t *testing.T) {
	st := &Statement{}
	f := constructSubject()
	st.ResetWorkingConstructClause()
	n, err := node.Parse("/_<foo>")
	if err != nil {
		t.Fatalf("node.Parse called for '/_<foo>' failed with error %v", err)
	}
	bn, err := node.Parse("_:v1")
	if err != nil {
		t.Fatalf("node.Parse called for '_:v1' failed with error %v", err)
	}
	runTabulatedConstructSubjectHookTest(t, "semantic.constructSubject", f, []testConstructSubjectHookTable{
		{
			valid: true,
			id:    "valid node",
			ces: []ConsumedElement{
				NewConsumedSymbol("CONSTRUCT_TRIPLES"),
				NewConsumedToken(&lexer.Token{
					Type: lexer.ItemNode,
					Text: "/_<foo>",
				}),
			},
			want: &ConstructClause{
				S: n,
			},
		},
		{
			valid: true,
			id:    "valid blank node",
			ces: []ConsumedElement{
				NewConsumedSymbol("CONSTRUCT_TRIPLES"),
				NewConsumedToken(&lexer.Token{
					Type: lexer.ItemBlankNode,
					Text: "_:v1",
				}),
			},
			want: &ConstructClause{
				S: bn,
			},
		},
		{
			valid: true,
			id:    "valid binding",
			ces: []ConsumedElement{
				NewConsumedSymbol("CONSTRUCT_TRIPLES"),
				NewConsumedToken(&lexer.Token{
					Type: lexer.ItemBinding,
					Text: "?foo",
				}),
			},
			want: &ConstructClause{
				SBinding: "?foo",
			},
		},
		{
			valid: false,
			id:    "invalid node and binding",
			ces: []ConsumedElement{
				NewConsumedSymbol("CONSTRUCT_TRIPLES"),
				NewConsumedToken(&lexer.Token{
					Type: lexer.ItemBlankNode,
					Text: "_:v1",
				}),
				NewConsumedSymbol("FOO"),
				NewConsumedToken(&lexer.Token{
					Type: lexer.ItemBinding,
					Text: "?foo",
				}),
			},
			want: &ConstructClause{},
		},
	})
}

type testConstructPredicateObjectHooksTable struct {
	valid bool
	id    string
	ces   []ConsumedElement
	want  *ConstructPredicateObjectPair
}

func runTabulatedConstructPredicateObjectHooksTest(t *testing.T, testName string, f ElementHook, table []testConstructPredicateObjectHooksTable) {
	st := &Statement{}
	st.ResetWorkingConstructClause()
	wcc := st.WorkingConstructClause()
	wcc.ResetWorkingPredicateObjectPair()
	failed := false
	for _, entry := range table {
		for _, ce := range entry.ces {
			if _, err := f(st, ce); err != nil {
				if entry.valid {
					t.Errorf("%s case %q should have never failed with error: %v", testName, entry.id, err)
				} else {
					failed = true
				}
			}
		}
		if entry.valid {
			if got, want := wcc.WorkingPredicateObjectPair(), entry.want; !reflect.DeepEqual(got, want) {
				t.Errorf("%s case %q should have populated all required fields; got %+v, want %+v", testName, entry.id, got, want)
			}
		} else {
			if !failed {
				t.Errorf("%s failed to reject invalid case %q", testName, entry.id)
			}
		}
		wcc.ResetWorkingPredicateObjectPair()
	}
}

func TestConstructPredicateHook(t *testing.T) {
	st := &Statement{}
	f := constructPredicate()
	st.ResetWorkingConstructClause()
	wcc := st.WorkingConstructClause()
	wcc.ResetWorkingPredicateObjectPair()
	ip, err := predicate.Parse(`"foo"@[]`)
	if err != nil {
		t.Fatalf("predicate.Parse failed with error %v", err)
	}
	tp, err := predicate.Parse(`"foo"@[2015-07-19T13:12:04.669618843-07:00]`)
	if err != nil {
		t.Fatalf("predicate.Parse failed with error %v", err)
	}
	runTabulatedConstructPredicateObjectHooksTest(t, "semantic.constructPredicateClause", f, []testConstructPredicateObjectHooksTable{
		{
			valid: true,
			id:    "valid immutable predicate",
			ces: []ConsumedElement{
				NewConsumedSymbol("CONSTRUCT_PREDICATE"),
				NewConsumedToken(&lexer.Token{
					Type: lexer.ItemPredicate,
					Text: `"foo"@[]`,
				}),
			},
			want: &ConstructPredicateObjectPair{
				P:         ip,
				PTemporal: false,
			},
		},
		{
			valid: true,
			id:    "valid temporal predicate",
			ces: []ConsumedElement{
				NewConsumedSymbol("CONSTRUCT_PREDICATE"),
				NewConsumedToken(&lexer.Token{
					Type: lexer.ItemPredicate,
					Text: `"foo"@[2015-07-19T13:12:04.669618843-07:00]`,
				}),
			},
			want: &ConstructPredicateObjectPair{
				P:         tp,
				PTemporal: true,
			},
		},
		{
			valid: true,
			id:    "valid temporal predicate with bound time anchor",
			ces: []ConsumedElement{
				NewConsumedSymbol("CONSTRUCT_PREDICATE"),
				NewConsumedToken(&lexer.Token{
					Type: lexer.ItemPredicate,
					Text: `"foo"@[?bar]`,
				}),
			},
			want: &ConstructPredicateObjectPair{
				PID:            "foo",
				PAnchorBinding: "?bar",
				PTemporal:      true,
			},
		},
		{
			valid: true,
			id:    "valid binding",
			ces: []ConsumedElement{
				NewConsumedSymbol("CONSTRUCT_PREDICATE"),
				NewConsumedToken(&lexer.Token{
					Type: lexer.ItemBinding,
					Text: "?foo",
				}),
			},
			want: &ConstructPredicateObjectPair{
				PBinding: "?foo",
			},
		},
		{
			valid: false,
			id:    "invalid temporal predicate and binding",
			ces: []ConsumedElement{
				NewConsumedSymbol("CONSTRUCT_PREDICATE"),
				NewConsumedToken(&lexer.Token{
					Type: lexer.ItemPredicate,
					Text: `"foo"@[?bar]`,
				}),
				NewConsumedSymbol("FOO"),
				NewConsumedToken(&lexer.Token{
					Type: lexer.ItemBinding,
					Text: "?foo",
				}),
			},
			want: &ConstructPredicateObjectPair{},
		},
	})
}

func TestConstructObjectHook(t *testing.T) {
	st := &Statement{}
	f := constructObject()
	st.ResetWorkingConstructClause()
	wcc := st.WorkingConstructClause()
	wcc.ResetWorkingPredicateObjectPair()
	n, err := node.Parse("/_<foo>")
	if err != nil {
		t.Fatalf("node.Parse failed with error %v", err)
	}
	no := triple.NewNodeObject(n)
	bn, err := node.Parse("_:v1")
	if err != nil {
		t.Fatalf("node.Parse failed with error %v", err)
	}
	bno := triple.NewNodeObject(bn)
	ip, err := predicate.Parse(`"foo"@[]`)
	if err != nil {
		t.Fatalf("predicate.Parse failed with error %v", err)
	}
	ipo := triple.NewPredicateObject(ip)
	tp, err := predicate.Parse(`"foo"@[2015-07-19T13:12:04.669618843-07:00]`)
	if err != nil {
		t.Fatalf("predicate.Parse failed with error %v", err)
	}
	tpo := triple.NewPredicateObject(tp)
	l, err := triple.ParseObject(`"1"^^type:int64`, literal.DefaultBuilder())
	if err != nil {
		t.Fatalf("literal.Parse should never fail to parse %s with error %v", `"1"^^type:int64`, err)
	}
	runTabulatedConstructPredicateObjectHooksTest(t, "semantic.constructObjectClause", f, []testConstructPredicateObjectHooksTable{
		{
			valid: true,
			id:    "valid node object",
			ces: []ConsumedElement{
				NewConsumedSymbol("CONSTRUCT_OBJECT"),
				NewConsumedToken(&lexer.Token{
					Type: lexer.ItemNode,
					Text: "/_<foo>",
				}),
			},
			want: &ConstructPredicateObjectPair{
				O: no,
			},
		},
		{
			valid: true,
			id:    "valid blank node object",
			ces: []ConsumedElement{
				NewConsumedSymbol("CONSTRUCT_OBJECT"),
				NewConsumedToken(&lexer.Token{
					Type: lexer.ItemBlankNode,
					Text: "_:v1",
				}),
			},
			want: &ConstructPredicateObjectPair{
				O: bno,
			},
		},
		{
			valid: true,
			id:    "valid literal object",
			ces: []ConsumedElement{
				NewConsumedSymbol("CONSTRUCT_OBJECT"),
				NewConsumedToken(&lexer.Token{
					Type: lexer.ItemLiteral,
					Text: `"1"^^type:int64`,
				}),
			},
			want: &ConstructPredicateObjectPair{
				O: l,
			},
		},
		{
			valid: true,
			id:    "valid immutable predicate object",
			ces: []ConsumedElement{
				NewConsumedSymbol("CONSTRUCT_OBJECT"),
				NewConsumedToken(&lexer.Token{
					Type: lexer.ItemPredicate,
					Text: `"foo"@[]`,
				}),
			},
			want: &ConstructPredicateObjectPair{
				O:         ipo,
				OTemporal: false,
			},
		},
		{
			valid: true,
			id:    "valid temporal predicate object",
			ces: []ConsumedElement{
				NewConsumedSymbol("CONSTRUCT_OBJECT"),
				NewConsumedToken(&lexer.Token{
					Type: lexer.ItemPredicate,
					Text: `"foo"@[2015-07-19T13:12:04.669618843-07:00]`,
				}),
			},
			want: &ConstructPredicateObjectPair{
				O:         tpo,
				OTemporal: true,
			},
		},
		{
			valid: true,
			id:    "valid temporal predicate object with bound time anchor",
			ces: []ConsumedElement{
				NewConsumedSymbol("CONSTRUCT_OBJECT"),
				NewConsumedToken(&lexer.Token{
					Type: lexer.ItemPredicate,
					Text: `"foo"@[?bar]`,
				}),
			},
			want: &ConstructPredicateObjectPair{
				OID:            "foo",
				OAnchorBinding: "?bar",
				OTemporal:      true,
			},
		},
		{
			valid: true,
			id:    "valid binding",
			ces: []ConsumedElement{
				NewConsumedSymbol("CONSTRUCT_OBJECT"),
				NewConsumedToken(&lexer.Token{
					Type: lexer.ItemBinding,
					Text: "?foo",
				}),
			},
			want: &ConstructPredicateObjectPair{
				OBinding: "?foo",
			},
		},
		{
			valid: false,
			id:    "invalid temporal predicate and binding objects",
			ces: []ConsumedElement{
				NewConsumedSymbol("CONSTRUCT_OBJECT"),
				NewConsumedToken(&lexer.Token{
					Type: lexer.ItemPredicate,
					Text: `"foo"@[?bar]`,
				}),
				NewConsumedSymbol("FOO"),
				NewConsumedToken(&lexer.Token{
					Type: lexer.ItemBinding,
					Text: "?foo",
				}),
			},
			want: &ConstructPredicateObjectPair{},
		},
	})
}
