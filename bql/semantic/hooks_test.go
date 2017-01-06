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

func TestSemanticAcceptInsertDelete(t *testing.T) {
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
		err  error
	)
	hook = graphAccumulator()
	for _, ce := range ces {
		hook, err = hook(st, ce)
		if err != nil {
			t.Errorf("semantic.GraphAccumulator hook should have never failed for %v with error %v", ce, err)
		}
	}
	data := st.GraphNames()
	if len(data) != 2 {
		t.Errorf("semantic.GraphAccumulator hook should have produced 2 graph bindings; instead produced %v", st.Graphs())
	}
	for _, g := range data {
		if g != "?foo" && g != "?bar" {
			t.Errorf("semantic.GraphAccumulator hook failed to provied either ?foo or ?bar; got %v instead", g)
		}
	}
}

func TestTypeBindingClauseHook(t *testing.T) {
	f := TypeBindingClauseHook(Insert)
	st := &Statement{}
	f(st, Symbol("FOO"))
	if got, want := st.Type(), Insert; got != want {
		t.Errorf("semantic.TypeBidingHook failed to set the right type; got %s, want %s", got, want)
	}
}

func TestWhereInitClauseHook(t *testing.T) {
	f := whereInitWorkingClause()
	st := &Statement{}
	f(st, Symbol("FOO"))
	if st.WorkingClause() == nil {
		t.Errorf("semantic.WhereInitWorkingClause should have returned a valid working clause for statement %v", st)
	}
}

func TestWhereWorkingClauseHook(t *testing.T) {
	f := whereNextWorkingClause()
	st := &Statement{}
	st.ResetWorkingGraphClause()
	f(st, Symbol("FOO"))
	f(st, Symbol("FOO"))
	if len(st.GraphPatternClauses()) != 2 {
		t.Errorf("semantic.whereNextWorkingClause should have returned two clauses for statement %v", st)
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
				t.Errorf("%s case %q should have populated all subject fields; got %+v, want %+v", testName, entry.id, got, want)
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
		t.Fatalf("literal.Parse should have never fail to pars %s with error %v", `"1"^^type:int64`, err)
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
			id: "missing biding",
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
			id: "all bidings available",
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
				{"?foo", false},
				{"?bar", false},
				{"?asc", false},
				{"?desc", true},
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
	pretty, invalid := fmt.Sprintf("\"\"@[%s]", date), fmt.Sprintf("\"INVALID\"@[%s]", date)
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
					Type: lexer.ItemPredicate,
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
					Type: lexer.ItemPredicate,
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
					Type: lexer.ItemPredicate,
					Text: pretty,
				}),
				NewConsumedSymbol("FOO"),
				NewConsumedToken(&lexer.Token{
					Type: lexer.ItemComma,
				}),
				NewConsumedSymbol("FOO"),
				NewConsumedToken(&lexer.Token{
					Type: lexer.ItemPredicate,
					Text: pretty,
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
					Type: lexer.ItemPredicate,
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
					Type: lexer.ItemPredicate,
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
					Type: lexer.ItemPredicate,
					Text: pretty,
				}),
				NewConsumedSymbol("FOO"),
				NewConsumedToken(&lexer.Token{
					Type: lexer.ItemComma,
				}),
				NewConsumedSymbol("FOO"),
				NewConsumedToken(&lexer.Token{
					Type: lexer.ItemPredicate,
					Text: invalid,
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
