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
	"reflect"
	"testing"
	"time"

	"github.com/google/badwolf/bql/lexer"
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
		if got, want := trpl.S().String(), "/_<s>"; got != want {
			t.Errorf("semantic.DataAccumulator hook failed to parse subject correctly; got %v, want %v", got, want)
		}
		if got, want := trpl.P().String(), `"p"@[]`; got != want {
			t.Errorf("semantic.DataAccumulator hook failed to parse prdicate correctly; got %v, want %v", got, want)
		}
		if got, want := trpl.O().String(), "/_<o>"; got != want {
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
	data := st.Graphs()
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

func TestWhereSubjectClauseHook(t *testing.T) {
	st := &Statement{}
	f := whereSubjectClause()
	st.ResetWorkingGraphClause()
	n, err := node.Parse("/_<foo>")
	if err != nil {
		t.Fatalf("node.Parse failed with error %v", err)
	}
	table := []struct {
		ces  []ConsumedElement
		want *GraphClause
	}{
		{
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
				s:          n,
				sAlias:     "?bar",
				sTypeAlias: "?bar2",
				sIDAlias:   "?bar3",
			},
		},
		{
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
				sBinding:   "?foo",
				sAlias:     "?bar",
				sTypeAlias: "?bar2",
				sIDAlias:   "?bar3",
			},
		},
	}
	for _, entry := range table {
		for _, ce := range entry.ces {
			if _, err := f(st, ce); err != nil {
				t.Errorf("semantic.whereSubjectClause should have never failed with error: %v", err)
			}
		}
		if got, want := st.WorkingClause(), entry.want; !reflect.DeepEqual(got, want) {
			t.Errorf("smeantic.whereSubjectClause should have populated all subject fields; got %+v, want %+v", got, want)
		}
		st.ResetWorkingGraphClause()
	}
}

func TestWherePredicatClauseHook(t *testing.T) {
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
	table := []struct {
		valid bool
		id    string
		ces   []ConsumedElement
		want  *GraphClause
	}{
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
				p:            p,
				pAlias:       "?bar",
				pIDAlias:     "?bar2",
				pAnchorAlias: "?bar3",
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
				pID:            "foo",
				pAnchorBinding: "?foo",
				pAlias:         "?bar",
				pIDAlias:       "?bar2",
				pAnchorAlias:   "?bar3",
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
				pID:              "foo",
				pLowerBoundAlias: "?fooLower",
				pUpperBoundAlias: "?fooUpper",
				pAlias:           "?bar",
				pIDAlias:         "?bar2",
				pAnchorAlias:     "?bar3",
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
				pID:          "foo",
				pLowerBound:  &tlb,
				pUpperBound:  &tub,
				pAlias:       "?bar",
				pIDAlias:     "?bar2",
				pAnchorAlias: "?bar3",
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
	}
	failed := false
	for _, entry := range table {
		for _, ce := range entry.ces {
			if _, err := f(st, ce); err != nil {
				if entry.valid {
					t.Errorf("semantic.wherePredicateClause case %q should have never failed with error: %v", entry.id, err)
				} else {
					failed = true
				}
			}
		}
		if entry.valid {
			if got, want := st.WorkingClause(), entry.want; !reflect.DeepEqual(got, want) {
				t.Errorf("semantic.wherePredicateClause case %q should have populated all subject fields; got %+v, want %+v", entry.id, got, want)
			}
		} else {
			if !failed {
				t.Errorf("semantic.wherePredicateClause failed to reject invalid case %q", entry.id)
			}
		}
		st.ResetWorkingGraphClause()
	}
}
