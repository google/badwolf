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

	"github.com/google/badwolf/triple"
	"github.com/google/badwolf/triple/literal"
	"github.com/google/badwolf/triple/node"
	"github.com/google/badwolf/triple/predicate"
)

func TestStatementType(t *testing.T) {
	st := &Statement{}
	st.BindType(Query)
	if got, want := st.Type(), Query; got != want {
		t.Errorf("semantic.NewStatement returned wrong statement type; got %s, want %s", got, want)
	}
}

func TestStatementAddGraph(t *testing.T) {
	st := &Statement{}
	st.BindType(Query)
	st.AddGraph("?foo")
	if got, want := st.Graphs(), []string{"?foo"}; !reflect.DeepEqual(got, want) {
		t.Errorf("semantic.AddGraph returned the wrong graphs avaiable; got %v, want %v", got, want)
	}
}

func TestStatementAddData(t *testing.T) {
	tr, err := triple.ParseTriple(`/_<foo> "foo"@[] /_<bar>`, literal.DefaultBuilder())
	if err != nil {
		t.Fatalf("triple.ParseTriple failed to parse valid triple with error %v", err)
	}
	st := &Statement{}
	st.BindType(Query)
	st.AddData(tr)
	if got, want := st.Data(), []*triple.Triple{tr}; !reflect.DeepEqual(got, want) {
		t.Errorf("semantic.AddData returned the wrong data avaiable; got %v, want %v", got, want)
	}
}

func TestGraphClauseSpecificity(t *testing.T) {
	table := []struct {
		gc   *GraphClause
		want int
	}{
		{&GraphClause{}, 0},
		{&GraphClause{S: &node.Node{}}, 1},
		{&GraphClause{S: &node.Node{}, P: &predicate.Predicate{}}, 2},
		{&GraphClause{S: &node.Node{}, P: &predicate.Predicate{}, O: &triple.Object{}}, 3},
	}
	for _, entry := range table {
		if got, want := entry.gc.Specificity(), entry.want; got != want {
			t.Errorf("semantic.GraphClause.Specificity failed to return the proper value for %v; got %d, want %d", entry.gc, got, want)
		}
	}
}

func TestGraphClauseManipulation(t *testing.T) {
	st := &Statement{}
	if st.WorkingClause() != nil {
		t.Fatalf("semantic.GraphClause.WorkingClause should not return a working clause without initilization in %v", st)
	}
	st.ResetWorkingGraphClause()
	if st.WorkingClause() == nil {
		t.Fatalf("semantic.GraphClause.WorkingClause should return a working clause after initilization in %v", st)
	}
	st.AddWorkingGrpahClause()
	if got, want := len(st.GraphPatternClauses()), 1; got != want {
		t.Fatalf("semantic.GraphClause.Clauses return wrong number of clauses in %v; got %d, want %d", st, got, want)
	}
}

func TestBindingListing(t *testing.T) {
	stm := Statement{}
	stm.ResetWorkingGraphClause()
	for i := 0; i < 10; i++ {
		wcls := stm.WorkingClause()
		v := string(i)
		cls := &GraphClause{
			SBinding:         "?" + v,
			SAlias:           "?" + v,
			STypeAlias:       "?" + v,
			SIDAlias:         "?" + v,
			PAlias:           "?" + v,
			PID:              "?" + v,
			PAnchorBinding:   "?" + v,
			PBinding:         "?" + v,
			PLowerBoundAlias: "?" + v,
			PUpperBoundAlias: "?" + v,
			PIDAlias:         "?" + v,
			PAnchorAlias:     "?" + v,
			OBinding:         "?" + v,
			OID:              "?" + v,
			OAlias:           "?" + v,
			OTypeAlias:       "?" + v,
			OIDAlias:         "?" + v,
			OAnchorAlias:     "?" + v,
			OAnchorBinding:   "?" + v,
			OLowerBoundAlias: "?" + v,
			OUpperBoundAlias: "?" + v,
		}
		*wcls = *cls
		stm.AddWorkingGrpahClause()
	}
	bds := stm.BindingsMap()
	if len(bds) != 10 {
		t.Errorf("Statement.Bindings failed to reteurn 10 bindings, instead returned %v", bds)
	}
	for b, cnt := range bds {
		if cnt != 19 {
			t.Errorf("Statement.Bindings failed to update binding %q to 20, got %d instead", b, cnt)
		}
	}
}

func TesIsEmptyClause(t *testing.T) {
	testTable := []struct {
		in  *GraphClause
		out bool
	}{
		{
			in:  &GraphClause{},
			out: true,
		},
		{
			in:  &GraphClause{SBinding: "?foo"},
			out: true,
		},
	}
	for _, entry := range testTable {
		if got, want := entry.in.IsEmpty(), entry.out; got != want {
			t.Errorf("IsEmpty for %v returned %v, but should have returned %v", entry.in, got, want)
		}
	}

}

func TestSortedGraphPatternClauses(t *testing.T) {
	s := &Statement{
		pattern: []*GraphClause{
			{},
			{S: &node.Node{}},
			{S: &node.Node{}, P: &predicate.Predicate{}},
			{S: &node.Node{}, P: &predicate.Predicate{}, O: &triple.Object{}},
		},
	}
	spc := 3
	for _, cls := range s.SortedGraphPatternClauses() {
		if want, got := spc, cls.Specificity(); got != want {
			t.Errorf("statement.SortedGraphPatternClauses failed to sort properly; got specificity %d, want specificity %d", got, want)
		}
		spc--
	}
}
