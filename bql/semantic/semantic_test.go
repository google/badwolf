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

	"github.com/google/badwolf/triple"
	"github.com/google/badwolf/triple/literal"
	"github.com/google/badwolf/triple/node"
	"github.com/google/badwolf/triple/predicate"
)

func TestStamentTypeString(t *testing.T) {

	table := []struct {
		tt   StatementType
		want string
	}{
		{Query, "QUERY"},
		{Insert, "INSERT"},
		{Delete, "DELETE"},
		{Create, "CREATE"},
		{Drop, "DROP"},
		{Construct, "CONSTRUCT"},
		{Deconstruct, "DECONSTRUCT"},
		{Show, "SHOW"},
		{StatementType(-1), "UNKNOWN"},
	}

	for i, entry := range table {
		if got, want := entry.tt.String(), entry.want; got != want {
			t.Errorf("[case %d] failed; got %v, want %v", i, got, want)
		}
	}
}

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
	if got, want := st.GraphNames(), []string{"?foo"}; !reflect.DeepEqual(got, want) {
		t.Errorf("semantic.AddGraph returned the wrong graphs available; got %v, want %v", got, want)
	}
}

func TestStatementAddInputGraph(t *testing.T) {
	st := &Statement{}
	st.BindType(Query)
	st.AddInputGraph("?foo")
	if got, want := st.InputGraphNames(), []string{"?foo"}; !reflect.DeepEqual(got, want) {
		t.Errorf("semantic.AddInputGraph returned the wrong graphs available; got %v, want %v", got, want)
	}
}

func TestStatementAddOutputGraph(t *testing.T) {
	st := &Statement{}
	st.BindType(Query)
	st.AddOutputGraph("?foo")
	if got, want := st.OutputGraphNames(), []string{"?foo"}; !reflect.DeepEqual(got, want) {
		t.Errorf("semantic.AddOutputGraph returned the wrong graphs available; got %v, want %v", got, want)
	}
}

func TestStatementAddData(t *testing.T) {
	tr, err := triple.Parse(`/_<foo> "foo"@[] /_<bar>`, literal.DefaultBuilder())
	if err != nil {
		t.Fatalf("triple.Parse failed to parse valid triple with error %v", err)
	}
	st := &Statement{}
	st.BindType(Query)
	st.AddData(tr)
	if got, want := st.Data(), []*triple.Triple{tr}; !reflect.DeepEqual(got, want) {
		t.Errorf("semantic.AddData returned the wrong data available; got %v, want %v", got, want)
	}
}

func TestGraphClauseString(t *testing.T) {
	timeObj1 := time.Date(2019, 11, 20, 2, 30, 10, 5, time.UTC)
	timeObj2 := time.Date(2019, 12, 3, 5, 40, 20, 7, time.UTC)
	timeObj3 := time.Date(2019, 11, 30, 22, 10, 50, 3, time.UTC)
	timeObj4 := time.Date(2019, 12, 2, 17, 0, 10, 15, time.UTC)
	// Testing that NewNodeFromStrings is not the point of this package. Taking the example from the unit tests.
	n, _ := node.NewNodeFromStrings("/some/type", "id_1")
	// Testing NewImmutable is not the point of this package.
	immutFoo, _ := predicate.NewImmutable("foo")
	nO, _ := node.NewNodeFromStrings("/some/other/type", "id_2")
	o := triple.NewNodeObject(nO)
	table := []struct {
		gc   *GraphClause
		want string
	}{
		{&GraphClause{}, `{ opt=false @[][] }`},
		{
			&GraphClause{
				Optional:         true,
				S:                n,
				SBinding:         "?nBinding",
				SAlias:           "?nAlias",
				STypeAlias:       "?nTypeAlias",
				SIDAlias:         "?nIDAlias",
				P:                immutFoo,
				PID:              "?predID",
				PBinding:         "?predBinding",
				PAlias:           "?predAlias",
				PIDAlias:         "?predIDAlias",
				PAnchorBinding:   "?predAnchorBinding",
				PAnchorAlias:     "?predAnchorAlias",
				PLowerBound:      &timeObj1,
				PUpperBound:      &timeObj2,
				PLowerBoundAlias: "?earlyYesterday",
				PUpperBoundAlias: "?someTimeInTheFuture",
				PTemporal:        true,
				O:                o,
				OBinding:         "?objBinding",
				OAlias:           "?objAlias",
				OID:              "?objID",
				OTypeAlias:       "?objTypeAlias",
				OIDAlias:         "?objCuteID",
				OAnchorBinding:   "?Popeyes",
				OAnchorAlias:     "?Olive",
				OLowerBound:      &timeObj3,
				OUpperBound:      &timeObj4,
				OLowerBoundAlias: "?SometimeSoon",
				OUpperBoundAlias: "?seemsSoFarAway",
				OTemporal:        true,
			},
			`{ opt=true /some/type<id_1> AS ?nAlias TYPE ?nTypeAlias ID ?nIDAlias "foo"@[] ?predBinding "?predID" AS ?predAlias ID ?predIDAlias AT ?predAnchorAlias /some/other/type<id_2> AS ?objAlias TYPE ?objTypeAlias ID ?objCuteID AT ?Olive AS ?objAlias ID ?objCuteID }`,
		},
		{
			&GraphClause{
				Optional:         true,
				S:                nil,
				SBinding:         "?nBinding",
				SAlias:           "?nAlias",
				STypeAlias:       "?nTypeAlias",
				SIDAlias:         "?nIDAlias",
				P:                nil,
				PID:              "?predID",
				PBinding:         "?predBinding",
				PAlias:           "?predAlias",
				PIDAlias:         "?predIDAlias",
				PAnchorBinding:   "?predAnchorBinding",
				PAnchorAlias:     "?predAnchorAlias",
				PLowerBound:      &timeObj1,
				PUpperBound:      &timeObj2,
				PLowerBoundAlias: "?earlyYesterday",
				PUpperBoundAlias: "?someTimeInTheFuture",
				PTemporal:        false,
				O:                nil,
				OAnchorBinding:   "?Popeyes",
				OLowerBound:      &timeObj3,
				OUpperBound:      &timeObj4,
				OLowerBoundAlias: "?SometimeSoon",
				OUpperBoundAlias: "?seemsSoFarAway",
				OTemporal:        false,
			},
			`{ opt=true ?nBinding AS ?nAlias TYPE ?nTypeAlias ID ?nIDAlias ?predBinding "?predID"@[] AS ?predAlias ID ?predIDAlias AT ?predAnchorAlias[] }`,
		},
		{
			&GraphClause{
				Optional:         true,
				S:                nil,
				SBinding:         "?nBinding",
				SAlias:           "?nAlias",
				STypeAlias:       "?nTypeAlias",
				SIDAlias:         "?nIDAlias",
				P:                nil,
				PID:              "?predID",
				PBinding:         "?predBinding",
				PAlias:           "?predAlias",
				PIDAlias:         "?predIDAlias",
				PAnchorBinding:   "?predAnchorBinding",
				PAnchorAlias:     "?predAnchorAlias",
				PLowerBound:      &timeObj1,
				PUpperBound:      &timeObj2,
				PLowerBoundAlias: "?earlyYesterday",
				PUpperBoundAlias: "?someTimeInTheFuture",
				PTemporal:        true,
				O:                nil,
				OBinding:         "?objBinding",
				OAlias:           "?objAlias",
				OID:              "?objID",
				OTypeAlias:       "?objTypeAlias",
				OIDAlias:         "?objCuteID",
				OAnchorAlias:     "?Olive",
				OLowerBound:      &timeObj3,
				OUpperBound:      &timeObj4,
				OLowerBoundAlias: "?SometimeSoon",
				OUpperBoundAlias: "?seemsSoFarAway",
				OTemporal:        true,
			},
			`{ opt=true ?nBinding AS ?nAlias TYPE ?nTypeAlias ID ?nIDAlias ?predBinding "?predID"@[?predAnchorBinding at ?predAnchorAlias] AS ?predAlias ID ?predIDAlias AT ?predAnchorAlias ?objBinding "?objID"[2019-11-30T22:10:50.000000003Z,2019-12-02T17:00:10.000000015Z] AS ?objAlias TYPE ?objTypeAlias ID ?objCuteID AT ?Olive AS ?objAlias ID ?objCuteID }`,
		},
	}

	for i, entry := range table {
		if got, want := entry.gc.String(), entry.want; got != want {
			t.Errorf("[case %d] failed; got %v, want %v", i, got, want)
		}
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
	st.AddWorkingGraphClause()
	if got, want := len(st.GraphPatternClauses()), 0; got != want {
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
		stm.AddWorkingGraphClause()
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

func TestIsEmptyClause(t *testing.T) {
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
			out: false,
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

func TestProjectionIsEmpty(t *testing.T) {
	s := &Statement{}
	s.ResetProjection()
	if !s.WorkingProjection().IsEmpty() {
		t.Errorf("s.WorkingProjections().IsEmpty() should be empty after reset, instead got %s", s.WorkingProjection())
	}
	if len(s.Projections()) != 0 {
		t.Errorf("s.Projections should be empty, instead got %s", s.Projections())
	}
	s.AddWorkingProjection()
	if len(s.Projections()) != 0 {
		t.Errorf("s.Projections should be empty after adding an empty projection, instead got %s", s.Projections())
	}
	p := s.WorkingProjection()
	p.Binding = "?foo"
	s.AddWorkingProjection()
	if len(s.Projections()) != 1 {
		t.Errorf("s.Projections should constina one projection, instead got %s", s.Projections())
	}
}

func TestConstructClauseManipulation(t *testing.T) {
	st := &Statement{}
	if st.WorkingConstructClause() != nil {
		t.Fatalf("semantic.ConstructClause.WorkingConstructClause should not return a working construct clause without initialization in %v", st)
	}
	st.ResetWorkingConstructClause()
	if st.WorkingConstructClause() == nil {
		t.Fatalf("semantic.ConstructClause.WorkingConstructClause should return a working construct clause after initialization in %v", st)
	}
	st.AddWorkingConstructClause()
	if got, want := len(st.ConstructClauses()), 0; got != want {
		t.Fatalf("semantic.ConstructClause.ConstructClauses returns wrong number of clauses in %v; got %d, want %d", st, got, want)
	}
}

func TestConstructPredicateObjectPairsManipulation(t *testing.T) {
	st := &Statement{}
	st.ResetWorkingConstructClause()
	wcc := st.WorkingConstructClause()
	if wcc.WorkingPredicateObjectPair() != nil {
		t.Fatalf("semantic.ConstructClause.WorkingPredicateObjectPair should not return a working predicate-object pair without initialization in %v", st)
	}
	wcc.ResetWorkingPredicateObjectPair()
	if wcc.WorkingPredicateObjectPair() == nil {
		t.Fatalf("semantic.ConstructClause.WorkingPredicateObjectPair should return a working predicate-object pair after initialization in %v", st)
	}
	wcc.AddWorkingPredicateObjectPair()
	if got, want := len(wcc.PredicateObjectPairs()), 0; got != want {
		t.Fatalf("semantic.ConstructClause.PredicateObjectPairs returns wrong number of predicate-object pairs in %v; got %d, want %d", st, got, want)
	}
}

func TestInputOutputBindings(t *testing.T) {
	s := &Statement{
		projection: []*Projection{
			{
				Binding: "?foo",
				Alias:   "?foo_alias",
			},
			{
				Binding: "?bar",
			},
		},
		constructClauses: []*ConstructClause{
			{
				SBinding: "?foo1",
				predicateObjectPairs: []*ConstructPredicateObjectPair{
					{
						PBinding: "?foo2",
						OBinding: "?foo3",
					},
				},
			},
			{
				SBinding: "?foo4",
				predicateObjectPairs: []*ConstructPredicateObjectPair{
					{
						PBinding: "?foo5",
						OBinding: "?foo6",
					},
					{
						PBinding: "?foo7",
						OBinding: "?foo8",
					},
					{
						PBinding: "?foo9",
						OBinding: "?foo10",
					},
				},
			},
			{
				predicateObjectPairs: []*ConstructPredicateObjectPair{
					{
						PAnchorBinding: "?foo11",
						OAnchorBinding: "?foo12",
					},
					{
						PAnchorBinding: "?foo13",
						OAnchorBinding: "?foo13",
					},
				},
			},
		},
	}
	want := []string{"?foo", "?bar", "?foo1", "?foo2", "?foo3", "?foo4", "?foo5", "?foo6",
		"?foo7", "?foo8", "?foo9", "?foo10", "?foo11", "?foo12", "?foo13", "?foo13"}
	if got := s.InputBindings(); !reflect.DeepEqual(got, want) {
		t.Errorf("s.InputBindings returned the wrong input bindings; got %v, want %v", got, want)
	}
	want = []string{"?foo_alias", "?bar", "?foo1", "?foo2", "?foo3", "?foo4", "?foo5", "?foo6",
		"?foo7", "?foo8", "?foo9", "?foo10", "?foo11", "?foo12", "?foo13"}
	if got := s.OutputBindings(); !reflect.DeepEqual(got, want) {
		t.Errorf("s.OutputBindings returned the wrong output bindings; got %v, want %v", got, want)
	}
}

func TestHasAlias(t *testing.T) {
	accept := []*GraphClause{
		{
			SAlias: "?t",
		},
		{
			STypeAlias: "?t",
		},
		{
			SIDAlias: "?t",
		},
		{
			PAlias: "?t",
		},
		{
			PIDAlias: "?t",
		},
		{
			PAnchorAlias: "?t",
		},
		{
			PLowerBoundAlias: "?t",
		},
		{
			PUpperBoundAlias: "?t",
		},
		{
			OAlias: "?t",
		},
		{
			OTypeAlias: "?t",
		},
		{
			OIDAlias: "?t",
		},
		{
			OAnchorAlias: "?t",
		},
		{
			OLowerBoundAlias: "?t",
		},
		{
			OUpperBoundAlias: "?t",
		},
	}

	for i, c := range accept {
		if !c.HasAlias() {
			t.Errorf("[case %d] failed to return true for clause %s", i, c.String())
		}
	}
}
