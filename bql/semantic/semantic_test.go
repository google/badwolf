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

	"github.com/google/badwolf/bql/grammar"
	"github.com/google/badwolf/bql/semantic"
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

func TestAcceptOpsByParseAndSemantic(t *testing.T) {
	table := []struct {
		query   string
		graphs  int
		triples int
	}{
		// Insert data.
		{`insert data into ?a {/_<foo> "bar"@[1975-01-01T00:01:01.999999999Z] /_<foo>};`, 1, 1},
		{`insert data into ?a {/_<foo> "bar"@[] "bar"@[1975-01-01T00:01:01.999999999Z]};`, 1, 1},
		{`insert data into ?a {/_<foo> "bar"@[] "yeah"^^type:text};`, 1, 1},
		// Insert into multiple graphs.
		{`insert data into ?a,?b,?c {/_<foo> "bar"@[] /_<foo>};`, 3, 1},
		// Insert multiple data.
		{`insert data into ?a {/_<foo> "bar"@[] /_<foo> .
			                      /_<foo> "bar"@[] "bar"@[1975-01-01T00:01:01.999999999Z] .
			                      /_<foo> "bar"@[] "yeah"^^type:text};`, 1, 3},
		// Delete data.
		{`delete data from ?a {/_<foo> "bar"@[] /_<foo>};`, 1, 1},
		{`delete data from ?a {/_<foo> "bar"@[] "bar"@[1975-01-01T00:01:01.999999999Z]};`, 1, 1},
		{`delete data from ?a {/_<foo> "bar"@[] "yeah"^^type:text};`, 1, 1},
		// Delete from multiple graphs.
		{`delete data from ?a,?b,?c {/_<foo> "bar"@[1975-01-01T00:01:01.999999999Z] /_<foo>};`, 3, 1},
		// Delete multiple data.
		{`delete data from ?a {/_<foo> "bar"@[] /_<foo> .
			                      /_<foo> "bar"@[] "bar"@[1975-01-01T00:01:01.999999999Z] .
			                      /_<foo> "bar"@[] "yeah"^^type:text};`, 1, 3},
		// Create graphs.
		{`create graph ?foo;`, 1, 0},
		// Drop graphs.
		{`drop graph ?foo, ?bar;`, 2, 0},
	}
	p, err := grammar.NewParser(grammar.SemanticBQL())
	if err != nil {
		t.Errorf("grammar.NewParser: should have produced a valid BQL parser")
	}
	for _, entry := range table {
		st := &semantic.Statement{}
		if err := p.Parse(grammar.NewLLk(entry.query, 1), st); err != nil {
			t.Errorf("Parser.consume: failed to accept entry %q with error %v", entry, err)
		}
		if got, want := len(st.Graphs()), entry.graphs; got != want {
			t.Errorf("Parser.consume: failed to collect right number of graphs for case %v; got %d, want %d", entry, got, want)
		}
		if got, want := len(st.Data()), entry.triples; got != want {
			t.Errorf("Parser.consume: failed to collect right number of triples for case %v; got %d, want %d", entry, got, want)
		}
	}
}

func TestAcceptQueryBySemanticParse(t *testing.T) {
	table := []string{
		// Test well type litterals are accepted.
		`select ?s from ?g where{?s ?p "1"^^type:int64};`,
		// Test predicates are accepted.
		// Test invalid predicate time anchor are rejected.
		`select ?s from ?b where{/_<foo> as ?s "id"@[2015] ?o};`,
		`select ?s from ?b where{/_<foo> as ?s "id"@[2015-07] ?o};`,
		`select ?s from ?b where{/_<foo> as ?s "id"@[2015-07-19] ?o};`,
		`select ?s from ?g where{/_<foo> as ?s "id"@[2015-07-19T13:12] ?o};`,
		`select ?s from ?g where{/_<foo> as ?s "id"@[2015-07-19T13:12:04] ?o};`,
		`select ?s from ?g where{/_<foo> as ?s "id"@[2015-07-19T13:12:04.669618843] ?o};`,
		`select ?s from ?g where{/_<foo> as ?s "id"@[2015-07-19T13:12:04.669618843-07:00] ?o};`,
		`select ?s from ?g where{/_<foo> as ?s "id"@[2015-07-19T13:12:04.669618843-07:00] as ?p ?o};`,
		`select ?s from ?g where{/_<foo> as ?s  ?p "id"@[2015-07-19T13:12:04.669618843-07:00] as ?o};`,
		// Test predicates with bindings are accepted.
		`select ?s from ?g where{/_<foo> as ?s "id"@[?ta] ?o};`,
		`select ?s from ?g where{/_<foo> as ?s  ?p "id"@[?ta] as ?o};`,
		// Test predicate bounds are accepted.
		`select ?s from ?g where{/_<foo> as ?s "id"@[2015-07-19T13:12:04.669618843-07:00, 2016-07-19T13:12:04.669618843-07:00] ?o};`,
		`select ?s from ?g where{/_<foo> as ?s  ?p "id"@[2015-07-19T13:12:04.669618843-07:00, 2016-07-19T13:12:04.669618843-07:00] as ?o};`,
		// Test predicate bounds with bounds are accepted.
		`select ?s from ?g where{/_<foo> as ?s "id"@[?foo, 2016-07-19T13:12:04.669618843-07:00] ?o};`,
		`select ?s from ?g where{/_<foo> as ?s  ?p "id"@[2015-07-19T13:12:04.669618843-07:00, ?bar] as ?o};`,
		`select ?s from ?g where{/_<foo> as ?s  ?p "id"@[?foo, ?bar] as ?o};`}
	p, err := grammar.NewParser(grammar.SemanticBQL())
	if err != nil {
		t.Errorf("grammar.NewParser: should have produced a valid BQL parser")
	}
	for _, input := range table {
		if err := p.Parse(grammar.NewLLk(input, 1), &semantic.Statement{}); err != nil {
			t.Errorf("Parser.consume: failed to accept input %q with error %v", input, err)
		}
	}
}

func TestRejectByParseAndSemantic(t *testing.T) {
	table := []string{
		// Test wront type litterals are rejected.
		`select ?s from ?g where{?s ?p "true"^^type:int64};`,
		// Test invalid predicate bounds are rejected.
		`select ?s from ?b where{/_<foo> as ?s "id"@[2018-07-19T13:12:04.669618843-07:00, 2015-07-19T13:12:04.669618843-07:00] ?o};`,
		`select ?s from ?b where{/_<foo> as ?s  ?p "id"@[2019-07-19T13:12:04.669618843-07:00, 2015-07-19T13:12:04.669618843-07:00] as ?o};`,
	}
	p, err := grammar.NewParser(grammar.SemanticBQL())
	if err != nil {
		t.Errorf("grammar.NewParser: should have produced a valid BQL parser")
	}
	for _, entry := range table {
		st := &semantic.Statement{}
		if err := p.Parse(grammar.NewLLk(entry, 1), st); err == nil {
			t.Errorf("Parser.consume: failed to reject invalid semantic entry %q", entry)
		}
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
	bds := stm.Bindings()
	if len(bds) != 10 {
		t.Errorf("Statement.Bindings failed to reteurn 10 bindings, instead returned %v", bds)
	}
	for b, cnt := range bds {
		if cnt != 20 {
			t.Errorf("Statement.Bindings failed to update binding %q to 20, got %d instead", b, cnt)
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
