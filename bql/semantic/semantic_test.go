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
		{&GraphClause{s: &node.Node{}}, 1},
		{&GraphClause{s: &node.Node{}, p: &predicate.Predicate{}}, 2},
		{&GraphClause{s: &node.Node{}, p: &predicate.Predicate{}, o: &triple.Object{}}, 3},
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

func TestAcceptByParseAndSemantic(t *testing.T) {
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
	p, err := grammar.NewParser(&grammar.SemanticBQL)
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

func TestRejectByParseAndSemantic(t *testing.T) {
	table := []string{
		`insert data into ?a {/_<foo> "bar"@["1234"] /_<foo>};`,
		`delete data from ?a {/_<foo> "bar"@[] "bar"@[123]};`,
		`create graph foo;`,
		`drop graph ?foo ?bar;`,
	}
	p, err := grammar.NewParser(&grammar.SemanticBQL)
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
