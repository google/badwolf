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

package grammar

import (
	"testing"

	"github.com/google/badwolf/bql/semantic"
)

func TestAcceptByParse(t *testing.T) {
	table := []string{
		// Test multiple var bindings are accepted.
		`select ?a from ?b where{?s ?p ?o};`,
		`select ?a, ?b from ?c where{?s ?p ?o};`,
		`select ?a, ?b, ?c from ?d where{?s ?p ?o};`,
		// Test aliases and functions.
		`select ?a as ?b from ?c where{?s ?p ?o};`,
		`select ?a as ?b, ?c as ?d from ?e where{?s ?p ?o};`,
		`select count(?a) as ?b, sum(?c) as ?d, ?e as ?f from ?g where{?s ?p ?o};`,
		`select count(distinct ?a) as ?b from ?c where{?s ?p ?o};`,
		// Test multiple graphs are accepted.
		`select ?a from ?b where{?s ?p ?o};`,
		`select ?a from ?b, ?c where{?s ?p ?o};`,
		`select ?a from ?b, ?c, ?d where{?s ?p ?o};`,
		// Test non empty clause.
		`select ?a from ?b where{?s ?p ?o};`,
		`select ?a from ?b where{?s as ?x ?p ?o};`,
		`select ?a from ?b where{?s as ?x type ?y ?p ?o};`,
		`select ?a from ?b where{?s as ?x type ?y id ?z ?p ?o};`,
		`select ?a from ?b where{?s ?p as ?x ?o};`,
		`select ?a from ?b where{?s ?p as ?x id ?y ?o};`,
		`select ?a from ?b where{?s ?p as ?x id ?y at ?z ?o};`,
		`select ?a from ?b where{?s ?p ?o as ?x};`,
		`select ?a from ?b where{?s ?p ?o as ?x type ?y};`,
		`select ?a from ?b where{?s ?p ?o as ?x type ?y id ?z};`,
		`select ?a from ?b where{?s ?p ?o as ?x type ?y id ?z at ?t};`,
		// Test clause with predicate bounds.
		`select ?a from ?b where{?s "foo"@[,] ?o};`,
		`select ?a from ?b where{?s "foo"@[,] as ?x id ?y at ?z ?o};`,
		`select ?a from ?b where{?s "foo"@[,] as ?x id ?y at ?z, ?zz ?o};`,
		`select ?a from ?b where{?s ?p "foo"@[,] as ?x id ?z at ?t, ?tt};`,
		// Test multiple clauses.
		`select ?a from ?b where{?s ?p ?o};`,
		`select ?a from ?b where{?s ?p ?o . ?s ?p ?o};`,
		`select ?a from ?b where{?s ?p ?o . ?s ?p ?o . ?s ?p ?o};`,
		// Test group by.
		`select ?a from ?b where{?s ?p ?o} group by ?a;`,
		`select ?a from ?b where{?s ?p ?o} group by ?a, ?b;`,
		`select ?a from ?b where{?s ?p ?o} group by ?a, ?b, ?c;`,
		// Test order by.
		`select ?a from ?b where{?s ?p ?o} order by ?a;`,
		`select ?a from ?b where{?s ?p ?o} order by ?a asc;`,
		`select ?a from ?b where{?s ?p ?o} order by ?a desc;`,
		`select ?a from ?b where{?s ?p ?o} order by ?a asc, ?b desc;`,
		`select ?a from ?b where{?s ?p ?o} order by ?a desc, ?b desc, ?c asc;`,
		// Test having clause.
		`select ?a from ?b where {?a ?p ?o} having not ?b;`,
		`select ?a from ?b where {?a ?p ?o} having (not ?b);`,
		`select ?a from ?b where {?a ?p ?o} having ?b and ?b;`,
		`select ?a from ?b where {?a ?p ?o} having ?b or ?b;`,
		`select ?a from ?b where {?a ?p ?o} having ?b < ?b;`,
		`select ?a from ?b where {?a ?p ?o} having ?b > ?b;`,
		`select ?a from ?b where {?a ?p ?o} having ?b = ?b;`,
		`select ?a from ?b where {?a ?p ?o} having (?b and ?b) or not (?b = ?b);`,
		`select ?a from ?b where {?a ?p ?o} having ((?b and ?b) or not (?b = ?b));`,
		// Test global time bounds.
		`select ?a from ?b where {?s ?p ?o} before ""@["123"];`,
		`select ?a from ?b where {?s ?p ?o} after ""@["123"];`,
		`select ?a from ?b where {?s ?p ?o} between ""@["123"], ""@["123"];`,
		// Test limit clause.
		`select ?a from ?b where {?s ?p ?o} limit "10"^^type:int64;`,
		// Insert data.
		`insert data into ?a {/_<foo> "bar"@["1234"] /_<foo>};`,
		`insert data into ?a {/_<foo> "bar"@["1234"] "bar"@["1234"]};`,
		`insert data into ?a {/_<foo> "bar"@["1234"] "yeah"^^type:text};`,
		// Insert into multiple graphs.
		`insert data into ?a,?b,?c {/_<foo> "bar"@["1234"] /_<foo>};`,
		// Insert multiple data.
		`insert data into ?a {/_<foo> "bar"@["1234"] /_<foo> .
		                      /_<foo> "bar"@["1234"] "bar"@["1234"] .
		                      /_<foo> "bar"@["1234"] "yeah"^^type:text};`,
		// Delete data.
		`delete data from ?a {/_<foo> "bar"@["1234"] /_<foo>};`,
		`delete data from ?a {/_<foo> "bar"@["1234"] "bar"@["1234"]};`,
		`delete data from ?a {/_<foo> "bar"@["1234"] "yeah"^^type:text};`,
		// Delete from multiple graphs.
		`delete data from ?a,?b,?c {/_<foo> "bar"@["1234"] /_<foo>};`,
		// Delete multiple data.
		`delete data from ?a {/_<foo> "bar"@["1234"] /_<foo> .
										      /_<foo> "bar"@["1234"] "bar"@["1234"] .
													/_<foo> "bar"@["1234"] "yeah"^^type:text};`,
		// Create graphs.
		`create graph ?a;`,
		`create graph ?a, ?b, ?c;`,
		// Drop graphs.
		`drop graph ?a;`,
		`drop graph ?a, ?b, ?c;`,
		// Issue 39 (https://github.com/google/badwolf/issues/39)
		`insert data into ?world {/room<000> "named"@[] "Hallway"^^type:text.
		                          /room<000> "connects_to"@[] /room<001>};`,
		`delete data from ?world {/room<000> "named"@[] "Hallway"^^type:text.
		                          /room<000> "connects_to"@[] /room<001>};`,
		// Test Construct clause.
		`construct {?s "new_predicate"@[] ?o} into ?a from ?b where {?s "old_predicate"@[,] ?o} having ?s = ?o;`,
		`construct {?s "new_predicate"@[] ?o} into ?a from ?b where {?s "old_predicate"@[,] ?o};`,
		`construct {?s ?p ?o} into ?a from ?b where {?n "_subject"@[] ?s.
							     ?n "_predicate"@[] ?p.
						             ?n "_object"@[] ?o};`,
		`construct {?s ?p ?o.
			    _:v "_subject"@[] ?s.
			    _:v "_predicate"@[] ?p.
			    _:v "_object"@[] ?o} into ?a from ?b where {?n "_subject"@[] ?s.
									?n "_predicate"@[] ?p.
									?n "_object"@[] ?o};`,
		`construct {?s "predicate_1"@[] ?o1;
			       "predicate_2"@[] ?o2} into ?a from ?b where {?s "old_predicate_1"@[,] ?o1.
									    ?s "old_predicate_2"@[,] ?o2};`,

		`construct {?s "predicate_1"@[] ?o1;
			       "predicate_2"@[] ?o2.
		            ?s "predicate_3"@[] ?o3} into ?a from ?b where {?s "old_predicate_1"@[,] ?o1.
									    ?s "old_predicate_2"@[,] ?o2.
									    ?s "old_predicate_3"@[,] ?o3};`,
	}
	p, err := NewParser(BQL())
	if err != nil {
		t.Errorf("grammar.NewParser: should have produced a valid BQL parser, %v", err)
	}
	for _, input := range table {
		if err := p.Parse(NewLLk(input, 1), &semantic.Statement{}); err != nil {
			t.Errorf("Parser.consume: failed to accept input %q with error %v", input, err)
		}
	}
}

func TestRejectByParse(t *testing.T) {
	table := []string{
		// Reject missing comas on var bindings or missing bindings.
		`select ?a ?wrong from ?b;`,
		`select ?a , from ?b;`,
		`select ?a as from ?b;`,
		`select ?a as ?b, from ?b;`,
		`select count(?a as ?b, from ?b;`,
		`select count(distinct) as ?a, from ?c;`,
		// Reject missing comas on var bindings or missing graphs.
		`select ?a from ?b ?c;`,
		`select ?a from ?b,;`,
		// Reject empty where clause.
		`select ?a from ?b where{};`,
		// Reject incomplete empty where clause.
		`select ?a from ?b where {;`,
		`select ?a from ?b where };`,
		// Reject incomplete clauses.
		`select ?a from ?b where {?s ?p};`,
		`select ?a from ?b where {?s ?p ?o . ?};`,
		// Reject incomplete clause aliasing.
		`select ?a from ?b where {?s id ?b as ?c ?d ?o};`,
		`select ?a from ?b where {?s ?p at ?t as ?a ?o};`,
		`select ?a from ?b where {?s ?p ?o at ?t id ?i};`,
		// Reject incomplete group by.
		`select ?a from ?b where{?s ?p ?o} group by;`,
		`select ?a from ?b where{?s ?p ?o} group ?a;`,
		`select ?a from ?b where{?s ?p ?o} by ?a;`,
		// Reject incomplete order by.
		`select ?a from ?b where{?s ?p ?o} order by;`,
		`select ?a from ?b where{?s ?p ?o} order ?a;`,
		`select ?a from ?b where{?s ?p ?o} by ?a;`,
		`select ?a from ?b where{?s ?p ?o} order by ?a, a;`,
		`select ?a from ?b where{?s ?p ?o} order by ?a, ?b, desc;`,
		// Reject invalid having clauses.
		`select ?a from ?b where {?a ?p ?o} having not ;`,
		`select ?a from ?b where {?a ?p ?o} having not ?b ?b;`,
		`select ?a from ?b where {?a ?p ?o} having (not );`,
		`select ?a from ?b where {?a ?p ?o} having and ?b;`,
		`select ?a from ?b where {?a ?p ?o} having ?b or ;`,
		`select ?a from ?b where {?a ?p ?o} having ?b  ?b;`,
		`select ?a from ?b where {?a ?p ?o} having > ?b;`,
		`select ?a from ?b where {?a ?p ?o} having ?b = ;`,
		`select ?a from ?b where {?a ?p ?o} having () or not (?b = ?b);`,
		`select ?a from ?b where {?a ?p ?o} having ((?b and ?b) (?b = ?b));`,
		// Reject invalid global time bounds.
		`select ?a from ?b where {?s ?p ?o} before ;`,
		`select ?a from ?b where {?s ?p ?o} after ;`,
		`select ?a from ?b where {?s ?p ?o} between "foo"@["123"], ;`,
		`select ?a from ?b where {?s ?p ?o} before "foo"@["123"]);`,
		`select ?a from ?b where {?s ?p ?o} before "foo"@["123"]  before "foo"@["123"];`,
		`select ?a from ?b where {?s ?p ?o} before "foo"@["123"] or before "foo"@["123"] ,;`,
		`select ?a from ?b where {?s ?p ?o} before "foo"@["123"] or before "foo"@["123"] and before "foo"@["123"]);`,
		// Test limit clause.
		`select ?a from ?b where {?s ?p ?o} limit ?b;`,
		`select ?a from ?b where {?s ?p ?o} limit ;`,
		// Insert incomplete data.
		`insert data into ?a {"bar"@["1234"] /_<foo>};`,
		`insert data into ?a {/_<foo> "bar"@["1234"]};`,
		`insert data into ?a {/_<foo> "bar"@["1234"]};`,
		// Insert into multiple incomplete graphs.
		`insert data into ?a,?b, {/_<foo> "bar"@["1234"] /_<foo>};`,
		// Insert multiple incomplete data.
		`insert data into ?a {/_<foo> "bar"@["1234"] /_<foo> .
		                      /_<foo> "bar"@["1234"] "bar"@["1234"] .
		                      "bar"@["1234"] "yeah"^^type:text};`,
		// Delete incomplete data.
		`delete data from ?a {"bar"@["1234"] /_<foo>};`,
		`delete data from ?a {/_<foo> "bar"@["1234"]};`,
		`delete data from ?a {/_<foo> "bar"@["1234"]};`,
		// Delete from multiple incomplete graphs.
		`delete data from ?a,?b, {/_<foo> "bar"@["1234"] /_<foo>};`,
		// Delete multiple incomplete data.
		`delete data from ?a {/_<foo> "bar"@["1234"] /_<foo> .
										      /_<foo> "bar"@["1234"] "bar"@["1234"] .
													"bar"@["1234"] "yeah"^^type:text};`,
		// Create graphs.
		`create graph ;`,
		`create graph ?a, ?b ?c;`,
		// Drop graphs.
		`drop graph ;`,
		`drop graph ?a ?b, ?c;`,
		// Construct clause without source.
		`construct {?s "foo"@[,] ?o} into ?a where{?s "foo"@[,] ?o} having ?s = ?o;`,
		// Construct clause without destination.
		`construct {?s "foo"@[,] ?o} from ?b where{?s "foo"@[,] ?o} having ?s = ?o;`,
		// Construct clause with badly formed blank node.
		`construct {?s ?p ?o.
			    _v "some_pred"@[] ?k } into ?a from ?b where {?s "foo"@[,] ?o};`,
		// Construct clause with badly formed triple.
		`construct {?s ?p ?o.
		            _:v "some_pred"@[]} into ?a from ?b where {?s "foo"@[,] ?o};`,
		// Construct clause with badly formed reification clause.
		`construct {?s "predicate_1"@[] ?o1;
		            ?s "predicate_2"@[] ?o2} into ?a from ?b where {?s "old_predicate_1"@[,] ?o1.
									    ?s "old_predicate_2"@[,] ?o2};`,

	}
	p, err := NewParser(BQL())
	if err != nil {
		t.Errorf("grammar.NewParser: should have produced a valid BQL parser, %v", err)
	}
	for _, input := range table {
		if err := p.Parse(NewLLk(input, 1), &semantic.Statement{}); err == nil {
			t.Errorf("Parser.consume: failed to reject input %q with parsing error", input)
		}
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
	p, err := NewParser(SemanticBQL())
	if err != nil {
		t.Errorf("grammar.NewParser: should have produced a valid BQL parser, %v", err)
	}
	for _, entry := range table {
		st := &semantic.Statement{}
		if err := p.Parse(NewLLk(entry.query, 1), st); err != nil {
			t.Errorf("Parser.consume: failed to accept entry %q with error %v", entry, err)
		}
		if got, want := len(st.GraphNames()), entry.graphs; got != want {
			t.Errorf("Parser.consume: failed to collect right number of graphs for case %v; got %d, want %d", entry, got, want)
		}
		if got, want := len(st.Data()), entry.triples; got != want {
			t.Errorf("Parser.consume: failed to collect right number of triples for case %v; got %d, want %d", entry, got, want)
		}
	}
}

func TestAcceptQueryBySemanticParse(t *testing.T) {
	table := []string{
		// Test well type literals are accepted.
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
		`select ?s from ?g where{/_<foo> as ?s  ?p "id"@[?foo, ?bar] as ?o};`,
		// Test group by acceptance.
		`select ?s from ?g where{/_<foo> as ?s  ?p "id"@[?foo, ?bar] as ?o} group by ?s;`,
		`select count(?s) as ?a, sum(?o) as ?b, ?o as ?c from ?g where{?s ?p ?o} group by ?c;`,
		// Test order by acceptance.
		`select ?s from ?g where{/_<foo> as ?s  ?p "id"@[?foo, ?bar] as ?o} order by ?s;`,
		`select ?s as ?a, ?o as ?b, ?o as ?c from ?g where{?s ?p ?o} order by ?a ASC, ?b DESC;`,
		`select ?s as ?a, ?o as ?b, ?o as ?c from ?g where{?s ?p ?o} order by ?a ASC, ?b DESC, ?a ASC, ?b DESC, ?c;`,
	}
	p, err := NewParser(SemanticBQL())
	if err != nil {
		t.Errorf("grammar.NewParser: should have produced a valid BQL parser, %v", err)
	}
	for _, input := range table {
		if err := p.Parse(NewLLk(input, 1), &semantic.Statement{}); err != nil {
			t.Errorf("Parser.consume: failed to accept input %q with error %v", input, err)
		}
	}
}

func TestRejectByParseAndSemantic(t *testing.T) {
	table := []string{
		// Test wrong type literals are rejected.
		`select ?s from ?g where{?s ?p "true"^^type:int64};`,
		// Test invalid predicate bounds are rejected.
		`select ?s from ?b where{/_<foo> as ?s "id"@[2018-07-19T13:12:04.669618843-07:00, 2015-07-19T13:12:04.669618843-07:00] ?o};`,
		`select ?s from ?b where{/_<foo> as ?s  ?p "id"@[2019-07-19T13:12:04.669618843-07:00, 2015-07-19T13:12:04.669618843-07:00] as ?o};`,
		// Check the bindings on the projection exist on the graph clauses.
		`select ?foo from ?g where {?s ?p ?o};`,
		// Reject invalid group by.
		`select ?s from ?g where{/_<foo> as ?s  ?p "id"@[?foo, ?bar] as ?o} group by ?unknown;`,
		`select count(?s) as ?a, sum(?o) as ?b, ?o as ?c from ?g where{?s ?p ?o};`,
		`select count(?s) as ?a, sum(?o) as ?b, ?o as ?c from ?g where{?s ?p ?o} group by ?b;`,
		`select count(?s) as ?a, sum(?o) as ?b, ?o as ?c from ?g where{?s ?p ?o} group by ?a;`,
		// Reject order by acceptance.
		`select ?s from ?g where{/_<foo> as ?s  ?p "id"@[?foo, ?bar] as ?o} order by ?unknown_s;`,
		`select ?s as ?a, ?o as ?b, ?o as ?c from ?g where{?s ?p ?o} order by ?a ASC, ?a DESC;`,
		// Wrong limit literal.
		`select ?s as ?a, ?o as ?b, ?o as ?c from ?g where{?s ?p ?o} LIMIT "true"^^type:bool;`,
	}
	p, err := NewParser(SemanticBQL())
	if err != nil {
		t.Errorf("grammar.NewParser: should have produced a valid BQL parser, %v", err)
	}
	for _, entry := range table {
		st := &semantic.Statement{}
		if err := p.Parse(NewLLk(entry, 1), st); err == nil {
			t.Errorf("Parser.consume: failed to reject invalid semantic entry %q", entry)
		}
	}
}

func TestSemanticStatementGraphClausesLengthCorrectness(t *testing.T) {
	table := []struct {
		query string
		want  int
	}{
		{
			query: `SELECT ?o,?l FROM ?bbacl WHERE { ?o "some_id"@[,] ?l } LIMIT "20"^^type:int64;`,
			want:  1,
		},
		{
			query: `SELECT ?o,?l FROM ?bbacl WHERE { ?o "some_id"@[,] ?x . ?x "some_id"@[,] ?l } LIMIT "20"^^type:int64;`,
			want:  2,
		},
		{
			query: `SELECT ?o,?l FROM ?bbacl WHERE { ?o "some_id"@[,] ?x . ?x "some_id"@[,] ?y . ?y "some_id"@[,] ?l } LIMIT "20"^^type:int64;`,
			want:  3,
		},
	}
	p, err := NewParser(SemanticBQL())
	if err != nil {
		t.Errorf("grammar.NewParser: should have produced a valid BQL parser, %v", err)
	}
	for _, entry := range table {
		st := &semantic.Statement{}
		if err := p.Parse(NewLLk(entry.query, 1), st); err != nil {
			t.Errorf("Parser.consume: failed to accept valid semantic entry %q", entry.query)
		}
		if got, want := len(st.GraphPatternClauses()), entry.want; got != want {
			t.Errorf("Invalid number of graph pattern clauses for query %q; got %d, want %d; %v", entry.query, got, want, st.GraphPatternClauses())
		}
	}
}
