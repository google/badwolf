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

package planner

import (
	"bytes"
	"strings"
	"testing"

	"golang.org/x/net/context"

	"github.com/google/badwolf/bql/grammar"
	"github.com/google/badwolf/bql/semantic"
	"github.com/google/badwolf/io"
	"github.com/google/badwolf/storage"
	"github.com/google/badwolf/storage/memory"
	"github.com/google/badwolf/triple"
	"github.com/google/badwolf/triple/literal"
)

func insertTest(t *testing.T) {
	ctx := context.Background()
	bql := `insert data into ?a {/_<foo> "bar"@[] /_<foo> .
                               /_<foo> "bar"@[] "bar"@[1975-01-01T00:01:01.999999999Z] .
                               /_<foo> "bar"@[] "yeah"^^type:text};`
	p, err := grammar.NewParser(grammar.SemanticBQL())
	if err != nil {
		t.Errorf("grammar.NewParser: should have produced a valid BQL parser, %v", err)
	}
	stm := &semantic.Statement{}
	if err = p.Parse(grammar.NewLLk(bql, 1), stm); err != nil {
		t.Errorf("Parser.consume: failed to accept BQL %q with error %v", bql, err)
	}
	pln, err := New(ctx, memory.DefaultStore, stm, 0)
	if err != nil {
		t.Errorf("planner.New: should have not failed to create a plan using memory.DefaultStorage for statement %v with error %v", stm, err)
	}
	if _, err = pln.Excecute(ctx); err != nil {
		t.Errorf("planner.Execute: failed to execute insert plan with error %v", err)
	}
	g, err := memory.DefaultStore.Graph(ctx, "?a")
	if err != nil {
		t.Errorf("memory.DefaultStore.Graph(%q) should have not fail with error %v", "?a", err)
	}
	i := 0
	ts := make(chan *triple.Triple)
	go func() {
		if err := g.Triples(ctx, ts); err != nil {
			t.Error(err)
		}
	}()
	for _ = range ts {
		i++
	}
	if i != 3 {
		t.Errorf("g.Triples should have returned 3 triples, returned %d instead", i)
	}
}

func deleteTest(t *testing.T) {
	ctx := context.Background()
	bql := `delete data from ?a {/_<foo> "bar"@[] /_<foo> .
                               /_<foo> "bar"@[] "bar"@[1975-01-01T00:01:01.999999999Z] .
                               /_<foo> "bar"@[] "yeah"^^type:text};`
	p, err := grammar.NewParser(grammar.SemanticBQL())
	if err != nil {
		t.Errorf("grammar.NewParser: should have produced a valid BQL parser, %v", err)
	}
	stm := &semantic.Statement{}
	if err = p.Parse(grammar.NewLLk(bql, 1), stm); err != nil {
		t.Errorf("Parser.consume: failed to accept BQL %q with error %v", bql, err)
	}
	pln, err := New(ctx, memory.DefaultStore, stm, 0)
	if err != nil {
		t.Errorf("planner.New: should have not failed to create a plan using memory.DefaultStorage for statement %v with error %v", stm, err)
	}
	if _, err = pln.Excecute(ctx); err != nil {
		t.Errorf("planner.Execute: failed to execute insert plan with error %v", err)
	}
	g, err := memory.DefaultStore.Graph(ctx, "?a")
	if err != nil {
		t.Errorf("memory.DefaultStore.Graph(%q) should have not fail with error %v", "?a", err)
	}
	i := 0
	ts := make(chan *triple.Triple)
	if err := g.Triples(ctx, ts); err != nil {
		t.Error(err)
	}
	for _ = range ts {
		i++
	}
	if i != 0 {
		t.Errorf("g.Triples should have returned 3 triples, returned %d instead", i)
	}
}

func TestPlannerInsertDoesNotFail(t *testing.T) {
	ctx := context.Background()
	if _, err := memory.DefaultStore.NewGraph(ctx, "?a"); err != nil {
		t.Errorf("memory.DefaultStore.NewGraph(%q) should have not failed with error %v", "?a", err)
	}
	insertTest(t)
	if err := memory.DefaultStore.DeleteGraph(ctx, "?a"); err != nil {
		t.Errorf("memory.DefaultStore.DeleteGraph(%q) should have not failed with error %v", "?a", err)
	}
}

func TestPlannerDeleteDoesNotFail(t *testing.T) {
	ctx := context.Background()
	if _, err := memory.DefaultStore.NewGraph(ctx, "?a"); err != nil {
		t.Errorf("memory.DefaultStore.NewGraph(%q) should have not failed with error %v", "?a", err)
	}
	deleteTest(t)
	if err := memory.DefaultStore.DeleteGraph(ctx, "?a"); err != nil {
		t.Errorf("memory.DefaultStore.DeleteGraph(%q) should have not failed with error %v", "?a", err)
	}
}

func TestPlannerInsertDeleteDoesNotFail(t *testing.T) {
	ctx := context.Background()
	if _, err := memory.DefaultStore.NewGraph(ctx, "?a"); err != nil {
		t.Errorf("memory.DefaultStore.NewGraph(%q) should have not failed with error %v", "?a", err)
	}
	deleteTest(t)
	if err := memory.DefaultStore.DeleteGraph(ctx, "?a"); err != nil {
		t.Errorf("memory.DefaultStore.DeleteGraph(%q) should have not failed with error %v", "?a", err)
	}
}

func TestPlannerCreateGraph(t *testing.T) {
	ctx := context.Background()
	memory.DefaultStore.DeleteGraph(ctx, "?foo")
	memory.DefaultStore.DeleteGraph(ctx, "?bar")

	bql := `create graph ?foo, ?bar;`
	p, err := grammar.NewParser(grammar.SemanticBQL())
	if err != nil {
		t.Errorf("grammar.NewParser: should have produced a valid BQL parser, %v", err)
	}
	stm := &semantic.Statement{}
	if err = p.Parse(grammar.NewLLk(bql, 1), stm); err != nil {
		t.Errorf("Parser.consume: failed to accept BQL %q with error %v", bql, err)
	}
	pln, err := New(ctx, memory.DefaultStore, stm, 0)
	if err != nil {
		t.Errorf("planner.New: should have not failed to create a plan using memory.DefaultStorage for statement %v with error %v", stm, err)
	}
	if _, err := pln.Excecute(ctx); err != nil {
		t.Errorf("planner.Execute: failed to execute insert plan with error %v", err)
	}
	if _, err := memory.DefaultStore.Graph(ctx, "?foo"); err != nil {
		t.Errorf("planner.Execute: failed to create graph %q with error %v", "?foo", err)
	}
	if _, err := memory.DefaultStore.Graph(ctx, "?bar"); err != nil {
		t.Errorf("planner.Execute: failed to create graph %q with error %v", "?bar", err)
	}
}

func TestPlannerDropGraph(t *testing.T) {
	ctx := context.Background()
	memory.DefaultStore.DeleteGraph(ctx, "?foo")
	memory.DefaultStore.DeleteGraph(ctx, "?bar")
	memory.DefaultStore.NewGraph(ctx, "?foo")
	memory.DefaultStore.NewGraph(ctx, "?bar")

	bql := `drop graph ?foo, ?bar;`
	p, err := grammar.NewParser(grammar.SemanticBQL())
	if err != nil {
		t.Errorf("grammar.NewParser: should have produced a valid BQL parser")
	}
	stm := &semantic.Statement{}
	if err = p.Parse(grammar.NewLLk(bql, 1), stm); err != nil {
		t.Errorf("Parser.consume: failed to accept BQL %q with error %v", bql, err)
	}
	pln, err := New(ctx, memory.DefaultStore, stm, 0)
	if err != nil {
		t.Errorf("planner.New: should have not failed to create a plan using memory.DefaultStorage for statement %v with error %v", stm, err)
	}
	if _, err := pln.Excecute(ctx); err != nil {
		t.Errorf("planner.Execute: failed to execute insert plan with error %v", err)
	}
	if g, err := memory.DefaultStore.Graph(ctx, "?foo"); err == nil {
		t.Errorf("planner.Execute: failed to drop graph %q; returned %v", "?foo", g)
	}
	if g, err := memory.DefaultStore.Graph(ctx, "?bar"); err == nil {
		t.Errorf("planner.Execute: failed to drop graph %q; returned %v", "?bar", g)
	}
}

const (
	originalTriples = `/u<joe> "parent_of"@[] /u<mary>
  	 /u<joe> "parent_of"@[] /u<peter>
	   /u<peter> "parent_of"@[] /u<john>
	   /u<peter> "parent_of"@[] /u<eve>
		 /u<peter> "bought"@[2016-01-01T00:00:00-08:00] /c<mini>
		 /u<peter> "bought"@[2016-02-01T00:00:00-08:00] /c<model s>
		 /u<peter> "bought"@[2016-03-01T00:00:00-08:00] /c<model x>
		 /u<peter> "bought"@[2016-04-01T00:00:00-08:00] /c<model y>
		 /c<mini> "is_a"@[] /t<car>
		 /c<model s> "is_a"@[] /t<car>
		 /c<model x> "is_a"@[] /t<car>
		 /c<model y> "is_a"@[] /t<car>
		 /l<barcelona> "predicate"@[] "turned"@[2016-01-01T00:00:00-08:00]
		 /l<barcelona> "predicate"@[] "turned"@[2016-02-01T00:00:00-08:00]
		 /l<barcelona> "predicate"@[] "turned"@[2016-03-01T00:00:00-08:00]
		 /l<barcelona> "predicate"@[] "turned"@[2016-04-01T00:00:00-08:00]
	`

	tripleFromIssue40 = `/room<Hallway> "connects_to"@[] /room<Kitchen>
	   /room<Kitchen> "connects_to"@[] /room<Hallway>
		 /room<Kitchen> "connects_to"@[] /room<Bathroom>
		 /room<Kitchen> "connects_to"@[] /room<Bedroom>
		 /room<Bathroom> "connects_to"@[] /room<Kitchen>
		 /room<Bedroom> "connects_to"@[] /room<Kitchen>
		 /room<Bedroom> "connects_to"@[] /room<Fire Escape>
		 /room<Fire Escape> "connects_to"@[] /room<Kitchen>
		 /item/book<000> "in"@[2016-04-10T4:21:00.000000000Z] /room<Hallway>
		 /item/book<000> "in"@[2016-04-10T4:23:00.000000000Z] /room<Kitchen>
		 /item/book<000> "in"@[2016-04-10T4:25:00.000000000Z] /room<Bedroom>
	`

	testTriples = originalTriples + tripleFromIssue40
)

func populateTestStore(t *testing.T) storage.Store {
	s, ctx := memory.NewStore(), context.Background()
	g, err := s.NewGraph(ctx, "?test")
	if err != nil {
		t.Fatalf("memory.NewGraph failed to create \"?test\" with error %v", err)
	}
	b := bytes.NewBufferString(testTriples)
	if _, err := io.ReadIntoGraph(ctx, g, b, literal.DefaultBuilder()); err != nil {
		t.Fatalf("io.ReadIntoGraph failed to read test graph with error %v", err)
	}
	trpls := make(chan *triple.Triple)
	go func() {
		if err := g.Triples(ctx, trpls); err != nil {
			t.Fatal(err)
		}
	}()
	cnt := 0
	for _ = range trpls {
		cnt++
	}
	if got, want := cnt, len(strings.Split(testTriples, "\n"))-1; got != want {
		t.Fatalf("Failed to import all test triples; got %v, want %v", got, want)
	}
	return s
}

func TestPlannerQuery(t *testing.T) {
	ctx := context.Background()
	testTable := []struct {
		q    string
		nbs  int
		nrws int
	}{
		{
			q:    `select ?s, ?p, ?o from ?test where {?s ?p ?o};`,
			nbs:  3,
			nrws: len(strings.Split(testTriples, "\n")) - 1,
		},
		{
			q:    `select ?p, ?o from ?test where {/u<joe> ?p ?o};`,
			nbs:  2,
			nrws: 2,
		},
		{
			q:    `select ?s, ?p from ?test where {?s ?p /t<car>};`,
			nbs:  2,
			nrws: 4,
		},
		{
			q:    `select ?s, ?o from ?test where {?s "parent_of"@[] ?o};`,
			nbs:  2,
			nrws: 4,
		},
		{
			q:    `select ?s, ?p, ?o from ?test where {/u<joe> as ?s "parent_of"@[] as ?p /u<mary> as ?o};`,
			nbs:  3,
			nrws: 1,
		},
		{
			q:    `select ?s, ?p, ?o from ?test where {/u<unknown> as ?s "parent_of"@[] as ?p /u<mary> as ?o};`,
			nbs:  3,
			nrws: 0,
		},
		{
			q:    `select ?o from ?test where {/u<joe> "parent_of"@[] ?o};`,
			nbs:  1,
			nrws: 2,
		},
		{
			q:    `select ?p from ?test where {/u<joe> ?p /u<mary>};`,
			nbs:  1,
			nrws: 1,
		},
		{
			q:    `select ?s from ?test where {?s "is_a"@[] /t<car>};`,
			nbs:  1,
			nrws: 4,
		},
		{
			q:    `select ?o from ?test where {/u<joe> "parent_of"@[] ?o. ?o "parent_of"@[] /u<john>};`,
			nbs:  1,
			nrws: 1,
		},
		{
			q:    `select ?s, ?o from ?test where {/u<joe> "parent_of"@[] ?o. ?o "parent_of"@[] ?s};`,
			nbs:  2,
			nrws: 2,
		},
		{
			q:    `select ?s, ?p, ?o, ?k, ?l, ?m from ?test where {?s ?p ?o. ?k ?l ?m};`,
			nbs:  6,
			nrws: (len(strings.Split(testTriples, "\n")) - 1) * (len(strings.Split(testTriples, "\n")) - 1),
		},
		{
			q:    `select ?s, ?p, ?o, ?k, ?l from ?test where {?s ?p ?o. ?k ?l ?m};`,
			nbs:  5,
			nrws: (len(strings.Split(testTriples, "\n")) - 1) * (len(strings.Split(testTriples, "\n")) - 1),
		},
		{
			q:    `select ?s, ?p, ?o, ?k from ?test where {?s ?p ?o. ?k ?l ?m};`,
			nbs:  4,
			nrws: (len(strings.Split(testTriples, "\n")) - 1) * (len(strings.Split(testTriples, "\n")) - 1),
		},
		{
			q:    `select ?s, ?p, ?o from ?test where {?s ?p ?o. ?k ?l ?m};`,
			nbs:  3,
			nrws: (len(strings.Split(testTriples, "\n")) - 1) * (len(strings.Split(testTriples, "\n")) - 1),
		},
		{
			q:    `select ?s, ?p from ?test where {?s ?p ?o. ?k ?l ?m};`,
			nbs:  2,
			nrws: (len(strings.Split(testTriples, "\n")) - 1) * (len(strings.Split(testTriples, "\n")) - 1),
		},
		{
			q:    `select ?s from ?test where {?s ?p ?o. ?k ?l ?m};`,
			nbs:  1,
			nrws: (len(strings.Split(testTriples, "\n")) - 1) * (len(strings.Split(testTriples, "\n")) - 1),
		},
		{
			q:    `select ?o from ?test where {/u<peter> "bought"@[,] ?o};`,
			nbs:  1,
			nrws: 4,
		},
		{
			q:    `select ?o from ?test where {/u<peter> "bought"@[,2015-01-01T00:00:00-08:00] ?o};`,
			nbs:  1,
			nrws: 0,
		},
		{
			q:    `select ?o from ?test where {/u<peter> "bought"@[2017-01-01T00:00:00-08:00,] ?o};`,
			nbs:  1,
			nrws: 0,
		},
		{
			q:    `select ?o from ?test where {/u<peter> "bought"@[2015-01-01T00:00:00-08:00,2017-01-01T00:00:00-08:00] ?o};`,
			nbs:  1,
			nrws: 4,
		},
		{
			q:    `select ?o from ?test where {/l<barcelona> "predicate"@[] "turned"@[,] as ?o};`,
			nbs:  1,
			nrws: 4,
		},
		{
			q:    `select ?o from ?test where {/l<barcelona> "predicate"@[] "turned"@[,2015-01-01T00:00:00-08:00] as ?o};`,
			nbs:  1,
			nrws: 0,
		},
		{
			q:    `select ?o from ?test where {/l<barcelona> "predicate"@[] "turned"@[2017-01-01T00:00:00-08:00,] as ?o};`,
			nbs:  1,
			nrws: 0,
		},
		{
			q:    `select ?o from ?test where {/l<barcelona> "predicate"@[] "turned"@[2015-01-01T00:00:00-08:00,2017-01-01T00:00:00-08:00] as ?o};`,
			nbs:  1,
			nrws: 4,
		},
		{
			q:    `select ?grandparent, count(?name) as ?grandchildren from ?test where {/u<joe> as ?grandparent "parent_of"@[] ?offspring . ?offspring "parent_of"@[] ?name} group by ?grandparent;`,
			nbs:  2,
			nrws: 1,
		},
		{
			q:    `select ?grandparent, count(distinct ?name) as ?grandchildren from ?test where {/u<joe> as ?grandparent "parent_of"@[] ?offspring . ?offspring "parent_of"@[] ?name} group by ?grandparent;`,
			nbs:  2,
			nrws: 1,
		},
		{
			q:    `select ?s, ?p, ?o, ?k, ?l, ?m from ?test where {?s ?p ?o. ?k ?l ?m} order by ?s, ?p, ?o, ?k, ?l, ?m;`,
			nbs:  6,
			nrws: (len(strings.Split(testTriples, "\n")) - 1) * (len(strings.Split(testTriples, "\n")) - 1),
		},
		{
			q:    `select ?s, ?p, ?o, ?k, ?l, ?m from ?test where {?s ?p ?o. ?k ?l ?m} order by ?s, ?p, ?o, ?k, ?l, ?m  having not(?s = ?s);`,
			nbs:  6,
			nrws: 0,
		},
		{
			q:    `select ?o from ?test where {/l<barcelona> "predicate"@[] "turned"@[2015-01-01T00:00:00-08:00,2017-01-01T00:00:00-08:00] as ?o} LIMIT "2"^^type:int64;`,
			nbs:  1,
			nrws: 2,
		},
		{
			q:    `select ?o from ?test where {/u<peter> "bought"@[2015-01-01T00:00:00-08:00,2017-01-01T00:00:00-08:00] ?o} before ""@[2014-01-01T00:00:00-08:00];`,
			nbs:  1,
			nrws: 0,
		},
		{
			q:    `select ?o from ?test where {/u<peter> "bought"@[2015-01-01T00:00:00-08:00,2017-01-01T00:00:00-08:00] ?o} after ""@[2017-01-01T00:00:00-08:00];`,
			nbs:  1,
			nrws: 0,
		},
		{
			q:    `select ?o from ?test where {/u<peter> "bought"@[2015-01-01T00:00:00-08:00,2017-01-01T00:00:00-08:00] ?o} between ""@[2014-01-01T00:00:00-08:00], ""@[2017-01-01T00:00:00-08:00];`,
			nbs:  1,
			nrws: 4,
		},
		{
			q:    `SELECT ?grandparent, COUNT(?grandparent) AS ?number_of_grandchildren FROM ?test WHERE{ ?gp ID ?grandparent "parent_of"@[] ?c . ?c "parent_of"@[] ?gc ID ?gc } GROUP BY ?grandparent;`,
			nbs:  2,
			nrws: 1,
		},
		{ // Issue 40 (https://github.com/google/badwolf/issues/40)
			q:    `SELECT ?item, ?t FROM ?test WHERE {?item "in"@[?t] /room<Bedroom>};`,
			nbs:  2,
			nrws: 1,
		},
	}

	s := populateTestStore(t)
	p, err := grammar.NewParser(grammar.SemanticBQL())
	if err != nil {
		t.Fatalf("grammar.NewParser: should have produced a valid BQL parser with error %v", err)
	}
	for _, entry := range testTable {
		st := &semantic.Statement{}
		if err := p.Parse(grammar.NewLLk(entry.q, 1), st); err != nil {
			t.Errorf("Parser.consume: failed to parse query %q with error %v", entry.q, err)
		}
		plnr, err := New(ctx, s, st, 0)
		if err != nil {
			t.Errorf("planner.New failed to create a valid query plan with error %v", err)
		}
		tbl, err := plnr.Excecute(ctx)
		if err != nil {
			t.Errorf("planner.Excecute failed for query %q with error %v", entry.q, err)
		}
		if got, want := len(tbl.Bindings()), entry.nbs; got != want {
			t.Errorf("tbl.Bindings returned the wrong number of bindings for %q; got %d, want %d", entry.q, got, want)
		}
		if got, want := len(tbl.Rows()), entry.nrws; got != want {
			t.Errorf("planner.Excecute failed to return the expected number of rows for query %q; got %d want %d\nGot:\n%v\n", entry.q, got, want, tbl)
		}
	}
}
