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
	"context"
	"fmt"
	"strings"
	"testing"

	"github.com/google/badwolf/bql/grammar"
	"github.com/google/badwolf/bql/semantic"
	"github.com/google/badwolf/io"
	"github.com/google/badwolf/storage"
	"github.com/google/badwolf/storage/memory"
	"github.com/google/badwolf/triple"
	"github.com/google/badwolf/triple/literal"
)

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

	constructTestSrcTriples = `/person<A> "met"@[] /person<B>
		/person<B> "met"@[] /person<C>
		/person<C> "met"@[] /person<D>
		/person<A> "met_at"@[2016-04-10T4:25:00.000000000Z] /person<B>
		/person<B> "met_at"@[2016-04-10T4:25:00.000000000Z] /person<C>
		/city<A> "is_connected_to"@[] /city<B>
		/city<A> "is_connected_to"@[] /city<C>
		/city<B> "is_connected_to"@[] /city<D>
		/city<B> "is_connected_to"@[] /city<E>
		/city<C> "is_connected_to"@[] /city<D>
		`

	constructTestDestTriples = `/person<D> "met"@[] /person<E>
	`

	deconstructTestSrcTriples = `/person<A> "lives_in"@[] /city<A>
		/person<B> "lives_in"@[] /city<B>
		/person<C> "lives_in"@[] /city<A>
		/person<D> "lives_in"@[] /city<B>
		`

	deconstructTestDestTriples = `/person<A> "met"@[] /person<B>
		/person<B> "met"@[] /person<C>
		/person<C> "met"@[] /person<D>
		/person<D> "met"@[] /person<A>
		/person<A> "met"@[] /person<C>
		/person<B> "met"@[] /person<D>
		`

	testTriples = originalTriples + tripleFromIssue40
)

func insertAndDeleteTest(t *testing.T) {
	ctx := context.Background()

	// Testing insertion of triples.
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
	pln, err := New(ctx, memory.DefaultStore, stm, 0, 10, nil)
	if err != nil {
		t.Errorf("planner.New: should have not failed to create a plan using memory.DefaultStorage for statement %v with error %v", stm, err)
	}
	if _, err = pln.Execute(ctx); err != nil {
		t.Errorf("planner.Execute: failed to execute insert plan with error %v", err)
	}
	g, err := memory.DefaultStore.Graph(ctx, "?a")
	if err != nil {
		t.Errorf("memory.DefaultStore.Graph(%q) should have not fail with error %v", "?a", err)
	}
	i := 0
	ts := make(chan *triple.Triple)
	go func() {
		if err := g.Triples(ctx, storage.DefaultLookup, ts); err != nil {
			t.Error(err)
		}
	}()
	for range ts {
		i++
	}
	if i != 3 {
		t.Errorf("g.Triples should have returned 3 triples, returned %d instead", i)
	}

	// Testing deletion of triples.
	bql = `delete data from ?a {/_<foo> "bar"@[] /_<foo> .
	                      /_<foo> "bar"@[] "bar"@[1975-01-01T00:01:01.999999999Z] .
			      /_<foo> "bar"@[] "yeah"^^type:text};`
	p, err = grammar.NewParser(grammar.SemanticBQL())
	if err != nil {
		t.Errorf("grammar.NewParser: should have produced a valid BQL parser, %v", err)
	}
	stm = &semantic.Statement{}
	if err = p.Parse(grammar.NewLLk(bql, 1), stm); err != nil {
		t.Errorf("Parser.consume: failed to accept BQL %q with error %v", bql, err)
	}
	pln, err = New(ctx, memory.DefaultStore, stm, 0, 10, nil)
	if err != nil {
		t.Errorf("planner.New: should have not failed to create a plan using memory.DefaultStorage for statement %v with error %v", stm, err)
	}
	if _, err = pln.Execute(ctx); err != nil {
		t.Errorf("planner.Execute: failed to execute insert plan with error %v", err)
	}
	g, err = memory.DefaultStore.Graph(ctx, "?a")
	if err != nil {
		t.Errorf("memory.DefaultStore.Graph(%q) should have not fail with error %v", "?a", err)
	}
	i = 0
	ts = make(chan *triple.Triple)
	if err = g.Triples(ctx, storage.DefaultLookup, ts); err != nil {
		t.Error(err)
	}
	for range ts {
		i++
	}
	if i != 0 {
		t.Errorf("g.Triples should have returned 0 triples, returned %d instead", i)
	}
}

func TestPlannerInsertDeleteDoesNotFail(t *testing.T) {
	ctx := context.Background()
	if _, err := memory.DefaultStore.NewGraph(ctx, "?a"); err != nil {
		t.Errorf("memory.DefaultStore.NewGraph(%q) should have not failed with error %v", "?a", err)
	}
	insertAndDeleteTest(t)
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
	pln, err := New(ctx, memory.DefaultStore, stm, 0, 10, nil)
	if err != nil {
		t.Errorf("planner.New: should have not failed to create a plan using memory.DefaultStorage for statement %v with error %v", stm, err)
	}
	if _, err := pln.Execute(ctx); err != nil {
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
	pln, err := New(ctx, memory.DefaultStore, stm, 0, 10, nil)
	if err != nil {
		t.Errorf("planner.New: should have not failed to create a plan using memory.DefaultStorage for statement %v with error %v", stm, err)
	}
	if _, err := pln.Execute(ctx); err != nil {
		t.Errorf("planner.Execute: failed to execute insert plan with error %v", err)
	}
	if g, err := memory.DefaultStore.Graph(ctx, "?foo"); err == nil {
		t.Errorf("planner.Execute: failed to drop graph %q; returned %v", "?foo", g)
	}
	if g, err := memory.DefaultStore.Graph(ctx, "?bar"); err == nil {
		t.Errorf("planner.Execute: failed to drop graph %q; returned %v", "?bar", g)
	}
}

func populateStoreWithTriples(ctx context.Context, s storage.Store, gn string, triples string, tb testing.TB) {
	g, err := s.NewGraph(ctx, gn)
	if err != nil {
		tb.Fatalf("memory.NewGraph failed to create \"%v\" with error %v", gn, err)
	}
	b := bytes.NewBufferString(triples)
	if _, err := io.ReadIntoGraph(ctx, g, b, literal.DefaultBuilder()); err != nil {
		tb.Fatalf("io.ReadIntoGraph failed to read test graph with error %v", err)
	}
	trpls := make(chan *triple.Triple)
	go func() {
		if err := g.Triples(ctx, storage.DefaultLookup, trpls); err != nil {
			tb.Fatal(err)
		}
	}()
	cnt := 0
	for range trpls {
		cnt++
	}
	if got, want := cnt, len(strings.Split(triples, "\n"))-1; got != want {
		tb.Fatalf("Failed to import all test triples; got %v, want %v", got, want)
	}
}

func TestPlannerQuery(t *testing.T) {
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
			q:    `select ?s as ?s1, ?p as ?p1, ?o as ?o1 from ?test where {?s ?p ?o};`,
			nbs:  3,
			nrws: len(strings.Split(testTriples, "\n")) - 1,
		},
		{
			q:    `select ?p, ?o from ?test where {/u<joe> ?p ?o};`,
			nbs:  2,
			nrws: 2,
		},
		{
			q:    `select ?p as ?p1, ?o as ?o1 from ?test where {/u<joe> ?p ?o};`,
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
			q:    `select ?s as ?s1 from ?test where {?s "is_a"@[] /t<car>};`,
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
		{
			q:    `SHOW GRAPHS;`,
			nbs:  1,
			nrws: 1,
		},
		/*
			/c<model s> "is_a"@[] /t<car>
			/c<model x> "is_a"@[] /t<car>
			/c<model y> "is_a"@[] /t<car>
		*/
		// OPTIONAL clauses.
		{
			q:    `SELECT ?car FROM ?test WHERE { ?car "is_a"@[] /t<car> };`,
			nbs:  1,
			nrws: 4,
		},
		{
			q: `SELECT ?car
			    FROM ?test
			    WHERE {
				   /c<model s> as ?car "is_a"@[] /t<car>
				};`,
			nbs:  1,
			nrws: 1,
		},
		{
			q: `SELECT ?car
			    FROM ?test
			    WHERE {
				   ?car "is_a"@[] /t<car> .
				   /c<model z> as ?car "is_a"@[] /t<car>
				};`,
			nbs:  1,
			nrws: 0,
		},
		{
			q: `SELECT ?car
				    FROM ?test
				    WHERE {
					   ?car "is_a"@[] /t<car> .
					   OPTIONAL { /c<model O> "is_a"@[] /t<car> }
					};`,
			nbs:  1,
			nrws: 4,
		},
		{
			q: `SELECT ?car
				    FROM ?test
				    WHERE {
					   ?car "is_a"@[] /t<car> .
					   OPTIONAL { ?car "is_a"@[] /t<car> }
					};`,
			nbs:  1,
			nrws: 4,
		},
		{
			q: `SELECT ?cars, ?type
				    FROM ?test
				    WHERE {
					   ?cars "is_a"@[] /t<car> .
					   OPTIONAL { ?cars "is_a"@[] ?type }
					};`,
			nbs:  2,
			nrws: 4,
		},
	}

	s, ctx := memory.NewStore(), context.Background()
	populateStoreWithTriples(ctx, s, "?test", testTriples, t)
	p, err := grammar.NewParser(grammar.SemanticBQL())
	if err != nil {
		t.Fatalf("grammar.NewParser: should have produced a valid BQL parser with error %v", err)
	}
	for _, entry := range testTable {
		st := &semantic.Statement{}
		if err := p.Parse(grammar.NewLLk(entry.q, 1), st); err != nil {
			t.Errorf("Parser.consume: failed to parse query %q with error %v", entry.q, err)
		}
		plnr, err := New(ctx, s, st, 0, 10, nil)
		if err != nil {
			t.Errorf("planner.New failed to create a valid query plan with error %v", err)
		}
		tbl, err := plnr.Execute(ctx)
		if err != nil {
			t.Errorf("planner.Execute failed for query %q with error %v", entry.q, err)
			continue
		}
		if got, want := len(tbl.Bindings()), entry.nbs; got != want {
			t.Errorf("tbl.Bindings returned the wrong number of bindings for %q; got %d, want %d", entry.q, got, want)
		}
		if got, want := len(tbl.Rows()), entry.nrws; got != want {
			t.Errorf("planner.Execute failed to return the expected number of rows for query %q; got %d want %d\nGot:\n%v\n", entry.q, got, want, tbl)
		}
	}
}

func TestPlannerConstructAddsCorrectNumberofTriples(t *testing.T) {
	sts, dts := len(strings.Split(constructTestSrcTriples, "\n"))-1, len(strings.Split(constructTestDestTriples, "\n"))-1
	testTable := []struct {
		s    string
		trps int
	}{
		{
			s: `construct {?s ?p ?o}
			    into ?dest
			    from ?src
			    where {?s ?p ?o};`,
			trps: sts + dts,
		},
		{
			s: `construct {?s "met"@[] ?o; "location"@[] /city<New York>}
			    into ?dest
			    from ?src
			    where {?s "met"@[] ?o};`,
			// 3 matching triples * 4 new triples per matched triple due to reification + 1 triple in dest graph.
			trps: 3*4 + dts,
		},
		{
			s: `construct {?s "met"@[] ?o; "location"@[] /city<New York>;
			                               "outcome"@[] "good"^^type:text }
			    into ?dest
			    from ?src
			    where {?s "met"@[] ?o};`,
			// 3 matching triples * 5 new triples per matched triple due to reification + 1 triple in dest graph.
			trps: 3*5 + dts,
		},
		{
			s: `construct {?s "met"@[?t] ?o; "location"@[] /city<New York>;
			                                 "outcome"@[] "good"^^type:text .
			               ?s "connected_to"@[] ?o}
			    into ?dest
			    from ?src
			    where {?s "met"@[] ?o.
			           ?s "met_at"@[?t] ?o};`,
			// 2 matching triples * (5 new triples due to reification + 1 explicitly constructed triple per matched triple) +
			// 1 triple in dest graph.
			trps: 2*6 + dts,
		},
		{
			s: `construct {?s "met"@[?t] ?o; "location"@[] /city<New York>;
			                                 "outcome"@[] "good"^^type:text .
			               ?s "connected_to"@[] ?o; "at"@[?t] /city<New York> }
			    into ?dest
			    from ?src
			    where {?s "met"@[] ?o.
			           ?s "met_at"@[?t] ?o};`,
			// 2 matching triples * 9 new triples due to reification + 1 triple in dest graph.
			trps: 2*9 + dts,
		},
		{
			s: `construct {?d2 "is_2_hops_from"@[] ?s1 }
			    into ?dest
			    from ?src
			    where {?s1 "is_connected_to"@[] ?d1.
			           ?d1 "is_connected_to"@[] ?d2};`,
			// 2 new triples (/city<A> "is_2_hops_from"@[] /city<D>, /city<A> "is_2_hops_from"@[] /city<E>) +  1 triple in dest graph.
			trps: 3,
		},
	}
	p, err := grammar.NewParser(grammar.SemanticBQL())
	if err != nil {
		t.Errorf("grammar.NewParser: should have produced a valid BQL parser, %v", err)
	}
	for _, entry := range testTable {

		s, ctx := memory.NewStore(), context.Background()
		populateStoreWithTriples(ctx, s, "?src", constructTestSrcTriples, t)
		populateStoreWithTriples(ctx, s, "?dest", constructTestDestTriples, t)

		st := &semantic.Statement{}
		if err := p.Parse(grammar.NewLLk(entry.s, 1), st); err != nil {
			t.Errorf("Parser.consume: failed to parse query %q with error %v", entry.s, err)
		}
		plnr, err := New(ctx, s, st, 0, 10, nil)
		if err != nil {
			t.Errorf("planner.New failed to create a valid query plan with error %v", err)
		}
		_, err = plnr.Execute(ctx)
		if err != nil {
			t.Errorf("planner.Execute failed for query %q with error %v", entry.s, err)
			continue
		}

		g, err := s.Graph(ctx, "?dest")
		if err != nil {
			t.Errorf("memory.DefaultStore.Graph(%q) should have not fail with error %v", "?test", err)
		}

		i := 0
		ts := make(chan *triple.Triple)
		go func() {
			if err := g.Triples(ctx, storage.DefaultLookup, ts); err != nil {
				t.Error(err)
			}
		}()
		for range ts {
			i++
		}
		if i != entry.trps {
			t.Errorf("g.Triples should have returned %v triples, returned %v instead", entry.trps, i)
		}
	}

}

func TestPlannerConstructAddsCorrectTriples(t *testing.T) {
	bql := `construct {?s "met"@[?t] ?o; "location"@[] /city<New York>;
	                                     "outcome"@[] "good"^^type:text.
	                   ?s "connected_to"@[] ?o }
	        into ?dest
	        from ?src
	        where {?s "met"@[] ?o.
		       ?s "met_at"@[?t] ?o};`
	p, err := grammar.NewParser(grammar.SemanticBQL())
	if err != nil {
		t.Errorf("grammar.NewParser: should have produced a valid BQL parser, %v", err)
	}
	s, ctx := memory.NewStore(), context.Background()
	populateStoreWithTriples(ctx, s, "?src", constructTestSrcTriples, t)
	populateStoreWithTriples(ctx, s, "?dest", "", t)

	st := &semantic.Statement{}
	if err := p.Parse(grammar.NewLLk(bql, 1), st); err != nil {
		t.Errorf("Parser.consume: failed to parse query %q with error %v", bql, err)
	}
	plnr, err := New(ctx, s, st, 0, 10, nil)
	if err != nil {
		t.Errorf("planner.New failed to create a valid query plan with error %v", err)
	}
	_, err = plnr.Execute(ctx)
	if err != nil {
		t.Errorf("planner.Execute failed for query %q with error %v", bql, err)
	}

	g, err := s.Graph(ctx, "?dest")
	if err != nil {
		t.Errorf("memory.DefaultStore.Graph(%q) should have not fail with error %v", "?test", err)
	}

	ts := make(chan *triple.Triple)
	go func() {
		if err := g.Triples(ctx, storage.DefaultLookup, ts); err != nil {
			t.Error(err)
		}
	}()

	bnm := make(map[string]map[string]bool)
	bns := make(map[string]string)
	bna := map[string]bool{
		"/_<b1>": true,
		"/_<b2>": true,
	}
	dtm := map[string]bool{
		fmt.Sprintf("%s\t%s\t%s", `/person<A>`, `"connected_to"@[]`, `/person<B>`):                                 false,
		fmt.Sprintf("%s\t%s\t%s", `/person<B>`, `"connected_to"@[]`, `/person<C>`):                                 false,
		fmt.Sprintf("%s\t%s\t%s", `/_<b1>`, `"_subject"@[2016-04-10T04:25:00Z]`, `/person<A>`):                     false,
		fmt.Sprintf("%s\t%s\t%s", `/_<b1>`, `"_predicate"@[2016-04-10T04:25:00Z]`, `"met"@[2016-04-10T04:25:00Z]`): false,
		fmt.Sprintf("%s\t%s\t%s", `/_<b1>`, `"_object"@[2016-04-10T04:25:00Z]`, `/person<B>`):                      false,
		fmt.Sprintf("%s\t%s\t%s", `/_<b1>`, `"location"@[]`, `/city<New York>`):                                    false,
		fmt.Sprintf("%s\t%s\t%s", `/_<b1>`, `"outcome"@[]`, `"good"^^type:text`):                                   false,
		fmt.Sprintf("%s\t%s\t%s", `/_<b2>`, `"_subject"@[2016-04-10T04:25:00Z]`, `/person<B>`):                     false,
		fmt.Sprintf("%s\t%s\t%s", `/_<b2>`, `"_predicate"@[2016-04-10T04:25:00Z]`, `"met"@[2016-04-10T04:25:00Z]`): false,
		fmt.Sprintf("%s\t%s\t%s", `/_<b2>`, `"_object"@[2016-04-10T04:25:00Z]`, `/person<C>`):                      false,
		fmt.Sprintf("%s\t%s\t%s", `/_<b2>`, `"location"@[]`, `/city<New York>`):                                    false,
		fmt.Sprintf("%s\t%s\t%s", `/_<b2>`, `"outcome"@[]`, `"good"^^type:text`):                                   false,
	}

	// First, we map each blank node generated to a potential blank node placeholder (such as b1 or b2.)
	sts := []*triple.Triple{}
	for elem := range ts {
		sts = append(sts, elem)
		if elem.Subject().Type().String() == "/_" {
			for k := range dtm {
				trp, err := triple.Parse(k, literal.DefaultBuilder())
				if err != nil {
					t.Errorf("Unable to parse triple: %v with error %v", k, err)
				}
				if trp.Subject().Type().String() == "/_" &&
					trp.Predicate().String() == elem.Predicate().String() &&
					trp.Object().String() == elem.Object().String() {
					if mp, ok := bnm[elem.Subject().String()]; !ok {
						bnm[elem.Subject().String()] = map[string]bool{
							trp.Subject().String(): true,
						}
					} else {
						mp[trp.Subject().String()] = true
					}
				}
			}

		}
	}

	// Then, we decide which place holder blank nodes can be used to substiute for a given blank node
	// by substituting the place holder in every triple where the given blank node is the subject and
	// checking if the triple exists in the map of expected triples.
	for _, t := range sts {
		if t.Subject().Type().String() == "/_" {
			for bn := range bnm[t.Subject().String()] {
				rep := fmt.Sprintf("%s\t%s\t%s", bn, t.Predicate().String(), t.Object().String())
				if _, ok := dtm[rep]; !ok {
					bnm[t.Subject().String()][bn] = false
				}
			}
		}
	}

	// Finally, we assign a blank node to a place-holder blank node, if the place-holder blank node is
	// not used to substitute any other blank node.
	for k, v := range bnm {
		for bn, p := range v {
			if p && bna[bn] {
				bns[k] = bn
				bna[bn] = false
				break
			}
		}
	}
	if len(sts) != len(dtm) {
		t.Errorf("g.Triples should have returned %v triples, returned %v instead", len(dtm), len(sts))
	}
	for _, elem := range sts {
		if elem.Subject().Type().String() == "/_" {
			if val, ok := bns[elem.Subject().String()]; ok {
				// Substitute the blank node with the mapped place holder blank node id.
				rep := fmt.Sprintf("%s\t%s\t%s", val, elem.Predicate().String(), elem.Object().String())
				if _, ok := dtm[rep]; !ok {
					t.Errorf("unexpected triple: %v added to graph", elem)
				}
				dtm[rep] = true
			} else {
				t.Errorf("unexpected triple: %v added to graph", elem)
			}
		} else {
			sr := elem.String()
			if _, ok := dtm[sr]; !ok {
				t.Errorf("unexpected triple: %v added to graph", elem)
			}
			dtm[sr] = true
		}
	}
	for k, v := range dtm {
		if v == false {
			t.Errorf("g.Triples did not return triple: %v", k)
		}
	}
}

func TestPlannerDeconstructRemovesCorrectTriples(t *testing.T) {
	testTable := []struct {
		s    string
		trps []string
	}{
		{
			s: `deconstruct {?p1 "met"@[] ?p2}
			    in ?dest
			    from ?src
			    where {?p1 "lives_in"@[] /city<A>.
				   ?p2 "lives_in"@[] /city<B>};`,
			trps: []string{fmt.Sprintf("%s\t%s\t%s", `/person<B>`, `"met"@[]`, `/person<C>`),
				fmt.Sprintf("%s\t%s\t%s", `/person<D>`, `"met"@[]`, `/person<A>`),
				fmt.Sprintf("%s\t%s\t%s", `/person<A>`, `"met"@[]`, `/person<C>`),
				fmt.Sprintf("%s\t%s\t%s", `/person<B>`, `"met"@[]`, `/person<D>`)},
		},
		{
			s: `deconstruct {?p1 "met"@[] ?p2.
		                         ?p2 "met"@[] ?p1}
			    in ?dest
			    from ?src
			    where {?p1 "lives_in"@[] /city<A>.
				   ?p2 "lives_in"@[] /city<B>};`,
			trps: []string{fmt.Sprintf("%s\t%s\t%s", `/person<A>`, `"met"@[]`, `/person<C>`),
				fmt.Sprintf("%s\t%s\t%s", `/person<B>`, `"met"@[]`, `/person<D>`)},
		},
	}
	p, err := grammar.NewParser(grammar.SemanticBQL())
	if err != nil {
		t.Errorf("grammar.NewParser: should have produced a valid BQL parser, %v", err)
	}
	for _, entry := range testTable {

		s, ctx := memory.NewStore(), context.Background()
		populateStoreWithTriples(ctx, s, "?src", deconstructTestSrcTriples, t)
		populateStoreWithTriples(ctx, s, "?dest", deconstructTestDestTriples, t)

		st := &semantic.Statement{}
		if err := p.Parse(grammar.NewLLk(entry.s, 1), st); err != nil {
			t.Errorf("Parser.consume: failed to parse query %q with error %v", entry.s, err)
		}
		plnr, err := New(ctx, s, st, 0, 10, nil)
		if err != nil {
			t.Errorf("planner.New failed to create a valid query plan with error %v", err)
		}
		_, err = plnr.Execute(ctx)
		if err != nil {
			t.Errorf("planner.Execute failed for query %q with error %v", entry.s, err)
			continue
		}

		g, err := s.Graph(ctx, "?dest")
		if err != nil {
			t.Errorf("memory.DefaultStore.Graph(%q) should have not fail with error %v", "?test", err)
		}

		ts := make(chan *triple.Triple)
		go func() {
			if err := g.Triples(ctx, storage.DefaultLookup, ts); err != nil {
				t.Error(err)
			}
		}()

		dt := make(map[string]bool)
		for _, trp := range entry.trps {
			dt[trp] = false
		}

		i := 0
		for trp := range ts {
			if val, ok := dt[trp.String()]; ok {
				if !val {
					i++
				}
				dt[trp.String()] = true
			} else {
				t.Errorf("unexpected triple: %v added to graph", trp)
			}
		}
		if i != len(entry.trps) {
			t.Errorf("g.Triples did not return some of the triples.")
		}
	}

}

func TestTreeTraversalToRoot(t *testing.T) {
	// Graph traversal data.
	traversalTriples := `/person<Gavin Belson>  "born in"@[]    /city<Springfield>
		/person<Gavin Belson>  "parent of"@[]  /person<Peter Belson>
		/person<Gavin Belson>  "parent of"@[]  /person<Mary Belson>
		/person<Mary Belson>   "parent of"@[]  /person<Amy Schumer>
		/person<Mary Belson>   "parent of"@[]  /person<Joe Schumer>`

	traversalQuery := `SELECT ?grandparent
		                 FROM ?test
										 WHERE {
										   ?s "parent of"@[] /person<Amy Schumer> .
											 ?grandparent "parent of"@[] ?s
										 };`

	// Load traversing data
	s, ctx := memory.NewStore(), context.Background()
	g, gErr := s.NewGraph(ctx, "?test")
	if gErr != nil {
		t.Fatalf("memory.NewGraph failed to create \"?test\" with error %v", gErr)
	}
	b := bytes.NewBufferString(traversalTriples)
	if _, err := io.ReadIntoGraph(ctx, g, b, literal.DefaultBuilder()); err != nil {
		t.Fatalf("io.ReadIntoGraph failed to read test graph with error %v", err)
	}
	p, pErr := grammar.NewParser(grammar.SemanticBQL())
	if pErr != nil {
		t.Fatalf("grammar.NewParser: should have produced a valid BQL parser with error %v", pErr)
	}
	st := &semantic.Statement{}
	if err := p.Parse(grammar.NewLLk(traversalQuery, 1), st); err != nil {
		t.Errorf("Parser.consume: failed to parse query %q with error %v", traversalQuery, err)
	}
	plnr, err := New(ctx, s, st, 0, 10, nil)
	if err != nil {
		t.Errorf("planner.New failed to create a valid query plan with error %v", err)
	}
	tbl, err := plnr.Execute(ctx)
	if err != nil {
		t.Errorf("planner.Execute failed for query %q with error %v", traversalQuery, err)
	}
	if got, want := len(tbl.Bindings()), 1; got != want {
		t.Errorf("tbl.Bindings returned the wrong number of bindings for %q; got %d, want %d", traversalQuery, got, want)
	}
	if got, want := len(tbl.Rows()), 1; got != want {
		t.Errorf("planner.Execute failed to return the expected number of rows for query %q; got %d want %d\nGot:\n%v\n", traversalQuery, got, want, tbl)
	}
}

func TestChaining(t *testing.T) {
	// Graph traversal data.
	traversalTriples := `/u<joe> "parent_of"@[] /u<mary>
		/u<joe> "parent_of"@[] /u<peter>
		/u<peter> "parent_of"@[] /u<john>
		/u<peter> "parent_of"@[] /u<eve>`

	traversalQuery := `SELECT ?o FROM ?test
	                   WHERE {
	                       /u<joe> "parent_of"@[] ?o .
		                   ?o "parent_of"@[] /u<john>
	                   };`

	// Load traversing data
	s, ctx := memory.NewStore(), context.Background()
	g, gErr := s.NewGraph(ctx, "?test")
	if gErr != nil {
		t.Fatalf("memory.NewGraph failed to create \"?test\" with error %v", gErr)
	}
	b := bytes.NewBufferString(traversalTriples)
	if _, err := io.ReadIntoGraph(ctx, g, b, literal.DefaultBuilder()); err != nil {
		t.Fatalf("io.ReadIntoGraph failed to read test graph with error %v", err)
	}
	p, pErr := grammar.NewParser(grammar.SemanticBQL())
	if pErr != nil {
		t.Fatalf("grammar.NewParser: should have produced a valid BQL parser with error %v", pErr)
	}
	st := &semantic.Statement{}
	if err := p.Parse(grammar.NewLLk(traversalQuery, 1), st); err != nil {
		t.Errorf("Parser.consume: failed to parse query %q with error %v", traversalQuery, err)
	}
	plnr, err := New(ctx, s, st, 0, 10, nil)
	if err != nil {
		t.Errorf("planner.New failed to create a valid query plan with error %v", err)
	}
	tbl, err := plnr.Execute(ctx)
	if err != nil {
		t.Errorf("planner.Execute failed for query %q with error %v", traversalQuery, err)
	}
	if got, want := len(tbl.Bindings()), 1; got != want {
		t.Errorf("tbl.Bindings returned the wrong number of bindings for %q; got %d, want %d", traversalQuery, got, want)
	}
	if got, want := len(tbl.Rows()), 1; got != want {
		t.Errorf("planner.Execute failed to return the expected number of rows for query %q; got %d want %d\nGot:\n%v\n", traversalQuery, got, want, tbl)
	}
}

func BenchmarkChaining(b *testing.B) {
	// Graph traversal data.
	traversalTriples := `/u<joe> "parent_of"@[] /u<mary>
		/u<joe> "parent_of"@[] /u<peter>
		/u<peter> "parent_of"@[] /u<john>
		/u<peter> "parent_of"@[] /u<eve>`

	traversalQuery := `SELECT ?o FROM ?test
	                   WHERE {
	                       /u<joe> "parent_of"@[] ?o .
		                   ?o "parent_of"@[] /u<john>
	                   };`

	// Load traversing data
	s, ctx := memory.NewStore(), context.Background()
	g, gErr := s.NewGraph(ctx, "?test")
	if gErr != nil {
		b.Fatalf("memory.NewGraph failed to create \"?test\" with error %v", gErr)
	}
	buf := bytes.NewBufferString(traversalTriples)
	if _, err := io.ReadIntoGraph(ctx, g, buf, literal.DefaultBuilder()); err != nil {
		b.Fatalf("io.ReadIntoGraph failed to read test graph with error %v", err)
	}
	for n := 0; n < b.N; n++ {
		p, pErr := grammar.NewParser(grammar.SemanticBQL())
		if pErr != nil {
			b.Fatalf("grammar.NewParser: should have produced a valid BQL parser with error %v", pErr)
		}
		st := &semantic.Statement{}
		if err := p.Parse(grammar.NewLLk(traversalQuery, 1), st); err != nil {
			b.Errorf("Parser.consume: failed to parse query %q with error %v", traversalQuery, err)
		}
		plnr, err := New(ctx, s, st, 0, 10, nil)
		if err != nil {
			b.Errorf("planner.New failed to create a valid query plan with error %v", err)
		}
		tbl, err := plnr.Execute(ctx)
		if err != nil {
			b.Errorf("planner.Execute failed for query %q with error %v", traversalQuery, err)
		}
		if got, want := len(tbl.Bindings()), 1; got != want {
			b.Errorf("tbl.Bindings returned the wrong number of bindings for %q; got %d, want %d", traversalQuery, got, want)
		}
		if got, want := len(tbl.Rows()), 1; got != want {
			b.Errorf("planner.Execute failed to return the expected number of rows for query %q; got %d want %d\nGot:\n%v\n", traversalQuery, got, want, tbl)
		}
	}
}

// Test to validate https://github.com/google/badwolf/issues/70
func TestReificationResolutionIssue70(t *testing.T) {
	// Graph traversal data.
	issue70Triples := `/_<c175b457-e6d6-4ce3-8312-674353815720>	"_predicate"@[]	"/some/immutable/id"@[]
		/_<c175b457-e6d6-4ce3-8312-674353815720>	"_owner"@[2017-05-23T16:41:12.187373-07:00]	/gid<0x9>
		/_<c175b457-e6d6-4ce3-8312-674353815720>	"_subject"@[]	/aid</some/subject/id>
		/_<c175b457-e6d6-4ce3-8312-674353815720>	"_object"@[]	/aid</some/object/id>
		/_<cd8bae87-be96-41af-b1a8-27df990c9825>	"_object"@[2017-05-23T16:41:12.187373-07:00]	/aid</some/object/id>
		/_<cd8bae87-be96-41af-b1a8-27df990c9825>	"_owner"@[2017-05-23T16:41:12.187373-07:00]	/gid<0x6>
		/_<cd8bae87-be96-41af-b1a8-27df990c9825>	"_predicate"@[2017-05-23T16:41:12.187373-07:00]	"/some/temporal/id"@[2017-05-23T16:41:12.187373-07:00]
		/_<cd8bae87-be96-41af-b1a8-27df990c9825>	"_subject"@[2017-05-23T16:41:12.187373-07:00]	/aid</some/subject/id>
		/aid</some/subject/id>	"/some/temporal/id"@[2017-05-23T16:41:12.187373-07:00]	/aid</some/object/id>
		/aid</some/subject/id>	"/some/immutable/id"@[]	/aid</some/object/id>
		/aid</some/subject/id>	"/some/ownerless_temporal/id"@[2017-05-23T16:41:12.187373-07:00]	/aid</some/object/id>`

	query := `
		SELECT ?bn, ?p
		FROM ?test
		WHERE {
			?bn "_subject"@[,]   /aid</some/subject/id>.
			?bn "_predicate"@[,] ?p .
			?bn "_object"@[,]    /aid</some/object/id>
		};`

	// Load traversing data
	s, ctx := memory.NewStore(), context.Background()
	g, gErr := s.NewGraph(ctx, "?test")
	if gErr != nil {
		t.Fatalf("memory.NewGraph failed to create \"?test\" with error %v", gErr)
	}
	b := bytes.NewBufferString(issue70Triples)
	if _, err := io.ReadIntoGraph(ctx, g, b, literal.DefaultBuilder()); err != nil {
		t.Fatalf("io.ReadIntoGraph failed to read test graph with error %v", err)
	}
	p, pErr := grammar.NewParser(grammar.SemanticBQL())
	if pErr != nil {
		t.Fatalf("grammar.NewParser: should have produced a valid BQL parser with error %v", pErr)
	}
	st := &semantic.Statement{}
	if err := p.Parse(grammar.NewLLk(query, 1), st); err != nil {
		t.Errorf("Parser.consume: failed to parse query %q with error %v", query, err)
	}
	plnr, err := New(ctx, s, st, 0, 10, nil)
	if err != nil {
		t.Errorf("planner.New failed to create a valid query plan with error %v", err)
	}
	tbl, err := plnr.Execute(ctx)
	if err != nil {
		t.Fatalf("planner.Execute failed for query %q with error %v", query, err)
	}
	if got, want := len(tbl.Bindings()), 2; got != want {
		t.Errorf("tbl.Bindings returned the wrong number of bindings for %q; got %d, want %d", query, got, want)
	}
	if got, want := len(tbl.Rows()), 1; got != want {
		t.Errorf("planner.Execute failed to return the expected number of rows for query %q; got %d want %d\nGot:\n%v\n", query, got, want, tbl)
	}
}

// benchmarkQuery is a helper function that runs a specified query on the testing data set for benchmarking purposes.
func benchmarkQuery(query string, b *testing.B) {
	s, ctx := memory.NewStore(), context.Background()
	populateStoreWithTriples(ctx, s, "?test", testTriples, b)
	p, err := grammar.NewParser(grammar.SemanticBQL())
	if err != nil {
		b.Fatalf("grammar.NewParser: should have produced a valid BQL parser with error %v", err)
	}
	b.ResetTimer()
	for n := 0; n < b.N; n++ {
		st := &semantic.Statement{}
		if err := p.Parse(grammar.NewLLk(query, 1), st); err != nil {
			b.Errorf("Parser.consume: failed to parse query %q with error %v", query, err)
		}
		plnr, err := New(ctx, s, st, 0, 10, nil)
		if err != nil {
			b.Errorf("planner.New failed to create a valid query plan with error %v", err)
		}
		_, err = plnr.Execute(ctx)
		if err != nil {
			b.Errorf("planner.Execute failed for query %q with error %v", query, err)
		}
	}
}

// These benchmark tests are used to observe the difference in speed between queries using the "as" keyword as opposed
// to queries that do not.
func BenchmarkReg1(b *testing.B) {
	benchmarkQuery(`select ?p, ?o as ?o1 from ?test where {/u<joe> ?p ?o};`, b)
}

func BenchmarkAs1(b *testing.B) {
	benchmarkQuery(`select ?p as ?p1, ?o as ?o1 from ?test where {/u<joe> ?p ?o};`, b)
}

func BenchmarkReg2(b *testing.B) {
	benchmarkQuery(`select ?s, ?p, ?o from ?test where {?s ?p ?o};`, b)
}

func BenchmarkAs2(b *testing.B) {
	benchmarkQuery(`select ?s as ?s1, ?p as ?p1, ?o as ?o1 from ?test where {?s ?p ?o};`, b)
}
