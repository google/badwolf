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
		/u<paul> "bought"@[2016-01-01T00:00:00-08:00] /c<model n>
		/u<paul> "bought"@[2016-04-01T00:00:00-08:00] /c<model r>
		/c<mini> "is_a"@[] /t<car>
		/c<model s> "is_a"@[] /t<car>
		/c<model x> "is_a"@[] /t<car>
		/c<model y> "is_a"@[] /t<car>
		/l<barcelona> "predicate"@[] "turned"@[2016-01-01T00:00:00-08:00]
		/l<barcelona> "predicate"@[] "turned"@[2016-02-01T00:00:00-08:00]
		/l<barcelona> "predicate"@[] "turned"@[2016-03-01T00:00:00-08:00]
		/l<barcelona> "predicate"@[] "turned"@[2016-04-01T00:00:00-08:00]
		/l<barcelona> "predicate"@[] "immutable_predicate"@[]
		/l<paris> "predicate"@[] "turned"@[2016-04-01T00:00:00-08:00]
		/u<alice> "height_cm"@[] "174"^^type:int64
		/u<alice> "tag"@[] "abc"^^type:text
		/u<bob> "height_cm"@[] "151"^^type:int64
		/u<charlie> "height_cm"@[] "174"^^type:int64
		/u<delta> "height_cm"@[] "174"^^type:int64
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
		t.Errorf("grammar.NewParser should have produced a valid BQL parser but got error: %v", err)
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
		t.Errorf("grammar.NewParser should have produced a valid BQL parser but got error: %v", err)
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
		t.Errorf("grammar.NewParser should have produced a valid BQL parser but got error: %v", err)
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
		t.Errorf("grammar.NewParser should have produced a valid BQL parser but got error: %v", err)
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
		q         string
		nBindings int
		nRows     int
	}{
		{
			q:         `select ?s, ?p, ?o from ?test where {?s ?p ?o};`,
			nBindings: 3,
			nRows:     len(strings.Split(testTriples, "\n")) - 1,
		},
		{
			q:         `select ?s as ?s1, ?p as ?p1, ?o as ?o1 from ?test where {?s ?p ?o};`,
			nBindings: 3,
			nRows:     len(strings.Split(testTriples, "\n")) - 1,
		},
		{
			q:         `select ?p, ?o from ?test where {/u<joe> ?p ?o};`,
			nBindings: 2,
			nRows:     2,
		},
		{
			q:         `select ?p as ?p1, ?o as ?o1 from ?test where {/u<joe> ?p ?o};`,
			nBindings: 2,
			nRows:     2,
		},
		{
			q:         `select ?s, ?p from ?test where {?s ?p /t<car>};`,
			nBindings: 2,
			nRows:     4,
		},
		{
			q:         `select ?s, ?o from ?test where {?s "parent_of"@[] ?o};`,
			nBindings: 2,
			nRows:     4,
		},
		{
			q:         `select ?s, ?p, ?o from ?test where {/u<joe> as ?s "parent_of"@[] as ?p /u<mary> as ?o};`,
			nBindings: 3,
			nRows:     1,
		},
		{
			q:         `select ?s, ?p, ?o from ?test where {/u<unknown> as ?s "parent_of"@[] as ?p /u<mary> as ?o};`,
			nBindings: 3,
			nRows:     0,
		},
		{
			q:         `select ?o from ?test where {/u<joe> "parent_of"@[] ?o};`,
			nBindings: 1,
			nRows:     2,
		},
		{
			q:         `select ?p from ?test where {/u<joe> ?p /u<mary>};`,
			nBindings: 1,
			nRows:     1,
		},
		{
			q:         `select ?s from ?test where {?s "is_a"@[] /t<car>};`,
			nBindings: 1,
			nRows:     4,
		},
		{
			q:         `select ?s as ?s1 from ?test where {?s "is_a"@[] /t<car>};`,
			nBindings: 1,
			nRows:     4,
		},
		{
			q:         `select ?o from ?test where {/u<joe> "parent_of"@[] ?o. ?o "parent_of"@[] /u<john>};`,
			nBindings: 1,
			nRows:     1,
		},
		{
			q:         `select ?s, ?o from ?test where {/u<joe> "parent_of"@[] ?o. ?o "parent_of"@[] ?s};`,
			nBindings: 2,
			nRows:     2,
		},
		{
			q:         `select ?s, ?p, ?o, ?k, ?l, ?m from ?test where {?s ?p ?o. ?k ?l ?m};`,
			nBindings: 6,
			nRows:     (len(strings.Split(testTriples, "\n")) - 1) * (len(strings.Split(testTriples, "\n")) - 1),
		},
		{
			q:         `select ?s, ?p, ?o, ?k, ?l from ?test where {?s ?p ?o. ?k ?l ?m};`,
			nBindings: 5,
			nRows:     (len(strings.Split(testTriples, "\n")) - 1) * (len(strings.Split(testTriples, "\n")) - 1),
		},
		{
			q:         `select ?s, ?p, ?o, ?k from ?test where {?s ?p ?o. ?k ?l ?m};`,
			nBindings: 4,
			nRows:     (len(strings.Split(testTriples, "\n")) - 1) * (len(strings.Split(testTriples, "\n")) - 1),
		},
		{
			q:         `select ?s, ?p, ?o from ?test where {?s ?p ?o. ?k ?l ?m};`,
			nBindings: 3,
			nRows:     (len(strings.Split(testTriples, "\n")) - 1) * (len(strings.Split(testTriples, "\n")) - 1),
		},
		{
			q:         `select ?s, ?p from ?test where {?s ?p ?o. ?k ?l ?m};`,
			nBindings: 2,
			nRows:     (len(strings.Split(testTriples, "\n")) - 1) * (len(strings.Split(testTriples, "\n")) - 1),
		},
		{
			q:         `select ?s from ?test where {?s ?p ?o. ?k ?l ?m};`,
			nBindings: 1,
			nRows:     (len(strings.Split(testTriples, "\n")) - 1) * (len(strings.Split(testTriples, "\n")) - 1),
		},
		{
			q:         `select ?o from ?test where {/u<peter> "bought"@[,] ?o};`,
			nBindings: 1,
			nRows:     4,
		},
		{
			q:         `select ?o from ?test where {/u<peter> "bought"@[,2015-01-01T00:00:00-08:00] ?o};`,
			nBindings: 1,
			nRows:     0,
		},
		{
			q:         `select ?o from ?test where {/u<peter> "bought"@[2017-01-01T00:00:00-08:00,] ?o};`,
			nBindings: 1,
			nRows:     0,
		},
		{
			q:         `select ?o from ?test where {/u<peter> "bought"@[2015-01-01T00:00:00-08:00,2017-01-01T00:00:00-08:00] ?o};`,
			nBindings: 1,
			nRows:     4,
		},
		{
			q:         `select ?o from ?test where {/l<barcelona> "predicate"@[] "turned"@[,] as ?o};`,
			nBindings: 1,
			nRows:     4,
		},
		{
			q:         `select ?o from ?test where {/l<barcelona> "predicate"@[] "turned"@[,2015-01-01T00:00:00-08:00] as ?o};`,
			nBindings: 1,
			nRows:     0,
		},
		{
			q:         `select ?o from ?test where {/l<barcelona> "predicate"@[] "turned"@[2017-01-01T00:00:00-08:00,] as ?o};`,
			nBindings: 1,
			nRows:     0,
		},
		{
			q:         `select ?o from ?test where {/l<barcelona> "predicate"@[] "turned"@[2015-01-01T00:00:00-08:00,2017-01-01T00:00:00-08:00] as ?o};`,
			nBindings: 1,
			nRows:     4,
		},
		{
			q:         `select ?grandparent, count(?name) as ?grandchildren from ?test where {/u<joe> as ?grandparent "parent_of"@[] ?offspring . ?offspring "parent_of"@[] ?name} group by ?grandparent;`,
			nBindings: 2,
			nRows:     1,
		},
		{
			q:         `select ?grandparent, count(distinct ?name) as ?grandchildren from ?test where {/u<joe> as ?grandparent "parent_of"@[] ?offspring . ?offspring "parent_of"@[] ?name} group by ?grandparent;`,
			nBindings: 2,
			nRows:     1,
		},
		{
			q:         `select ?s, ?p, ?o, ?k, ?l, ?m from ?test where {?s ?p ?o. ?k ?l ?m} order by ?s, ?p, ?o, ?k, ?l, ?m;`,
			nBindings: 6,
			nRows:     (len(strings.Split(testTriples, "\n")) - 1) * (len(strings.Split(testTriples, "\n")) - 1),
		},
		{
			q:         `select ?s, ?p, ?o, ?k, ?l, ?m from ?test where {?s ?p ?o. ?k ?l ?m} order by ?s, ?p, ?o, ?k, ?l, ?m  having not(?s = ?s);`,
			nBindings: 6,
			nRows:     0,
		},
		{
			q:         `select ?o from ?test where {/l<barcelona> "predicate"@[] "turned"@[2015-01-01T00:00:00-08:00,2017-01-01T00:00:00-08:00] as ?o} LIMIT "2"^^type:int64;`,
			nBindings: 1,
			nRows:     2,
		},
		{
			q:         `select ?o from ?test where {/u<peter> "bought"@[2015-01-01T00:00:00-08:00,2017-01-01T00:00:00-08:00] ?o} before 2016-03-01T00:00:00-08:00;`,
			nBindings: 1,
			nRows:     3,
		},
		{
			q:         `select ?o from ?test where {/u<peter> "bought"@[2015-01-01T00:00:00-08:00,2017-01-01T00:00:00-08:00] ?o} after 2016-02-01T00:00:00-08:00;`,
			nBindings: 1,
			nRows:     3,
		},
		{
			q:         `select ?o from ?test where {/u<peter> "bought"@[2015-01-01T00:00:00-08:00,2017-01-01T00:00:00-08:00] ?o} between 2014-01-01T00:00:00-08:00, 2017-01-01T00:00:00-08:00;`,
			nBindings: 1,
			nRows:     4,
		},
		{
			q:         `SELECT ?grandparent, COUNT(?grandparent) AS ?number_of_grandchildren FROM ?test WHERE{ ?gp ID ?grandparent "parent_of"@[] ?c . ?c "parent_of"@[] ?gc ID ?gc } GROUP BY ?grandparent;`,
			nBindings: 2,
			nRows:     1,
		},
		{ // Issue 40 (https://github.com/google/badwolf/issues/40)
			q:         `SELECT ?item, ?t FROM ?test WHERE {?item "in"@[?t] /room<Bedroom>};`,
			nBindings: 2,
			nRows:     1,
		},
		{
			q:         `SHOW GRAPHS;`,
			nBindings: 1,
			nRows:     1,
		},
		{
			q:         `select ?s, ?o from ?test where {?s "tag"@[] ?o} having ?o = "abc"^^type:text;`,
			nBindings: 2,
			nRows:     1,
		},
		{
			q:         `select ?s, ?height from ?test where {?s "height_cm"@[] ?height} having ?height > "0"^^type:int64;`,
			nBindings: 2,
			nRows:     4,
		},
		{
			q:         `select ?s, ?height from ?test where {?s "height_cm"@[] ?height} having ?height > "160"^^type:int64;`,
			nBindings: 2,
			nRows:     3,
		},
		{
			q:         `select ?s, ?height from ?test where {?s "height_cm"@[] ?height} having ?height = "151"^^type:int64;`,
			nBindings: 2,
			nRows:     1,
		},
		/*
			/c<model s> "is_a"@[] /t<car>
			/c<model x> "is_a"@[] /t<car>
			/c<model y> "is_a"@[] /t<car>
		*/
		// OPTIONAL clauses.
		{
			q:         `SELECT ?car FROM ?test WHERE { ?car "is_a"@[] /t<car> };`,
			nBindings: 1,
			nRows:     4,
		},
		{
			q: `SELECT ?car
			    FROM ?test
			    WHERE {
				   /c<model s> as ?car "is_a"@[] /t<car>
				};`,
			nBindings: 1,
			nRows:     1,
		},
		{
			q: `SELECT ?car
			    FROM ?test
			    WHERE {
				   ?car "is_a"@[] /t<car> .
				   /c<model z> as ?car "is_a"@[] /t<car>
				};`,
			nBindings: 1,
			nRows:     0,
		},
		{
			q: `SELECT ?car
				FROM ?test
				WHERE {
					?car "is_a"@[] /t<car> .
					OPTIONAL { /c<model O> "is_a"@[] /t<car> }
				};`,
			nBindings: 1,
			nRows:     4,
		},
		{
			q: `SELECT ?car
				FROM ?test
				WHERE {
					?car "is_a"@[] /t<car> .
					OPTIONAL { ?car "is_a"@[] /t<car> }
				};`,
			nBindings: 1,
			nRows:     4,
		},
		{
			q: `SELECT ?cars, ?type
				FROM ?test
				WHERE {
					?cars "is_a"@[] /t<car> .
					OPTIONAL { ?cars "is_a"@[] ?type }
				};`,
			nBindings: 2,
			nRows:     4,
		},
		{
			q: `SELECT ?p, ?o
				FROM ?test
				WHERE {
					/c<mini> ?p ?o
				}
				HAVING ?o > "37"^^type:int64;`,
			nBindings: 2,
			nRows:     0,
		},
		{
			q: `SELECT ?o
				FROM ?test
				WHERE {
					/u<alice> "height_cm"@[] ?o
				}
				HAVING ?o = /u<peter>;`,
			nBindings: 1,
			nRows:     0,
		},
		{
			q: `SELECT ?s_id, ?height
				FROM ?test
				WHERE {
					?s ID ?s_id "height_cm"@[] ?height
				}
				HAVING ?s_id = "alice"^^type:text;`,
			nBindings: 2,
			nRows:     1,
		},
		{
			q: `SELECT ?p_id, ?o
				FROM ?test
				WHERE {
					/u<peter> ?p ID ?p_id ?o
				}
				HAVING ?p_id < "parent_of"^^type:text;`,
			nBindings: 2,
			nRows:     4,
		},
		{
			q: `SELECT ?s, ?s_type
				FROM ?test
				WHERE {
					?s TYPE ?s_type ?p ?o
				}
				HAVING ?s_type = "/c"^^type:text;`,
			nBindings: 2,
			nRows:     4,
		},
		{
			q: `SELECT ?s, ?s_type
				FROM ?test
				WHERE {
					?s TYPE ?s_type ?p ?o
				}
				HAVING ?s_type < "/l"^^type:text;`,
			nBindings: 2,
			nRows:     7,
		},
		{
			q: `SELECT ?p, ?p_id, ?o
				FROM ?test
				WHERE {
					/u<peter> ?p ID ?p_id ?o
				}
				HAVING ?p_id = "bought"^^type:text
				AFTER 2016-03-01T00:00:00-08:00;`,
			nBindings: 3,
			nRows:     2,
		},
		{
			q: `SELECT ?p, ?p_id, ?o
				FROM ?test
				WHERE {
					/u<peter> ?p ID ?p_id ?o
				}
				HAVING ?p_id < "parent_of"^^type:text
				BEFORE 2016-03-01T00:00:00-08:00;`,
			nBindings: 3,
			nRows:     3,
		},
		{
			q: `SELECT ?p, ?p_id, ?o
				FROM ?test
				WHERE {
					/u<peter> ?p ID ?p_id ?o
				}
				HAVING ?p_id = "bought"^^type:text
				BETWEEN 2016-02-01T00:00:00-08:00, 2016-03-01T00:00:00-08:00;`,
			nBindings: 3,
			nRows:     2,
		},
		{
			q: `SELECT ?p, ?p_id, ?o
				FROM ?test
				WHERE {
					/u<peter> ?p ID ?p_id ?o
				}
				HAVING ?p_id < "work_with"^^type:text
				BEFORE 2016-02-01T00:00:00-08:00;`,
			nBindings: 3,
			nRows:     4,
		},
		{
			q: `SELECT ?o, ?o_type
				FROM ?test
				WHERE {
					?s ?p ?o TYPE ?o_type
				}
				HAVING (?s = /u<joe>) OR (?s = /l<barcelona>) OR (?s = /u<alice>);`,
			nBindings: 2,
			nRows:     2,
		},
		{
			q: `SELECT ?o, ?o_type
				FROM ?test
				WHERE {
					?s ?p ?o .
					OPTIONAL { ?s ?p ?o TYPE ?o_type }
				}
				HAVING (?s = /u<joe>) OR (?s = /l<barcelona>) OR (?s = /u<alice>);`,
			nBindings: 2,
			nRows:     9,
		},
		{
			q: `SELECT ?p, ?time, ?o
				FROM ?test
				WHERE {
					/u<peter> ?p AT ?time ?o
				};`,
			nBindings: 3,
			nRows:     4,
		},
		{
			q: `SELECT ?p, ?time, ?o
				FROM ?test
				WHERE {
					/u<peter> ?p ?o .
					OPTIONAL { /u<peter> ?p AT ?time ?o }
				};`,
			nBindings: 3,
			nRows:     6,
		},
		{
			q: `SELECT ?time, ?o
				FROM ?test
				WHERE {
					/u<joe> "parent_of"@[?time] ?o
				};`,
			nBindings: 2,
			nRows:     0,
		},
		{
			q: `SELECT ?time, ?o
				FROM ?test
				WHERE {
					/u<joe> ?p ?o .
					OPTIONAL { /u<joe> "parent_of"@[?time] ?o }
				};`,
			nBindings: 2,
			nRows:     2,
		},
		{
			q: `SELECT ?p, ?o, ?time_o
				FROM ?test
				WHERE {
					/l<barcelona> ?p ?o AT ?time_o
				};`,
			nBindings: 3,
			nRows:     4,
		},
		{
			q: `SELECT ?p, ?o, ?time_o
				FROM ?test
				WHERE {
					/l<barcelona> ?p ?o .
					OPTIONAL { /l<barcelona> ?p ?o AT ?time_o }
				};`,
			nBindings: 3,
			nRows:     5,
		},
		{
			q: `SELECT ?p, ?o_time
				FROM ?test
				WHERE {
					/l<barcelona> ?p "immutable_predicate"@[?o_time]
				};`,
			nBindings: 2,
			nRows:     0,
		},
		{
			q: `SELECT ?p, ?o_time
				FROM ?test
				WHERE {
					/l<barcelona> ?p ?o .
					OPTIONAL { /l<barcelona> ?p "immutable_predicate"@[?o_time] }
				};`,
			nBindings: 2,
			nRows:     5,
		},
		{
			q: `SELECT ?s, ?s_alias, ?s_id, ?s_type
				FROM ?test
				WHERE {
					?s AS ?s_alias ID ?s_id TYPE ?s_type "parent_of"@[] ?o
				};`,
			nBindings: 4,
			nRows:     4,
		},
		{
			q: `SELECT ?s, ?s_alias, ?s_id, ?s_type
				FROM ?test
				WHERE {
					?s AS ?s_alias TYPE ?s_type ID ?s_id "parent_of"@[] ?o
				};`,
			nBindings: 4,
			nRows:     4,
		},
		{
			q: `SELECT ?o, ?o_alias, ?o_id, ?o_type
				FROM ?test
				WHERE {
					?s "parent_of"@[] ?o AS ?o_alias ID ?o_id TYPE ?o_type
				};`,
			nBindings: 4,
			nRows:     4,
		},
		{
			q: `SELECT ?o, ?o_alias, ?o_id, ?o_type
				FROM ?test
				WHERE {
					?s "parent_of"@[] ?o AS ?o_alias TYPE ?o_type ID ?o_id
				};`,
			nBindings: 4,
			nRows:     4,
		},
		{
			q: `SELECT ?o
				FROM ?test
				WHERE {
					/u<peter> "bought"@[2015-01-01T00:00:00-08:00,2017-01-01T00:00:00-08:00] ?o
				}
				BEFORE 2016-03-01T00:00:00-08:00
				LIMIT "1"^^type:int64;`,
			nBindings: 1,
			nRows:     1,
		},
		{
			q: `SELECT ?o
				FROM ?test
				WHERE {
					/u<peter> "bought"@[2015-01-01T00:00:00-08:00,2017-01-01T00:00:00-08:00] ?o
				}
				AFTER 2016-02-01T00:00:00-08:00
				LIMIT "1"^^type:int64;`,
			nBindings: 1,
			nRows:     1,
		},
		{
			q: `SELECT ?p, ?time
				FROM ?test
				WHERE {
					/u<peter> ?p AT ?time ?o
				}
				HAVING ?time < 2016-03-01T00:00:00-08:00;`,
			nBindings: 2,
			nRows:     2,
		},
		{
			q: `SELECT ?p, ?time
				FROM ?test
				WHERE {
					/u<peter> ?p AT ?time ?o
				}
				HAVING ?time < 2016-03-01T00:00:00-08:00
				LIMIT "1"^^type:int64;`,
			nBindings: 2,
			nRows:     1,
		},
		{
			q: `SELECT ?s, ?p, ?p_id, ?time
				FROM ?test
				WHERE {
					?s ?p ID ?p_id AT ?time ?o
				}
				HAVING (?p_id < "in"^^type:text) AND (?time > 2016-02-01T00:00:00-08:00);`,
			nBindings: 4,
			nRows:     3,
		},
		{
			q: `SELECT ?p, ?time
				FROM ?test
				WHERE {
					/u<peter> ?p AT ?time ?o
				}
				HAVING ?time = 2016-01-01T01:00:00-07:00;`,
			nBindings: 2,
			nRows:     1,
		},
		{
			q: `SELECT ?p, ?time
				FROM ?test
				WHERE {
					/u<peter> ?p AT ?time ?o
				}
				HAVING ?time > 2016-02-01T00:00:00-07:00;`,
			nBindings: 2,
			nRows:     3,
		},
		{
			q: `SELECT ?s, ?p, ?o
				FROM ?test
				WHERE {
					?s ?p ?o
				}
				HAVING ?p = "height_cm"@[];`,
			nBindings: 3,
			nRows:     4,
		},
		{
			q: `SELECT ?s, ?p, ?o
				FROM ?test
				WHERE {
					?s ?p ?o
				}
				HAVING ?p = "bought"@[2016-03-01T00:00:00-08:00];`,
			nBindings: 3,
			nRows:     1,
		},
		{
			q: `SELECT ?s, ?p, ?o
				FROM ?test
				WHERE {
					?s ?p ?o
				}
				HAVING (?p = "tag"@[]) OR (?p = "bought"@[2016-02-01T00:00:00-08:00]);`,
			nBindings: 3,
			nRows:     2,
		},
		{
			q: `SELECT ?p, ?o
				FROM ?test
				WHERE {
					/u<joe> ?p ?o .
				};`,
			nBindings: 2,
			nRows:     2,
		},
		{
			q: `SELECT ?o
				FROM ?test
				WHERE {
					/u<joe> "parent_of"@[] ?o .
					?o "parent_of"@[] /u<john> .
				};`,
			nBindings: 1,
			nRows:     1,
		},
		{
			q: `SELECT ?p1, ?p2
				FROM ?test
				WHERE {
					/u<joe> ?p1 /u<mary> .
					/u<joe> ?p2 /u<peter> .
				};`,
			nBindings: 2,
			nRows:     1,
		},
		{
			q: `SELECT ?car
				FROM ?test
				WHERE {
					?car "is_a"@[] /t<car> .
					OPTIONAL { /c<model O> "is_a"@[] /t<car> } .
				};`,
			nBindings: 1,
			nRows:     4,
		},
		{
			q: `SELECT ?s, ?height
				FROM ?test
				WHERE {
					?s "height_cm"@[] ?height
				}
				HAVING ?s = /u<bob>;`,
			nBindings: 2,
			nRows:     1,
		},
		{
			q: `SELECT ?s, ?p, ?o
				FROM ?test
				WHERE {
					?s ?p ?o
				}
				HAVING (?s = /u<bob>) OR (?o = /t<car>);`,
			nBindings: 3,
			nRows:     5,
		},
		{
			q: `SELECT ?s, ?o, ?o_time
				FROM ?test
				WHERE {
					?s ?p ?o AT ?o_time
				};`,
			nBindings: 3,
			nRows:     5,
		},
		{
			q: `SELECT ?s, ?o, ?o_time
				FROM ?test
				WHERE {
					?s ?p ?o .
					OPTIONAL { ?s ?p ?o AT ?o_time }
				}
				HAVING (?s = /l<barcelona>) OR (?s = /u<joe>) OR (?s = /u<bob>);`,
			nBindings: 3,
			nRows:     8,
		},
		{
			q: `SELECT ?s, ?o_time
				FROM ?test
				WHERE {
					?s ?p "turned"@[?o_time]
				};`,
			nBindings: 2,
			nRows:     5,
		},
		{
			q: `SELECT ?p, ?o
				FROM ?test
				WHERE {
					/u<peter> ?p ?o .
					FILTER latest(?p)
				};`,
			nBindings: 2,
			nRows:     1,
		},
		{
			q: `SELECT ?p_alias, ?o
				FROM ?test
				WHERE {
					/u<peter> ?p AS ?p_alias ?o .
					FILTER latest(?p_alias)
				};`,
			nBindings: 2,
			nRows:     1,
		},
		{
			q: `SELECT ?p, ?o
				FROM ?test
				WHERE {
					/l<barcelona> ?p ?o .
					FILTER latest(?o)
				};`,
			nBindings: 2,
			nRows:     1,
		},
		{
			q: `SELECT ?p, ?o_alias
				FROM ?test
				WHERE {
					/l<barcelona> ?p ?o AS ?o_alias .
					FILTER latest(?o_alias)
				};`,
			nBindings: 2,
			nRows:     1,
		},
		{
			q: `SELECT ?p1, ?p2
				FROM ?test
				WHERE {
					/u<peter> ?p1 ?o1 .
					/item/book<000> ?p2 ?o2 .
					FILTER latest(?p1) .
					FILTER latest(?p2)
				};`,
			nBindings: 2,
			nRows:     1,
		},
		{
			q: `SELECT ?s, ?p, ?o
				FROM ?test
				WHERE {
					?s ?p ?o .
					FILTER latest(?p)
				};`,
			nBindings: 3,
			nRows:     3,
		},
		{
			q: `SELECT ?s, ?p, ?o
				FROM ?test
				WHERE {
					?s ?p ?o .
					FILTER latest(?o)
				};`,
			nBindings: 3,
			nRows:     2,
		},
		{
			q: `SELECT ?s, ?p_alias, ?o
				FROM ?test
				WHERE {
					?s "bought"@[2016-03-01T00:00:00-08:00] AS ?p_alias ?o .
					FILTER latest(?p_alias)
				};`,
			nBindings: 3,
			nRows:     1,
		},
		{
			q: `SELECT ?s, ?p, ?o_alias
				FROM ?test
				WHERE {
					?s ?p "turned"@[2016-03-01T00:00:00-08:00] AS ?o_alias .
					FILTER latest(?o_alias)
				};`,
			nBindings: 3,
			nRows:     1,
		},
		{
			q: `SELECT ?s, ?p, ?o
				FROM ?test
				WHERE {
					?s "bought"@[?time] ?o .
					OPTIONAL { ?s ?p ?o } .
					FILTER latest(?p)
				};`,
			nBindings: 3,
			nRows:     6,
		},
		{
			q: `SELECT ?p
				FROM ?test
				WHERE {
					/u<peter> ?p ?o1 .
					/u<paul> ?p ?o2 .
					FILTER latest(?p)
				};`,
			nBindings: 1,
			nRows:     1,
		},
		{
			q: `SELECT ?p, ?o
				FROM ?test
				WHERE {
					/u<peter> ?p ?o .
					FILTER lAtEsT(?p) .
				};`,
			nBindings: 2,
			nRows:     1,
		},
		{
			q: `SELECT ?p, ?o
				FROM ?test
				WHERE {
					/u<peter> ?p ?o .
					FILTER isImmutable(?p)
				};`,
			nBindings: 2,
			nRows:     2,
		},
		{
			q: `SELECT ?s, ?p, ?o
				FROM ?test
				WHERE {
					?s ?p ?o .
					FILTER isImmutable(?o)
				};`,
			nBindings: 3,
			nRows:     1,
		},
		{
			q: `SELECT ?p_alias, ?o
				FROM ?test
				WHERE {
					/u<peter> ?p AS ?p_alias ?o .
					FILTER isImmutable(?p_alias)
				};`,
			nBindings: 2,
			nRows:     2,
		},
		{
			q: `SELECT ?s, ?p, ?o_alias
				FROM ?test
				WHERE {
					?s ?p ?o AS ?o_alias .
					FILTER isImmutable(?o_alias)
				};`,
			nBindings: 3,
			nRows:     1,
		},
		{
			q: `SELECT ?p, ?o
				FROM ?test
				WHERE {
					/u<peter> ?p ?o .
					FILTER isTemporal(?p)
				};`,
			nBindings: 2,
			nRows:     4,
		},
		{
			q: `SELECT ?s, ?p, ?o
				FROM ?test
				WHERE {
					?s ?p ?o .
					FILTER isTemporal(?o)
				};`,
			nBindings: 3,
			nRows:     5,
		},
		{
			q: `SELECT ?p_alias, ?o
				FROM ?test
				WHERE {
					/u<peter> ?p AS ?p_alias ?o .
					FILTER isTemporal(?p_alias)
				};`,
			nBindings: 2,
			nRows:     4,
		},
		{
			q: `SELECT ?s, ?p, ?o_alias
				FROM ?test
				WHERE {
					?s ?p ?o AS ?o_alias .
					FILTER isTemporal(?o_alias)
				};`,
			nBindings: 3,
			nRows:     5,
		},
		{
			q: `SELECT ?p, ?o
				FROM ?test
				WHERE {
					/u<peter> ?p ?o .
					FILTER isTemporal(?p)
				}
				BETWEEN 2016-02-01T00:00:00-08:00, 2016-03-01T00:00:00-08:00;`,
			nBindings: 2,
			nRows:     2,
		},
		{
			q: `SELECT ?p, ?o
				FROM ?test
				WHERE {
					/u<peter> ?p ?o .
					FILTER latest(?p)
				}
				BETWEEN 2016-02-01T00:00:00-08:00, 2016-03-01T00:00:00-08:00;`,
			nBindings: 2,
			nRows:     1,
		},
		{
			q: `SELECT ?s, ?p, ?o
				FROM ?test
				WHERE {
					?s ?p ?o .
					FILTER isTemporal(?p)
				}
				LIMIT "3"^^type:int64;`,
			nBindings: 3,
			nRows:     3,
		},
	}

	s, ctx := memory.NewStore(), context.Background()
	populateStoreWithTriples(ctx, s, "?test", testTriples, t)
	for _, entry := range testTable {
		// Setup for test:
		p, err := grammar.NewParser(grammar.SemanticBQL())
		if err != nil {
			t.Fatalf("grammar.NewParser should have produced a valid BQL parser but got error: %v", err)
		}
		st := &semantic.Statement{}
		if err := p.Parse(grammar.NewLLk(entry.q, 1), st); err != nil {
			t.Fatalf("parser.Parse failed for query \"%s\"\nwith error: %v", entry.q, err)
		}
		plnr, err := New(ctx, s, st, 0, 10, nil)
		if err != nil {
			t.Fatalf("planner.New failed to create a valid query plan with error: %v", err)
		}

		// Actual test:
		tbl, err := plnr.Execute(ctx)
		if err != nil {
			t.Fatalf("planner.Execute(%s)\n= _, %v; want _, nil", entry.q, err)
		}
		if got, want := len(tbl.Bindings()), entry.nBindings; got != want {
			t.Errorf("planner.Execute(%s)\n= a Table with %d bindings; want %d", entry.q, got, want)
		}
		if got, want := len(tbl.Rows()), entry.nRows; got != want {
			t.Errorf("planner.Execute(%s)\n= a Table with %d rows; want %d\nTable:\n%v\n", entry.q, got, want, tbl)
		}
	}
}

func TestPlannerQueryError(t *testing.T) {
	testTable := []struct {
		q string
	}{
		{
			q: `SELECT ?s_id, ?height
				FROM ?test
				WHERE {
					?s ID ?s_id "height_cm"@[] ?height
				}
				HAVING ?s_id > "37"^^type:int64;`,
		},
		{
			q: `SELECT ?s_id, ?height
				FROM ?test
				WHERE {
					?s ID ?s_id "height_cm"@[] ?height
				}
				HAVING ?s_id = /u<alice>;`,
		},
		{
			q: `SELECT ?s, ?p, ?o
				FROM ?test
				WHERE {
					?s ?p ?o
				}
				HAVING ?p < "height_cm"@[];`,
		},
		{
			q: `SELECT ?s, ?p, ?o
				FROM ?test
				WHERE {
					?s ?p ?o
				}
				HAVING ?p > "bought"@[2016-01-01T00:00:00-08:00];`,
		},
		{
			q: `SELECT ?s, ?height
				FROM ?test
				WHERE {
					?s "height_cm"@[] ?height
				}
				HAVING ?s < /u<zzzzz>;`,
		},
		{
			q: `SELECT ?s, ?height
				FROM ?test
				WHERE {
					?s "height_cm"@[] ?height
				}
				HAVING ?s > /u<alice>;`,
		},
		{
			q: `SELECT ?p, ?o
				FROM ?test
				WHERE {
					/u<peter> ?p ?o .
					FILTER latest(?p) .
					FILTER latest(?p)
				};`,
		},
		{
			q: `SELECT ?p, ?o
				FROM ?test
				WHERE {
					/l<barcelona> ?p ?o .
					FILTER latest(?p) .
					FILTER latest(?o)
				};`,
		},
		{
			q: `SELECT ?p, ?o
				FROM ?test
				WHERE {
					/u<peter> ?p ?o .
					FILTER latest(?b_not_exist)
				};`,
		},
		{
			q: `SELECT ?s, ?p, ?o
				FROM ?test
				WHERE {
					?s ID ?sID ?p ?o .
					FILTER latest(?sID)
				};`,
		},
	}

	s, ctx := memory.NewStore(), context.Background()
	populateStoreWithTriples(ctx, s, "?test", testTriples, t)
	for _, entry := range testTable {
		// Setup for test:
		p, err := grammar.NewParser(grammar.SemanticBQL())
		if err != nil {
			t.Fatalf("grammar.NewParser should have produced a valid BQL parser but got error: %v", err)
		}
		st := &semantic.Statement{}
		if err := p.Parse(grammar.NewLLk(entry.q, 1), st); err != nil {
			t.Fatalf("parser.Parse failed for query \"%s\"\nwith error: %v", entry.q, err)
		}
		plnr, err := New(ctx, s, st, 0, 10, nil)
		if err != nil {
			t.Fatalf("planner.New failed to create a valid query plan with error: %v", err)
		}

		// Actual test:
		_, err = plnr.Execute(ctx)
		if err == nil {
			t.Errorf("planner.Execute(%s)\n= _, nil; want _, error", entry.q)
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

	for _, entry := range testTable {
		p, err := grammar.NewParser(grammar.SemanticBQL())
		if err != nil {
			t.Errorf("grammar.NewParser should have produced a valid BQL parser but got error: %v", err)
		}

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
		t.Errorf("grammar.NewParser should have produced a valid BQL parser but got error: %v", err)
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

	for _, entry := range testTable {
		p, err := grammar.NewParser(grammar.SemanticBQL())
		if err != nil {
			t.Errorf("grammar.NewParser should have produced a valid BQL parser but got error: %v", err)
		}

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
		t.Fatalf("grammar.NewParser should have produced a valid BQL parser but got error: %v", pErr)
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
		t.Fatalf("grammar.NewParser should have produced a valid BQL parser but got error: %v", pErr)
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
			b.Fatalf("grammar.NewParser should have produced a valid BQL parser but got error: %v", pErr)
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
		t.Fatalf("grammar.NewParser should have produced a valid BQL parser but got error: %v", pErr)
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
	b.ResetTimer()
	for n := 0; n < b.N; n++ {
		p, err := grammar.NewParser(grammar.SemanticBQL())
		if err != nil {
			b.Fatalf("grammar.NewParser should have produced a valid BQL parser but got error: %v", err)
		}

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
