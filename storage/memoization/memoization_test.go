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

package memoization

import (
	"context"
	"reflect"
	"testing"

	"github.com/google/badwolf/triple/predicate"

	"github.com/google/badwolf/storage/memory"
	"github.com/google/badwolf/triple"
	"github.com/google/badwolf/triple/literal"
	"github.com/google/badwolf/triple/node"

	"github.com/google/badwolf/storage"
	"github.com/pborman/uuid"
)

func TestCombinedUUID(t *testing.T) {
	want := "op:9dae52f4-9b35-5d5f-bd8e-195d4b16fc30:00000000-0000-0000-0000-000000000000:00000000-0000-0000-0000-000000000000"
	if got := combinedUUID("op", storage.DefaultLookup, uuid.NIL, uuid.NIL); got != want {
		t.Errorf("combinedUUID returned the wrong value; got %v, want %v", got, want)
	}
}

func createTriples(t *testing.T, ss []string) []*triple.Triple {
	ts := []*triple.Triple{}
	for _, s := range ss {
		trpl, err := triple.Parse(s, literal.DefaultBuilder())
		if err != nil {
			t.Errorf("triple.Parse failed to parse valid triple %s with error %v", s, err)
			continue
		}
		ts = append(ts, trpl)
	}
	return ts
}

func buildTriples(t *testing.T) []*triple.Triple {
	return createTriples(t, []string{
		"/u<john>\t\"knows\"@[]\t/u<mary>",
		"/u<john>\t\"knows\"@[]\t/u<peter>",
		"/u<john>\t\"knows\"@[]\t/u<alice>",
		"/u<mary>\t\"knows\"@[]\t/u<andrew>",
		"/u<mary>\t\"knows\"@[]\t/u<kim>",
		"/u<mary>\t\"knows\"@[]\t/u<alice>",
		"/u<john>\t\"meet\"@[2010-04-10T4:21:00.000000000Z]\t/u<mary>",
		"/u<john>\t\"meet\"@[2011-04-10T4:21:00.000000000Z]\t/u<mary>",
		"/u<john>\t\"meet\"@[2012-04-10T4:21:00.000000000Z]\t/u<mary>",
		"/u<john>\t\"meet\"@[2013-04-10T4:21:00.000000000Z]\t/u<mary>",
		"/u<john>\t\"meet\"@[2014-04-10T4:21:00.000000000Z]\t/u<mary>",
		"/u<john>\t\"meet\"@[2015-04-10T4:21:00.000000000Z]\t/u<mary>",
		"/u<john>\t\"meet\"@[2016-04-10T4:21:00.000000000Z]\t/u<mary>",
		"/u<john>\t\"meet\"@[2017-04-10T4:21:00.000000000Z]\t/u<mary>",
		"/u<john>\t\"meet\"@[2018-04-10T4:21:00.000000000Z]\t/u<mary>",
		"/u<john>\t\"meet\"@[2019-04-10T4:21:00.000000000Z]\t/u<mary>",
	})
}

func buildtMemoizedStore(t *testing.T) (context.Context, *storeMemoizer) {
	ms := New(memory.NewStore())
	ctx := context.Background()
	g, err := ms.NewGraph(ctx, "?test")
	if err != nil {
		t.Fatal(err)
	}
	if err := g.AddTriples(ctx, buildTriples(t)); err != nil {
		t.Fatal(err)
	}
	return ctx, ms.(*storeMemoizer)
}

func TestStoreWrapper(t *testing.T) {
	ctx, sm := buildtMemoizedStore(t)

	if got, want := sm.Name(ctx), sm.s.Name(ctx); got != want {
		t.Errorf("failed to retrieve the right name; got %q, want %q", got, want)
	}

	if got, want := sm.Version(ctx), sm.s.Version(ctx); got != want {
		t.Errorf("failed to retrieve the right version; got %q, want %q", got, want)
	}

	if _, err := sm.Graph(ctx, "UNKNOWV"); err == nil {
		t.Error("Graph should have fail for unknow graph.")
	}

	g, err := sm.Graph(ctx, "?test")
	if err != nil {
		t.Fatal(err)
	}
	if got, want := g.ID(ctx), "?test"; got != want {
		t.Errorf("failed to retrieve the right graph ID; got %q, want %q", got, want)
	}

	c := make(chan string)
	var got []string
	go sm.GraphNames(ctx, c)
	for s := range c {
		got = append(got, s)
	}
	if want := []string{"?test"}; !reflect.DeepEqual(got, want) {
		t.Errorf("failed to retrieve the right graph IDs; got %q, want %q", got, want)
	}

	if err := sm.DeleteGraph(ctx, "?test"); err != nil {
		t.Fatal(err)
	}

	c = make(chan string)
	got = nil
	go sm.GraphNames(ctx, c)
	for s := range c {
		got = append(got, s)
	}
	if want := []string(nil); !reflect.DeepEqual(got, want) {
		t.Errorf("failed to retrieve the right graph IDs; got %q, want %q", got, want)
	}
}

func TestTripleDelition(t *testing.T) {
	ctx, sm := buildtMemoizedStore(t)

	g, err := sm.Graph(ctx, "?test")
	if err != nil {
		t.Fatal(err)
	}

	if err := g.RemoveTriples(ctx, nil); err != nil {
		t.Fatal(err)
	}
}

func TestObjects(t *testing.T) {
	ctx, sm := buildtMemoizedStore(t)
	ts := buildTriples(t)

	g, err := sm.Graph(ctx, "?test")
	if err != nil {
		t.Fatal(err)
	}

	// Query the fist round.
	objects := func() []*triple.Object {
		objs := make(chan *triple.Object)
		go func() {
			if err := g.Objects(ctx, ts[0].Subject(), ts[0].Predicate(), storage.DefaultLookup, objs); err != nil {
				t.Fatal(err)
			}
		}()
		var ots []*triple.Object
		for o := range objs {
			ots = append(ots, o)
		}
		return ots
	}

	og := objects()
	for i, max := 0, 100; i < max; i++ {
		if got, want := objects(), og; !reflect.DeepEqual(got, want) {
			t.Fatalf("failed to returned the right objects; got %v, want %v", got, want)
		}
	}
}

func TestSubjects(t *testing.T) {
	ctx, sm := buildtMemoizedStore(t)
	ts := buildTriples(t)

	g, err := sm.Graph(ctx, "?test")
	if err != nil {
		t.Fatal(err)
	}

	// Query the fist round.
	subjects := func() []*node.Node {
		subs := make(chan *node.Node)
		go func() {
			if err := g.Subjects(ctx, ts[0].Predicate(), ts[0].Object(), storage.DefaultLookup, subs); err != nil {
				t.Fatal(err)
			}
		}()
		var ns []*node.Node
		for s := range subs {
			ns = append(ns, s)
		}
		return ns
	}

	og := subjects()
	for i, max := 0, 100; i < max; i++ {
		if got, want := subjects(), og; !reflect.DeepEqual(got, want) {
			t.Fatalf("failed to returned the right subjects; got %v, want %v", got, want)
		}
	}
}

func TestPredicatesForSubject(t *testing.T) {
	ctx, sm := buildtMemoizedStore(t)
	ts := buildTriples(t)

	g, err := sm.Graph(ctx, "?test")
	if err != nil {
		t.Fatal(err)
	}

	// Query the fist round.
	predicates := func() []*predicate.Predicate {
		prds := make(chan *predicate.Predicate)
		go func() {
			if err := g.PredicatesForSubject(ctx, ts[0].Subject(), storage.DefaultLookup, prds); err != nil {
				t.Fatal(err)
			}
		}()
		var ps []*predicate.Predicate
		for p := range prds {
			ps = append(ps, p)
		}
		return ps
	}

	og := predicates()
	for i, max := 0, 100; i < max; i++ {
		if got, want := predicates(), og; !reflect.DeepEqual(got, want) {
			t.Fatalf("failed to returned the right predicates; got %v, want %v", got, want)
		}
	}
}
func TestPredicatesForObject(t *testing.T) {
	ctx, sm := buildtMemoizedStore(t)
	ts := buildTriples(t)

	g, err := sm.Graph(ctx, "?test")
	if err != nil {
		t.Fatal(err)
	}

	// Query the fist round.
	predicates := func() []*predicate.Predicate {
		prds := make(chan *predicate.Predicate)
		go func() {
			if err := g.PredicatesForObject(ctx, ts[0].Object(), storage.DefaultLookup, prds); err != nil {
				t.Fatal(err)
			}
		}()
		var ps []*predicate.Predicate
		for p := range prds {
			ps = append(ps, p)
		}
		return ps
	}

	og := predicates()
	for i, max := 0, 100; i < max; i++ {
		if got, want := predicates(), og; !reflect.DeepEqual(got, want) {
			t.Fatalf("failed to returned the right predicates; got %v, want %v", got, want)
		}
	}
}

func TestPredicatesForSubjectAndObject(t *testing.T) {
	ctx, sm := buildtMemoizedStore(t)
	ts := buildTriples(t)

	g, err := sm.Graph(ctx, "?test")
	if err != nil {
		t.Fatal(err)
	}

	// Query the fist round.
	predicates := func() []*predicate.Predicate {
		prds := make(chan *predicate.Predicate)
		go func() {
			if err := g.PredicatesForSubjectAndObject(ctx, ts[0].Subject(), ts[0].Object(), storage.DefaultLookup, prds); err != nil {
				t.Fatal(err)
			}
		}()
		var ps []*predicate.Predicate
		for p := range prds {
			ps = append(ps, p)
		}
		return ps
	}

	og := predicates()
	for i, max := 0, 100; i < max; i++ {
		if got, want := predicates(), og; !reflect.DeepEqual(got, want) {
			t.Fatalf("failed to returned the right predicates; got %v, want %v", got, want)
		}
	}
}

func TestTriplesForSubject(t *testing.T) {
	ctx, sm := buildtMemoizedStore(t)
	ts := buildTriples(t)

	g, err := sm.Graph(ctx, "?test")
	if err != nil {
		t.Fatal(err)
	}

	// Query the fist round.
	triples := func() []*triple.Triple {
		trps := make(chan *triple.Triple)
		go func() {
			if err := g.TriplesForSubject(ctx, ts[0].Subject(), storage.DefaultLookup, trps); err != nil {
				t.Fatal(err)
			}
		}()
		var ts []*triple.Triple
		for t := range trps {
			ts = append(ts, t)
		}
		return ts
	}

	og := triples()
	for i, max := 0, 100; i < max; i++ {
		if got, want := triples(), og; !reflect.DeepEqual(got, want) {
			t.Fatalf("failed to returned the right triples; got %v, want %v", got, want)
		}
	}
}

func TestTriplesForPredicate(t *testing.T) {
	ctx, sm := buildtMemoizedStore(t)
	ts := buildTriples(t)

	g, err := sm.Graph(ctx, "?test")
	if err != nil {
		t.Fatal(err)
	}

	// Query the fist round.
	triples := func() []*triple.Triple {
		trps := make(chan *triple.Triple)
		go func() {
			if err := g.TriplesForPredicate(ctx, ts[0].Predicate(), storage.DefaultLookup, trps); err != nil {
				t.Fatal(err)
			}
		}()
		var ts []*triple.Triple
		for t := range trps {
			ts = append(ts, t)
		}
		return ts
	}

	og := triples()
	for i, max := 0, 100; i < max; i++ {
		if got, want := triples(), og; !reflect.DeepEqual(got, want) {
			t.Fatalf("failed to returned the right triples; got %v, want %v", got, want)
		}
	}
}

func TestTriplesForObject(t *testing.T) {
	ctx, sm := buildtMemoizedStore(t)
	ts := buildTriples(t)

	g, err := sm.Graph(ctx, "?test")
	if err != nil {
		t.Fatal(err)
	}

	// Query the fist round.
	triples := func() []*triple.Triple {
		trps := make(chan *triple.Triple)
		go func() {
			if err := g.TriplesForObject(ctx, ts[0].Object(), storage.DefaultLookup, trps); err != nil {
				t.Fatal(err)
			}
		}()
		var ts []*triple.Triple
		for t := range trps {
			ts = append(ts, t)
		}
		return ts
	}

	og := triples()
	for i, max := 0, 100; i < max; i++ {
		if got, want := triples(), og; !reflect.DeepEqual(got, want) {
			t.Fatalf("failed to returned the right triples; got %v, want %v", got, want)
		}
	}
}

func TestTriplesForSubjectAndPredicate(t *testing.T) {
	ctx, sm := buildtMemoizedStore(t)
	ts := buildTriples(t)

	g, err := sm.Graph(ctx, "?test")
	if err != nil {
		t.Fatal(err)
	}

	// Query the fist round.
	triples := func() []*triple.Triple {
		trps := make(chan *triple.Triple)
		go func() {
			if err := g.TriplesForSubjectAndPredicate(ctx, ts[0].Subject(), ts[0].Predicate(), storage.DefaultLookup, trps); err != nil {
				t.Fatal(err)
			}
		}()
		var ts []*triple.Triple
		for t := range trps {
			ts = append(ts, t)
		}
		return ts
	}

	og := triples()
	for i, max := 0, 100; i < max; i++ {
		if got, want := triples(), og; !reflect.DeepEqual(got, want) {
			t.Fatalf("failed to returned the right triples; got %v, want %v", got, want)
		}
	}
}

func TestTriplesForPredicateAndObject(t *testing.T) {
	ctx, sm := buildtMemoizedStore(t)
	ts := buildTriples(t)

	g, err := sm.Graph(ctx, "?test")
	if err != nil {
		t.Fatal(err)
	}

	// Query the fist round.
	triples := func() []*triple.Triple {
		trps := make(chan *triple.Triple)
		go func() {
			if err := g.TriplesForPredicateAndObject(ctx, ts[0].Predicate(), ts[0].Object(), storage.DefaultLookup, trps); err != nil {
				t.Fatal(err)
			}
		}()
		var ts []*triple.Triple
		for t := range trps {
			ts = append(ts, t)
		}
		return ts
	}

	og := triples()
	for i, max := 0, 100; i < max; i++ {
		if got, want := triples(), og; !reflect.DeepEqual(got, want) {
			t.Fatalf("failed to returned the right triples; got %v, want %v", got, want)
		}
	}
}

func TestExist(t *testing.T) {
	ctx, sm := buildtMemoizedStore(t)
	ts := buildTriples(t)

	g, err := sm.Graph(ctx, "?test")
	if err != nil {
		t.Fatal(err)
	}

	// Query the fist round.
	boolean := func() bool {
		b, err := g.Exist(ctx, ts[0])
		if err != nil {
			t.Fatal(err)
		}
		return b
	}

	og := boolean()
	for i, max := 0, 100; i < max; i++ {
		if got, want := boolean(), og; !reflect.DeepEqual(got, want) {
			t.Fatalf("failed to returned the right boolean; got %v, want %v", got, want)
		}
	}
}

func TestTriples(t *testing.T) {
	ctx, sm := buildtMemoizedStore(t)

	g, err := sm.Graph(ctx, "?test")
	if err != nil {
		t.Fatal(err)
	}

	// Query the fist round.
	triples := func() []*triple.Triple {
		trps := make(chan *triple.Triple)
		go func() {
			if err := g.Triples(ctx, storage.DefaultLookup, trps); err != nil {
				t.Fatal(err)
			}
		}()
		var ts []*triple.Triple
		for t := range trps {
			ts = append(ts, t)
		}
		return ts
	}

	og := triples()
	for i, max := 0, 100; i < max; i++ {
		if got, want := triples(), og; !reflect.DeepEqual(got, want) {
			t.Fatalf("failed to returned the right triples; got %v, want %v", got, want)
		}
	}
}
