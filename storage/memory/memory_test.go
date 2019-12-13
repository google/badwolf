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

package memory

import (
	"context"
	"reflect"
	"testing"
	"time"

	"github.com/google/badwolf/storage"
	"github.com/google/badwolf/triple"
	"github.com/google/badwolf/triple/literal"
	"github.com/google/badwolf/triple/node"
	"github.com/google/badwolf/triple/predicate"
)

func TestMemoryStore(t *testing.T) {
	s, ctx := NewStore(), context.Background()
	// Create a new graph.
	if _, err := s.NewGraph(ctx, "test"); err != nil {
		t.Errorf("memoryStore.NewGraph: should never fail to crate a graph; %s", err)
	}
	// Get an existing graph.
	if _, err := s.Graph(ctx, "test"); err != nil {
		t.Errorf("memoryStore.Graph: should never fail to get an existing graph; %s", err)
	}
	// Delete an existing graph.
	if err := s.DeleteGraph(ctx, "test"); err != nil {
		t.Errorf("memoryStore.DeleteGraph: should never fail to delete an existing graph; %s", err)
	}
	// Get a non existing graph.
	if _, err := s.Graph(ctx, "test"); err == nil {
		t.Errorf("memoryStore.Graph: should never succeed to get a non existing graph; %s", err)
	}
	// Delete an existing graph.
	if err := s.DeleteGraph(ctx, "test"); err == nil {
		t.Errorf("memoryStore.DeleteGraph: should never succed to delete a non existing graph; %s", err)
	}
}

func TestGraphNames(t *testing.T) {
	gs, ctx := []string{"?foo", "?bar", "?test"}, context.Background()
	s := NewStore()
	for _, g := range gs {
		if _, err := s.NewGraph(ctx, g); err != nil {
			t.Errorf("memoryStore.NewGraph: should never fail to crate a graph %s; %s", g, err)
		}
	}
	// To avoid blocking on the test. On a real usage of the driver you would like
	// to call the graph operation on a separated goroutine using a sync.WaitGroup
	// to collect the error code eventually.
	gns := make(chan string, len(gs))
	if err := s.GraphNames(ctx, gns); err != nil {
		t.Errorf("memoryStore.GraphNames: failed with error %v", err)
	}
	cnt := 0
	for g := range gns {
		found := false
		for _, gn := range gs {
			if g == gn {
				found = true
				break
			}
		}
		if !found {
			t.Errorf("memoryStore.GraphNames: failed to return the expected graph names; got %v", g)
		}
		cnt++
	}
	if got, want := cnt, len(gs); got != want {
		t.Errorf("memoryStore.GraphNames: failed to return the expected number of graph names; got %d, want %d", got, want)
	}
}

func TestDefaultLookupChecker(t *testing.T) {
	dlu := storage.DefaultLookup
	c := newChecker(dlu, nil)
	ip, err := predicate.NewImmutable("foo")
	if err != nil {
		t.Fatal(err)
	}
	tp, err := predicate.NewTemporal("bar", time.Now())
	if err != nil {
		t.Fatal(err)
	}
	if !c.CheckAndUpdate(ip) {
		t.Errorf("Immutable predicates should always validate with default lookup %v", dlu)
	}
	if !c.CheckAndUpdate(tp) {
		t.Errorf("Temporal predicates should always validate with default lookup %v", dlu)
	}
}

func TestLimitedItemsLookupChecker(t *testing.T) {
	blu := &storage.LookupOptions{MaxElements: 1}
	c := newChecker(blu, nil)
	ip, err := predicate.NewImmutable("foo")
	if err != nil {
		t.Fatal(err)
	}
	if !c.CheckAndUpdate(ip) {
		t.Errorf("The first predicate should always succeed on bounded lookup %v", blu)
	}
	for i := 0; i < 10; i++ {
		if c.CheckAndUpdate(ip) {
			t.Errorf("Bounded lookup %v should never succeed after being exahausted", blu)
		}
	}
}

func TestTemporalBoundedLookupChecker(t *testing.T) {
	lpa, err := predicate.Parse("\"foo\"@[2013-07-19T13:12:04.669618843-07:00]")
	if err != nil {
		t.Fatalf("Failed to parse fixture predicate with error %v", err)
	}
	mpa, err := predicate.Parse("\"foo\"@[2014-07-19T13:12:04.669618843-07:00]")
	if err != nil {
		t.Fatalf("Failed to parse fixture predicate with error %v", err)
	}
	upa, err := predicate.Parse("\"foo\"@[2015-07-19T13:12:04.669618843-07:00]")
	if err != nil {
		t.Fatalf("Failed to parse fixture predicate with error %v", err)
	}
	// Check lower bound
	lb, _ := lpa.TimeAnchor()
	blu := &storage.LookupOptions{LowerAnchor: lb}
	clu := newChecker(blu, nil)
	if !clu.CheckAndUpdate(mpa) {
		t.Errorf("Failed to reject invalid predicate %v by checker %v", mpa, clu)
	}
	lb, _ = mpa.TimeAnchor()
	blu = &storage.LookupOptions{LowerAnchor: lb}
	clu = newChecker(blu, nil)
	if clu.CheckAndUpdate(lpa) {
		t.Errorf("Failed to reject invalid predicate %v by checker %v", mpa, clu)
	}
	// Check upper bound.
	ub, _ := upa.TimeAnchor()
	buu := &storage.LookupOptions{UpperAnchor: ub}
	cuu := newChecker(buu, nil)
	if !cuu.CheckAndUpdate(mpa) {
		t.Errorf("Failed to reject invalid predicate %v by checker %v", mpa, cuu)
	}
	ub, _ = mpa.TimeAnchor()
	buu = &storage.LookupOptions{UpperAnchor: ub}
	cuu = newChecker(buu, nil)
	if cuu.CheckAndUpdate(upa) {
		t.Errorf("Failed to reject invalid predicate %v by checker %v", mpa, cuu)
	}
}

func TestTemporalExactChecker(t *testing.T) {
	lpa, err := predicate.Parse("\"foo\"@[2013-07-19T13:12:04.669618843-07:00]")
	if err != nil {
		t.Fatalf("Failed to parse fixture predicate with error %v", err)
	}
	mpa, err := predicate.Parse("\"foo\"@[2014-07-19T13:12:04.669618843-07:00]")
	if err != nil {
		t.Fatalf("Failed to parse fixture predicate with error %v", err)
	}
	upa, err := predicate.Parse("\"foo\"@[2015-07-19T13:12:04.669618843-07:00]")
	if err != nil {
		t.Fatalf("Failed to parse fixture predicate with error %v", err)
	}
	// Check lower bound
	lb, _ := lpa.TimeAnchor()
	blu := &storage.LookupOptions{LowerAnchor: lb}
	clu := newChecker(blu, mpa)
	if !clu.CheckAndUpdate(mpa) {
		t.Errorf("Failed to accept predicate %v by checker %v", mpa, clu)
	}
	lb, _ = mpa.TimeAnchor()
	blu = &storage.LookupOptions{LowerAnchor: lb}
	clu = newChecker(blu, mpa)
	if clu.CheckAndUpdate(lpa) {
		t.Errorf("Failed to reject invalid predicate %v by checker %v", mpa, clu)
	}
	// Check upper bound.
	ub, _ := upa.TimeAnchor()
	buu := &storage.LookupOptions{UpperAnchor: ub}
	cuu := newChecker(buu, mpa)
	if !cuu.CheckAndUpdate(mpa) {
		t.Errorf("Failed to reject invalid predicate %v by checker %v", mpa, cuu)
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

func getTestTriples(t *testing.T) []*triple.Triple {
	return createTriples(t, []string{
		"/u<john>\t\"knows\"@[]\t/u<mary>",
		"/u<john>\t\"knows\"@[]\t/u<peter>",
		"/u<john>\t\"knows\"@[]\t/u<alice>",
		"/u<mary>\t\"knows\"@[]\t/u<andrew>",
		"/u<mary>\t\"knows\"@[]\t/u<kim>",
		"/u<mary>\t\"knows\"@[]\t/u<alice>",
	})
}

func getTestTemporalTriples(t *testing.T) []*triple.Triple {
	return createTriples(t, []string{
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

func TestAddRemoveTriples(t *testing.T) {
	ts, ctx := getTestTriples(t), context.Background()
	g, _ := NewStore().NewGraph(ctx, "test")
	if err := g.AddTriples(ctx, ts); err != nil {
		t.Errorf("g.AddTriples(_) failed failed to add test triples with error %v", err)
	}
	if err := g.RemoveTriples(ctx, ts); err != nil {
		t.Errorf("g.RemoveTriples(_) failed failed to remove test triples with error %v", err)
	}
}

func TestObjects(t *testing.T) {
	ts, ctx := getTestTriples(t), context.Background()
	g, _ := NewStore().NewGraph(ctx, "test")
	if err := g.AddTriples(ctx, ts); err != nil {
		t.Errorf("g.AddTriples(_) failed failed to add test triples with error %v", err)
	}
	// To avoid blocking on the test. On a real usage of the driver you would like
	// to call the graph operation on a separated goroutine using a sync.WaitGroup
	// to collect the error code eventually.
	os := make(chan *triple.Object, 100)
	if err := g.Objects(ctx, ts[0].Subject(), ts[0].Predicate(), storage.DefaultLookup, os); err != nil {
		t.Errorf("g.Objects(%s, %s) failed with error %v", ts[0].Subject(), ts[0].Predicate(), err)
	}
	cnt := 0
	for o := range os {
		cnt++
		n, _ := o.Node()
		ty, id := n.Type().String(), n.ID().String()
		if ty != "/u" || (id != "mary" && id != "peter" && id != "alice") {
			t.Errorf("g.Objects(%s, %s) failed to return a valid object; returned %s instead", ts[0].Subject(), ts[0].Predicate(), n)
		}
	}
	if cnt != 3 {
		t.Errorf("g.Objects(%s, %s) failed to retrieve 3 objects, got %d instead", ts[0].Subject(), ts[0].Predicate(), cnt)
	}
}

func TestObjectsLastestTemporal(t *testing.T) {
	ts, ctx := getTestTemporalTriples(t), context.Background()
	g, _ := NewStore().NewGraph(ctx, "test")
	if err := g.AddTriples(ctx, ts); err != nil {
		t.Errorf("g.AddTriples(_) failed failed to add test triples with error %v", err)
	}
	// To avoid blocking on the test. On a real usage of the driver you would like
	// to call the graph operation on a separated goroutine using a sync.WaitGroup
	// to collect the error code eventually.
	os := make(chan *triple.Object, 100)
	lo := &storage.LookupOptions{LatestAnchor: true}
	if err := g.Objects(ctx, ts[0].Subject(), ts[0].Predicate(), lo, os); err != nil {
		t.Errorf("g.Objects(%s, %s) failed with error %v", ts[0].Subject(), ts[0].Predicate(), err)
	}
	cnt := 0
	for o := range os {
		cnt++
		n, _ := o.Node()
		ty, id := n.Type().String(), n.ID().String()
		if ty != "/u" || (id != "mary" && id != "peter" && id != "alice") {
			t.Errorf("g.Objects(%s, %s) failed to return a valid object; returned %s instead", ts[0].Subject(), ts[0].Predicate(), n)
		}
	}
	if cnt != 1 {
		t.Errorf("g.Objects(%s, %s) failed to retrieve 3 objects, got %d instead", ts[0].Subject(), ts[0].Predicate(), cnt)
	}
}

func TestSubjects(t *testing.T) {
	ts, ctx := getTestTriples(t), context.Background()
	g, _ := NewStore().NewGraph(ctx, "test")
	if err := g.AddTriples(ctx, ts); err != nil {
		t.Errorf("g.AddTriples(_) failed failed to add test triples with error %v", err)
	}
	// To avoid blocking on the test. On a real usage of the driver you would like
	// to call the graph operation on a separated goroutine using a sync.WaitGroup
	// to collect the error code eventually.
	ss := make(chan *node.Node, 100)
	if err := g.Subjects(ctx, ts[0].Predicate(), ts[0].Object(), storage.DefaultLookup, ss); err != nil {
		t.Errorf("g.Subjects(%s, %s) failed with error %v", ts[0].Predicate(), ts[0].Object(), err)
	}
	cnt := 0
	for s := range ss {
		cnt++
		ty, id := s.Type().String(), s.ID().String()
		if ty != "/u" || id != "john" {
			t.Errorf("g.Subjects(%s, %s) failed to return a valid subject; returned %s instead", ts[0].Predicate(), ts[0].Object(), s)
		}
	}
	if cnt != 1 {
		t.Errorf("g.Objects(%s, %s) failed to retrieve 1 objects, got %d instead", ts[0].Subject(), ts[0].Predicate(), cnt)
	}
}

func TestSubjectsLatestTemporal(t *testing.T) {
	ts, ctx := getTestTemporalTriples(t), context.Background()
	g, _ := NewStore().NewGraph(ctx, "test")
	if err := g.AddTriples(ctx, ts); err != nil {
		t.Errorf("g.AddTriples(_) failed failed to add test triples with error %v", err)
	}
	// To avoid blocking on the test. On a real usage of the driver you would like
	// to call the graph operation on a separated goroutine using a sync.WaitGroup
	// to collect the error code eventually.
	ss := make(chan *node.Node, 100)
	lo := &storage.LookupOptions{LatestAnchor: true}
	if err := g.Subjects(ctx, ts[0].Predicate(), ts[0].Object(), lo, ss); err != nil {
		t.Errorf("g.Subjects(%s, %s) failed with error %v", ts[0].Predicate(), ts[0].Object(), err)
	}
	cnt := 0
	for s := range ss {
		cnt++
		ty, id := s.Type().String(), s.ID().String()
		if ty != "/u" || id != "john" {
			t.Errorf("g.Subjects(%s, %s) failed to return a valid subject; returned %s instead", ts[0].Predicate(), ts[0].Object(), s)
		}
	}
	if cnt != 1 {
		t.Errorf("g.Objects(%s, %s) failed to retrieve 1 objects, got %d instead", ts[0].Subject(), ts[0].Predicate(), cnt)
	}
}

func TestPredicatesForSubjectAndObject(t *testing.T) {
	ts, ctx := getTestTriples(t), context.Background()
	g, _ := NewStore().NewGraph(ctx, "test")
	if err := g.AddTriples(ctx, ts); err != nil {
		t.Errorf("g.AddTriples(_) failed failed to add test triples with error %v", err)
	}
	// To avoid blocking on the test. On a real usage of the driver you would like
	// to call the graph operation on a separated goroutine using a sync.WaitGroup
	// to collect the error code eventually.
	ps := make(chan *predicate.Predicate, 100)
	if err := g.PredicatesForSubjectAndObject(ctx, ts[0].Subject(), ts[0].Object(), storage.DefaultLookup, ps); err != nil {
		t.Errorf("g.PredicatesForSubjectAndObject(%s, %s) failed with error %v", ts[0].Subject(), ts[0].Object(), err)
	}
	cnt := 0
	for p := range ps {
		cnt++
		if p.Type() != predicate.Immutable || p.ID() != "knows" {
			t.Errorf("g.PredicatesForSubjectAndObject(%s, %s) failed to return a valid subject; returned %s instead", ts[0].Subject(), ts[0].Object(), p)
		}
	}
	if cnt != 1 {
		t.Errorf("g.PredicatesForSubjectAndObject(%s, %s) failed to retrieve 1 predicate, got %d instead", ts[0].Subject(), ts[0].Object(), cnt)
	}
}
func TestPredicatesForSubjectAndObjectLatestTemporal(t *testing.T) {
	ts, ctx := getTestTemporalTriples(t), context.Background()
	g, _ := NewStore().NewGraph(ctx, "test")
	if err := g.AddTriples(ctx, ts); err != nil {
		t.Errorf("g.AddTriples(_) failed failed to add test triples with error %v", err)
	}
	// To avoid blocking on the test. On a real usage of the driver you would like
	// to call the graph operation on a separated goroutine using a sync.WaitGroup
	// to collect the error code eventually.
	ps := make(chan *predicate.Predicate, 100)
	lo := &storage.LookupOptions{LatestAnchor: true}
	if err := g.PredicatesForSubjectAndObject(ctx, ts[0].Subject(), ts[0].Object(), lo, ps); err != nil {
		t.Errorf("g.PredicatesForSubjectAndObject(%s, %s) failed with error %v", ts[0].Subject(), ts[0].Object(), err)
	}
	cnt := 0
	for p := range ps {
		cnt++
		if !reflect.DeepEqual(p.UUID(), ts[len(ts)-1].Predicate().UUID()) {
			t.Errorf("g.PredicatesForSubjectAndObject(%s, %s) failed to return a valid subject; returned %s instead", ts[0].Subject(), ts[0].Object(), p)
		}
	}
	if cnt != 1 {
		t.Errorf("g.PredicatesForSubjectAndObject(%s, %s) failed to retrieve 1 predicate, got %d instead", ts[0].Subject(), ts[0].Object(), cnt)
	}
}

func TestPredicatesForSubject(t *testing.T) {
	ts, ctx := getTestTriples(t), context.Background()
	g, _ := NewStore().NewGraph(ctx, "test")
	if err := g.AddTriples(ctx, ts); err != nil {
		t.Errorf("g.AddTriples(_) failed failed to add test triples with error %v", err)
	}
	// To avoid blocking on the test. On a real usage of the driver you would like
	// to call the graph operation on a separated goroutine using a sync.WaitGroup
	// to collect the error code eventually.
	ps := make(chan *predicate.Predicate, 100)
	if err := g.PredicatesForSubject(ctx, ts[0].Subject(), storage.DefaultLookup, ps); err != nil {
		t.Errorf("g.PredicatesForSubject(%s) failed with error %v", ts[0].Subject(), err)
	}
	cnt := 0
	for p := range ps {
		cnt++
		if p.Type() != predicate.Immutable || p.ID() != "knows" {
			t.Errorf("g.PredicatesForSubject(%s) failed to return a valid predicate; returned %s instead", ts[0].Subject(), p)
		}
	}
	if cnt != 3 {
		t.Errorf("g.PredicatesForSubjectAndObject(%s) failed to retrieve 3 predicates, got %d instead", ts[0].Subject(), cnt)
	}
}

func TestPredicatesForSubjectLatestTemporal(t *testing.T) {
	ts, ctx := getTestTemporalTriples(t), context.Background()
	g, _ := NewStore().NewGraph(ctx, "test")
	if err := g.AddTriples(ctx, ts); err != nil {
		t.Errorf("g.AddTriples(_) failed failed to add test triples with error %v", err)
	}
	// To avoid blocking on the test. On a real usage of the driver you would like
	// to call the graph operation on a separated goroutine using a sync.WaitGroup
	// to collect the error code eventually.
	ps := make(chan *predicate.Predicate, 100)
	lo := &storage.LookupOptions{LatestAnchor: true}
	if err := g.PredicatesForSubject(ctx, ts[0].Subject(), lo, ps); err != nil {
		t.Errorf("g.PredicatesForSubject(%s) failed with error %v", ts[0].Subject(), err)
	}
	cnt := 0
	for p := range ps {
		cnt++
		if !reflect.DeepEqual(p.UUID(), ts[len(ts)-1].Predicate().UUID()) {
			t.Errorf("g.PredicatesForSubject(%s) failed to return a valid predicate; returned %s instead", ts[0].Subject(), p)
		}
	}
	if cnt != 1 {
		t.Errorf("g.PredicatesForSubjectAndObject(%s) failed to retrieve 3 predicates, got %d instead", ts[0].Subject(), cnt)
	}
}

func TestPredicatesForObject(t *testing.T) {
	ts, ctx := getTestTriples(t), context.Background()
	g, _ := NewStore().NewGraph(ctx, "test")
	if err := g.AddTriples(ctx, ts); err != nil {
		t.Errorf("g.AddTriples(_) failed failed to add test triples with error %v", err)
	}
	// To avoid blocking on the test. On a real usage of the driver you would like
	// to call the graph operation on a separated goroutine using a sync.WaitGroup
	// to collect the error code eventually.
	ps := make(chan *predicate.Predicate, 100)
	if err := g.PredicatesForObject(ctx, ts[0].Object(), storage.DefaultLookup, ps); err != nil {
		t.Errorf("g.PredicatesForObject(%s) failed with error %v", ts[0].Object(), err)
	}
	cnt := 0
	for p := range ps {
		cnt++
		if p.Type() != predicate.Immutable || p.ID() != "knows" {
			t.Errorf("g.PredicatesForObject(%s) failed to return a valid predicate; returned %s instead", ts[0].Object(), p)
		}
	}
	if cnt != 1 {
		t.Errorf("g.PredicatesForObject(%s) failed to retrieve 1 predicate, got %d instead", ts[0].Object(), cnt)
	}
}
func TestPredicatesForObjectLatestTemporal(t *testing.T) {
	ts, ctx := getTestTemporalTriples(t), context.Background()
	g, _ := NewStore().NewGraph(ctx, "test")
	if err := g.AddTriples(ctx, ts); err != nil {
		t.Errorf("g.AddTriples(_) failed failed to add test triples with error %v", err)
	}
	// To avoid blocking on the test. On a real usage of the driver you would like
	// to call the graph operation on a separated goroutine using a sync.WaitGroup
	// to collect the error code eventually.
	ps := make(chan *predicate.Predicate, 100)
	lo := &storage.LookupOptions{LatestAnchor: true}
	if err := g.PredicatesForObject(ctx, ts[0].Object(), lo, ps); err != nil {
		t.Errorf("g.PredicatesForObject(%s) failed with error %v", ts[0].Object(), err)
	}
	cnt := 0
	for p := range ps {
		cnt++
		if !reflect.DeepEqual(p.UUID(), ts[len(ts)-1].Predicate().UUID()) {
			t.Errorf("g.PredicatesForObject(%s) failed to return a valid predicate; returned %s instead", ts[0].Object(), p)
		}
	}
	if cnt != 1 {
		t.Errorf("g.PredicatesForObject(%s) failed to retrieve 1 predicate, got %d instead", ts[0].Object(), cnt)
	}
}

func TestTriplesForSubject(t *testing.T) {
	ts, ctx := getTestTriples(t), context.Background()
	g, _ := NewStore().NewGraph(ctx, "test")
	if err := g.AddTriples(ctx, ts); err != nil {
		t.Errorf("g.AddTriples(_) failed failed to add test triples with error %v", err)
	}
	// To avoid blocking on the test. On a real usage of the driver you would like
	// to call the graph operation on a separated goroutine using a sync.WaitGroup
	// to collect the error code eventually.
	trpls := make(chan *triple.Triple, 100)
	if err := g.TriplesForSubject(ctx, ts[0].Subject(), storage.DefaultLookup, trpls); err != nil {
		t.Errorf("g.TriplesForSubject(%s) failed with error %v", ts[0].Subject(), err)
	}
	cnt := 0
	for range trpls {
		cnt++
	}
	if cnt != 3 {
		t.Errorf("g.triplesForSubject(%s) failed to retrieve 3 predicates, got %d instead", ts[0].Subject(), cnt)
	}
}

func TestTriplesForSubjectLatestTemporal(t *testing.T) {
	ts, ctx := getTestTemporalTriples(t), context.Background()
	g, _ := NewStore().NewGraph(ctx, "test")
	if err := g.AddTriples(ctx, ts); err != nil {
		t.Errorf("g.AddTriples(_) failed failed to add test triples with error %v", err)
	}
	// To avoid blocking on the test. On a real usage of the driver you would like
	// to call the graph operation on a separated goroutine using a sync.WaitGroup
	// to collect the error code eventually.
	trpls := make(chan *triple.Triple, 100)
	lo := &storage.LookupOptions{LatestAnchor: true}
	if err := g.TriplesForSubject(ctx, ts[0].Subject(), lo, trpls); err != nil {
		t.Errorf("g.TriplesForSubject(%s) failed with error %v", ts[0].Subject(), err)
	}
	cnt := 0
	for rts := range trpls {
		cnt++
		if !reflect.DeepEqual(rts.Predicate().UUID(), ts[len(ts)-1].Predicate().UUID()) {
			t.Errorf("g.PredicatesForObject(%s) failed to return a valid predicate; returned %s instead", ts[0].Object(), rts.Predicate())
		}
	}
	if cnt != 1 {
		t.Errorf("g.triplesForSubject(%s) failed to retrieve 3 predicates, got %d instead", ts[0].Subject(), cnt)
	}
}

func TestTriplesForPredicate(t *testing.T) {
	ts, ctx := getTestTriples(t), context.Background()
	g, _ := NewStore().NewGraph(ctx, "test")
	if err := g.AddTriples(ctx, ts); err != nil {
		t.Errorf("g.AddTriples(_) failed failed to add test triples with error %v", err)
	}
	// To avoid blocking on the test. On a real usage of the driver you would like
	// to call the graph operation on a separated goroutine using a sync.WaitGroup
	// to collect the error code eventually.
	trpls := make(chan *triple.Triple, 100)
	if err := g.TriplesForPredicate(ctx, ts[0].Predicate(), storage.DefaultLookup, trpls); err != nil {
		t.Errorf("g.TriplesForPredicate(%s) failed with error %v", ts[0].Subject(), err)
	}
	cnt := 0
	for range trpls {
		cnt++
	}
	if cnt != 6 {
		t.Errorf("g.triplesForPredicate(%s) failed to retrieve 3 predicates, got %d instead", ts[0].Predicate(), cnt)
	}
}
func TestTriplesForPredicateLatestTemporal(t *testing.T) {
	ts, ctx := getTestTemporalTriples(t), context.Background()
	g, _ := NewStore().NewGraph(ctx, "test")
	if err := g.AddTriples(ctx, ts); err != nil {
		t.Errorf("g.AddTriples(_) failed failed to add test triples with error %v", err)
	}
	// To avoid blocking on the test. On a real usage of the driver you would like
	// to call the graph operation on a separated goroutine using a sync.WaitGroup
	// to collect the error code eventually.
	trpls := make(chan *triple.Triple, 100)
	lo := &storage.LookupOptions{LatestAnchor: true}
	if err := g.TriplesForPredicate(ctx, ts[0].Predicate(), lo, trpls); err != nil {
		t.Errorf("g.TriplesForPredicate(%s) failed with error %v", ts[0].Subject(), err)
	}
	cnt := 0
	for rts := range trpls {
		cnt++
		if !reflect.DeepEqual(rts.Predicate().UUID(), ts[len(ts)-1].Predicate().UUID()) {
			t.Errorf("g.PredicatesForObject(%s) failed to return a valid predicate; returned %s instead", ts[0].Object(), rts.Predicate())
		}
	}
	if cnt != 1 {
		t.Errorf("g.triplesForPredicate(%s) failed to retrieve 3 predicates, got %d instead", ts[0].Predicate(), cnt)
	}
}

func TestTriplesForObject(t *testing.T) {
	ts, ctx := getTestTriples(t), context.Background()
	g, _ := NewStore().NewGraph(ctx, "test")
	if err := g.AddTriples(ctx, ts); err != nil {
		t.Errorf("g.AddTriples(_) failed failed to add test triples with error %v", err)
	}
	// To avoid blocking on the test. On a real usage of the driver you would like
	// to call the graph operation on a separated goroutine using a sync.WaitGroup
	// to collect the error code eventually.
	trpls := make(chan *triple.Triple, 100)
	if err := g.TriplesForObject(ctx, ts[0].Object(), storage.DefaultLookup, trpls); err != nil {
		t.Errorf("g.TriplesForObject(%s) failed with error %v", ts[0].Object(), err)
	}
	cnt := 0
	for range trpls {
		cnt++
	}
	if cnt != 1 {
		t.Errorf("g.TriplesForObject(%s) failed to retrieve 1 predicates, got %d instead", ts[0].Object(), cnt)
	}
}

func TestTriplesForObjectLatestTemporal(t *testing.T) {
	ts, ctx := getTestTemporalTriples(t), context.Background()
	g, _ := NewStore().NewGraph(ctx, "test")
	if err := g.AddTriples(ctx, ts); err != nil {
		t.Errorf("g.AddTriples(_) failed failed to add test triples with error %v", err)
	}
	// To avoid blocking on the test. On a real usage of the driver you would like
	// to call the graph operation on a separated goroutine using a sync.WaitGroup
	// to collect the error code eventually.
	trpls := make(chan *triple.Triple, 100)
	lo := &storage.LookupOptions{LatestAnchor: true}
	if err := g.TriplesForObject(ctx, ts[0].Object(), lo, trpls); err != nil {
		t.Errorf("g.TriplesForObject(%s) failed with error %v", ts[0].Object(), err)
	}
	cnt := 0
	for rts := range trpls {
		cnt++
		if !reflect.DeepEqual(rts.Predicate().UUID(), ts[len(ts)-1].Predicate().UUID()) {
			t.Errorf("g.PredicatesForObject(%s) failed to return a valid predicate; returned %s instead", ts[0].Object(), rts.Predicate())
		}
	}
	if cnt != 1 {
		t.Errorf("g.TriplesForObject(%s) failed to retrieve 1 predicates, got %d instead", ts[0].Object(), cnt)
	}
}

func mustParse(t string) *time.Time {
	r, err := time.Parse(time.RFC3339Nano, t)
	if err != nil {
		panic(err)
	}
	return &r
}

func TestTriplesForObjectWithLimit(t *testing.T) {
	ts := createTriples(t, []string{
		"/u<bob>\t\"kissed\"@[2015-01-01T00:00:00-09:00]\t/u<mary>",
		"/u<bob>\t\"kissed\"@[2015-02-01T00:00:00-09:00]\t/u<mary>",
		"/u<bob>\t\"kissed\"@[2015-03-01T00:00:00-09:00]\t/u<mary>",
		"/u<bob>\t\"kissed\"@[2015-04-01T00:00:00-09:00]\t/u<mary>",
		"/u<bob>\t\"kissed\"@[2015-05-01T00:00:00-09:00]\t/u<mary>",
		"/u<bob>\t\"kissed\"@[2015-06-01T00:00:00-09:00]\t/u<mary>",
	})
	ctx := context.Background()
	g, _ := NewStore().NewGraph(ctx, "test")
	if err := g.AddTriples(ctx, ts); err != nil {
		t.Errorf("g.AddTriples(_) failed failed to add test triples with error %v", err)
	}
	// To avoid blocking on the test. On a real usage of the driver you would like
	// to call the graph operation on a separated goroutine using a sync.WaitGroup
	// to collect the error code eventually.
	trpls := make(chan *triple.Triple, 100)
	lo := &storage.LookupOptions{
		MaxElements: 2,
		LowerAnchor: mustParse("2015-04-01T00:00:00-08:00"),
		UpperAnchor: mustParse("2015-06-01T00:00:00-10:00"),
	}
	if err := g.TriplesForObject(ctx, ts[0].Object(), lo, trpls); err != nil {
		t.Errorf("g.TriplesForObject(%s) failed with error %v", ts[0].Object(), err)
	}
	cnt := 0
	for tr := range trpls {
		ta, err := tr.Predicate().TimeAnchor()
		if err != nil {
			t.Error(err)
			continue
		}
		if ta.Before(*lo.LowerAnchor) || ta.After(*lo.UpperAnchor) {
			t.Errorf("g.TriplesForObject(%s) unexpected triple receved: %s", ts[0].Object(), tr)
		}
		cnt++
	}
	if cnt != lo.MaxElements {
		t.Errorf("g.TriplesForObject(%s) failed to retrieve 2 triples, got %d instead", ts[0].Object(), cnt)
	}
}

func TestTriplesForSubjectAndPredicate(t *testing.T) {
	ts, ctx := getTestTriples(t), context.Background()
	g, _ := NewStore().NewGraph(ctx, "test")
	if err := g.AddTriples(ctx, ts); err != nil {
		t.Errorf("g.AddTriples(_) failed failed to add test triples with error %v", err)
	}
	// To avoid blocking on the test. On a real usage of the driver you would like
	// to call the graph operation on a separated goroutine using a sync.WaitGroup
	// to collect the error code eventually.
	trpls := make(chan *triple.Triple, 100)
	if err := g.TriplesForSubjectAndPredicate(ctx, ts[0].Subject(), ts[0].Predicate(), storage.DefaultLookup, trpls); err != nil {
		t.Errorf("g.TriplesForSubjectAndPredicate(%s, %s) failed with error %v", ts[0].Subject(), ts[0].Predicate(), err)
	}
	cnt := 0
	for range trpls {
		cnt++
	}
	if cnt != 3 {
		t.Errorf("g.TriplesForSubjectAndPredicate(%s, %s) failed to retrieve 3 predicates, got %d instead", ts[0].Subject(), ts[0].Predicate(), cnt)
	}
}
func TestTriplesForSubjectAndPredicateLatestTemporal(t *testing.T) {
	ts, ctx := getTestTemporalTriples(t), context.Background()
	g, _ := NewStore().NewGraph(ctx, "test")
	if err := g.AddTriples(ctx, ts); err != nil {
		t.Errorf("g.AddTriples(_) failed failed to add test triples with error %v", err)
	}
	// To avoid blocking on the test. On a real usage of the driver you would like
	// to call the graph operation on a separated goroutine using a sync.WaitGroup
	// to collect the error code eventually.
	trpls := make(chan *triple.Triple, 100)
	lo := &storage.LookupOptions{LatestAnchor: true}
	if err := g.TriplesForSubjectAndPredicate(ctx, ts[0].Subject(), ts[0].Predicate(), lo, trpls); err != nil {
		t.Errorf("g.TriplesForSubjectAndPredicate(%s, %s) failed with error %v", ts[0].Subject(), ts[0].Predicate(), err)
	}
	cnt := 0
	for rts := range trpls {
		cnt++
		if !reflect.DeepEqual(rts.Predicate().UUID(), ts[len(ts)-1].Predicate().UUID()) {
			t.Errorf("g.PredicatesForObject(%s) failed to return a valid predicate; returned %s instead", ts[0].Object(), rts.Predicate())
		}
	}
	if cnt != 1 {
		t.Errorf("g.TriplesForSubjectAndPredicate(%s, %s) failed to retrieve 3 predicates, got %d instead", ts[0].Subject(), ts[0].Predicate(), cnt)
	}
}

func TestTriplesForPredicateAndObject(t *testing.T) {
	ts, ctx := getTestTriples(t), context.Background()
	g, _ := NewStore().NewGraph(ctx, "test")
	if err := g.AddTriples(ctx, ts); err != nil {
		t.Errorf("g.AddTriples(_) failed failed to add test triples with error %v", err)
	}
	// To avoid blocking on the test. On a real usage of the driver you would like
	// to call the graph operation on a separated goroutine using a sync.WaitGroup
	// to collect the error code eventually.
	trpls := make(chan *triple.Triple, 100)
	if err := g.TriplesForPredicateAndObject(ctx, ts[0].Predicate(), ts[0].Object(), storage.DefaultLookup, trpls); err != nil {
		t.Errorf("g.TriplesForPredicateAndObject(%s, %s) failed with error %v", ts[0].Predicate(), ts[0].Object(), err)
	}
	cnt := 0
	for range trpls {
		cnt++
	}
	if cnt != 1 {
		t.Errorf("g.TriplesForPredicateAndObject(%s, %s) failed to retrieve 1 predicates, got %d instead", ts[0].Predicate(), ts[0].Object(), cnt)
	}
}

func TestTriplesForPredicateAndObjectLatestTemporal(t *testing.T) {
	ts, ctx := getTestTemporalTriples(t), context.Background()
	g, _ := NewStore().NewGraph(ctx, "test")
	if err := g.AddTriples(ctx, ts); err != nil {
		t.Errorf("g.AddTriples(_) failed failed to add test triples with error %v", err)
	}
	// To avoid blocking on the test. On a real usage of the driver you would like
	// to call the graph operation on a separated goroutine using a sync.WaitGroup
	// to collect the error code eventually.
	trpls := make(chan *triple.Triple, 100)
	lo := &storage.LookupOptions{LatestAnchor: true}
	if err := g.TriplesForPredicateAndObject(ctx, ts[0].Predicate(), ts[0].Object(), lo, trpls); err != nil {
		t.Errorf("g.TriplesForPredicateAndObject(%s, %s) failed with error %v", ts[0].Predicate(), ts[0].Object(), err)
	}
	cnt := 0
	for rts := range trpls {
		cnt++
		if !reflect.DeepEqual(rts.Predicate().UUID(), ts[len(ts)-1].Predicate().UUID()) {
			t.Errorf("g.PredicatesForObject(%s) failed to return a valid predicate; returned %s instead", ts[0].Object(), rts.Predicate())
		}
	}
	if cnt != 1 {
		t.Errorf("g.TriplesForPredicateAndObject(%s, %s) failed to retrieve 1 predicates, got %d instead", ts[0].Predicate(), ts[0].Object(), cnt)
	}
}

func TestExists(t *testing.T) {
	ts, ctx := getTestTriples(t), context.Background()
	g, _ := NewStore().NewGraph(ctx, "test")
	if err := g.AddTriples(ctx, ts); err != nil {
		t.Errorf("g.AddTriples(_) failed failed to add test triples with error %v", err)
	}
	for _, trpl := range ts {
		b, err := g.Exist(ctx, trpl)
		if err != nil {
			t.Errorf("g.Exist should have not failed for triple %s with error %s", trpl, err)
		}
		if !b {
			t.Errorf("g.Exist should have not failed for triple %s", trpl)
		}
	}
}

func TestTriples(t *testing.T) {
	ts, ctx := getTestTriples(t), context.Background()
	g, _ := NewStore().NewGraph(ctx, "test")
	if err := g.AddTriples(ctx, ts); err != nil {
		t.Errorf("g.AddTriples(_) failed failed to add test triples with error %v", err)
	}
	// To avoid blocking on the test. On a real usage of the driver you would like
	// to call the graph operation on a separated goroutine using a sync.WaitGroup
	// to collect the error code eventually.
	trpls := make(chan *triple.Triple, 100)
	if err := g.Triples(ctx, storage.DefaultLookup, trpls); err != nil {
		t.Fatal(err)
	}
	cnt := 0
	for range trpls {
		cnt++
	}
	if cnt != 6 {
		t.Errorf("g.TriplesForPredicateAndObject(%s, %s) failed to retrieve 1 predicates, got %d instead", ts[0].Predicate(), ts[0].Object(), cnt)
	}
}

func TestTriplesLastestTemporal(t *testing.T) {
	ts, ctx := getTestTemporalTriples(t), context.Background()
	g, _ := NewStore().NewGraph(ctx, "test")
	if err := g.AddTriples(ctx, ts); err != nil {
		t.Errorf("g.AddTriples(_) failed failed to add test triples with error %v", err)
	}
	// To avoid blocking on the test. On a real usage of the driver you would like
	// to call the graph operation on a separated goroutine using a sync.WaitGroup
	// to collect the error code eventually.
	trpls := make(chan *triple.Triple, 100)
	lo := &storage.LookupOptions{LatestAnchor: true}
	if err := g.Triples(ctx, lo, trpls); err != nil {
		t.Fatal(err)
	}
	cnt := 0
	for rts := range trpls {
		cnt++
		if !reflect.DeepEqual(rts.Predicate().UUID(), ts[len(ts)-1].Predicate().UUID()) {
			t.Errorf("g.PredicatesForObject(%s) failed to return a valid predicate; returned %s instead", ts[0].Object(), rts.Predicate())
		}
	}
	if cnt != 1 {
		t.Errorf("g.TriplesForPredicateAndObject(%s, %s) failed to retrieve 1 predicates, got %d instead", ts[0].Predicate(), ts[0].Object(), cnt)
	}
}
