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

	"github.com/google/badwolf/bql/planner/filter"
	"github.com/google/badwolf/storage"
	"github.com/google/badwolf/tools/testutil"
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
	if !c.CheckGlobalTimeBounds(ip) {
		t.Errorf("Immutable predicates should always validate with default lookup %v", dlu)
	}
	if !c.CheckGlobalTimeBounds(tp) {
		t.Errorf("Temporal predicates should always validate with default lookup %v", dlu)
	}
}

func TestLimitedItemsLookupChecker(t *testing.T) {
	blu := &storage.LookupOptions{MaxElements: 1}
	c := newChecker(blu, nil)
	if !c.CheckLimitAndUpdate() {
		t.Errorf("The first call to CheckLimitAndUpdate() should always succeed on lookup %v that started with MaxElements set to 1", blu)
	}
	for i := 0; i < 10; i++ {
		if c.CheckLimitAndUpdate() {
			t.Errorf("Lookup %v should never succeed after being exahausted", blu)
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
	if !clu.CheckGlobalTimeBounds(mpa) {
		t.Errorf("Failed to reject invalid predicate %v by checker %v", mpa, clu)
	}
	lb, _ = mpa.TimeAnchor()
	blu = &storage.LookupOptions{LowerAnchor: lb}
	clu = newChecker(blu, nil)
	if clu.CheckGlobalTimeBounds(lpa) {
		t.Errorf("Failed to reject invalid predicate %v by checker %v", mpa, clu)
	}
	// Check upper bound.
	ub, _ := upa.TimeAnchor()
	buu := &storage.LookupOptions{UpperAnchor: ub}
	cuu := newChecker(buu, nil)
	if !cuu.CheckGlobalTimeBounds(mpa) {
		t.Errorf("Failed to reject invalid predicate %v by checker %v", mpa, cuu)
	}
	ub, _ = mpa.TimeAnchor()
	buu = &storage.LookupOptions{UpperAnchor: ub}
	cuu = newChecker(buu, nil)
	if cuu.CheckGlobalTimeBounds(upa) {
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
	if !clu.CheckGlobalTimeBounds(mpa) {
		t.Errorf("Failed to accept predicate %v by checker %v", mpa, clu)
	}
	lb, _ = mpa.TimeAnchor()
	blu = &storage.LookupOptions{LowerAnchor: lb}
	clu = newChecker(blu, mpa)
	if clu.CheckGlobalTimeBounds(lpa) {
		t.Errorf("Failed to reject invalid predicate %v by checker %v", mpa, clu)
	}
	// Check upper bound.
	ub, _ := upa.TimeAnchor()
	buu := &storage.LookupOptions{UpperAnchor: ub}
	cuu := newChecker(buu, mpa)
	if !cuu.CheckGlobalTimeBounds(mpa) {
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

func getTestTriplesFilter(t *testing.T) []*triple.Triple {
	return createTriples(t, []string{
		"/u<john>\t\"meet\"@[2012-04-10T04:21:00Z]\t/u<mary>",
		"/u<john>\t\"meet\"@[2013-04-10T04:21:00Z]\t/u<mary>",
		"/u<john>\t\"meet\"@[2014-04-10T04:21:00Z]\t/u<mary>",
		"/u<john>\t\"meet\"@[2014-04-10T04:21:00Z]\t/u<bob>",
		"/u<john>\t\"parent_of\"@[]\t/u<paul>",
		"/_<bn>\t\"_predicate\"@[]\t\"meet\"@[2020-04-10T04:21:00Z]",
		"/_<bn>\t\"_predicate\"@[]\t\"meet\"@[2021-04-10T04:21:00Z]",
		"/_<bn>\t\"_predicate\"@[]\t\"height_cm\"@[]",
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

func TestObjectsLatestAnchor(t *testing.T) {
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

func TestObjectsFilter(t *testing.T) {
	ts, ctx := getTestTriplesFilter(t), context.Background()
	g, _ := NewStore().NewGraph(ctx, "test")
	if err := g.AddTriples(ctx, ts); err != nil {
		t.Fatalf("g.AddTriples(_) failed to add test triples with error: %v", err)
	}

	testTable := []struct {
		id   string
		lo   *storage.LookupOptions
		s    *node.Node
		p    *predicate.Predicate
		want map[string]int
	}{
		{
			id:   "FILTER latest predicate",
			lo:   &storage.LookupOptions{FilterOptions: &filter.StorageOptions{Operation: filter.Latest, Field: filter.PredicateField}},
			s:    testutil.MustBuildNodeFromStrings(t, "/u", "john"),
			p:    testutil.MustBuildPredicate(t, `"meet"@[2012-04-10T04:21:00Z]`),
			want: map[string]int{"/u<mary>": 1},
		},
		{
			id:   "FILTER latest predicate duplicate timestamp",
			lo:   &storage.LookupOptions{FilterOptions: &filter.StorageOptions{Operation: filter.Latest, Field: filter.PredicateField}},
			s:    testutil.MustBuildNodeFromStrings(t, "/u", "john"),
			p:    testutil.MustBuildPredicate(t, `"meet"@[2014-04-10T04:21:00Z]`),
			want: map[string]int{"/u<mary>": 1, "/u<bob>": 1},
		},
		{
			id:   "FILTER latest object",
			lo:   &storage.LookupOptions{FilterOptions: &filter.StorageOptions{Operation: filter.Latest, Field: filter.ObjectField}},
			s:    testutil.MustBuildNodeFromStrings(t, "/_", "bn"),
			p:    testutil.MustBuildPredicate(t, `"_predicate"@[]`),
			want: map[string]int{`"meet"@[2021-04-10T04:21:00Z]`: 1},
		},
		{
			id:   "FILTER isImmutable predicate",
			lo:   &storage.LookupOptions{FilterOptions: &filter.StorageOptions{Operation: filter.IsImmutable, Field: filter.PredicateField}},
			s:    testutil.MustBuildNodeFromStrings(t, "/u", "john"),
			p:    testutil.MustBuildPredicate(t, `"parent_of"@[]`),
			want: map[string]int{"/u<paul>": 1},
		},
		{
			id:   "FILTER isImmutable object",
			lo:   &storage.LookupOptions{FilterOptions: &filter.StorageOptions{Operation: filter.IsImmutable, Field: filter.ObjectField}},
			s:    testutil.MustBuildNodeFromStrings(t, "/_", "bn"),
			p:    testutil.MustBuildPredicate(t, `"_predicate"@[]`),
			want: map[string]int{`"height_cm"@[]`: 1},
		},
		{
			id:   "FILTER isTemporal predicate",
			lo:   &storage.LookupOptions{FilterOptions: &filter.StorageOptions{Operation: filter.IsTemporal, Field: filter.PredicateField}},
			s:    testutil.MustBuildNodeFromStrings(t, "/u", "john"),
			p:    testutil.MustBuildPredicate(t, `"parent_of"@[]`),
			want: map[string]int{},
		},
		{
			id:   "FILTER isTemporal object",
			lo:   &storage.LookupOptions{FilterOptions: &filter.StorageOptions{Operation: filter.IsTemporal, Field: filter.ObjectField}},
			s:    testutil.MustBuildNodeFromStrings(t, "/_", "bn"),
			p:    testutil.MustBuildPredicate(t, `"_predicate"@[]`),
			want: map[string]int{`"meet"@[2020-04-10T04:21:00Z]`: 1, `"meet"@[2021-04-10T04:21:00Z]`: 1},
		},
		{
			id:   "FILTER latest between",
			lo:   &storage.LookupOptions{LowerAnchor: testutil.MustBuildTime(t, "2012-04-10T04:21:00Z"), UpperAnchor: testutil.MustBuildTime(t, "2013-04-10T04:21:00Z"), FilterOptions: &filter.StorageOptions{Operation: filter.Latest, Field: filter.PredicateField}},
			s:    testutil.MustBuildNodeFromStrings(t, "/u", "john"),
			p:    testutil.MustBuildPredicate(t, `"meet"@[2013-04-10T04:21:00Z]`),
			want: map[string]int{"/u<mary>": 1},
		},
	}

	for _, entry := range testTable {
		t.Run(entry.id, func(t *testing.T) {
			// To avoid blocking on the test we use a buffered channel of size 100. On a real
			// usage of the driver you would like to call the graph operation on a separated
			// goroutine using a sync.WaitGroup to collect the error code eventually.
			os := make(chan *triple.Object, 100)
			s := entry.s
			p := entry.p
			if err := g.Objects(ctx, s, p, entry.lo, os); err != nil {
				t.Fatalf("g.Objects(%s, %s, %s) = %v; want nil", s, p, entry.lo, err)
			}
			for o := range os {
				oStr := o.String()
				if _, ok := entry.want[oStr]; !ok {
					t.Fatalf("g.Objects(%s, %s, %s) retrieved unexpected %s", s, p, entry.lo, oStr)
				}
				entry.want[oStr] = entry.want[oStr] - 1
				if entry.want[oStr] == 0 {
					delete(entry.want, oStr)
				}
			}
			if len(entry.want) != 0 {
				t.Errorf("g.Objects(%s, %s, %s) failed to retrieve some expected elements: %v", s, p, entry.lo, entry.want)
			}
		})
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

func TestSubjectsLatestAnchor(t *testing.T) {
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

func TestSubjectsFilter(t *testing.T) {
	ts, ctx := getTestTriplesFilter(t), context.Background()
	g, _ := NewStore().NewGraph(ctx, "test")
	if err := g.AddTriples(ctx, ts); err != nil {
		t.Fatalf("g.AddTriples(_) failed to add test triples with error: %v", err)
	}

	testTable := []struct {
		id   string
		lo   *storage.LookupOptions
		p    *predicate.Predicate
		o    *triple.Object
		want map[string]int
	}{
		{
			id:   "FILTER latest predicate",
			lo:   &storage.LookupOptions{FilterOptions: &filter.StorageOptions{Operation: filter.Latest, Field: filter.PredicateField}},
			p:    testutil.MustBuildPredicate(t, `"meet"@[2012-04-10T04:21:00Z]`),
			o:    triple.NewNodeObject(testutil.MustBuildNodeFromStrings(t, "/u", "mary")),
			want: map[string]int{"/u<john>": 1},
		},
		{
			id:   "FILTER latest predicate duplicate timestamp",
			lo:   &storage.LookupOptions{FilterOptions: &filter.StorageOptions{Operation: filter.Latest, Field: filter.PredicateField}},
			p:    testutil.MustBuildPredicate(t, `"meet"@[2014-04-10T04:21:00Z]`),
			o:    triple.NewNodeObject(testutil.MustBuildNodeFromStrings(t, "/u", "mary")),
			want: map[string]int{"/u<john>": 1},
		},
		{
			id:   "FILTER latest object",
			lo:   &storage.LookupOptions{FilterOptions: &filter.StorageOptions{Operation: filter.Latest, Field: filter.ObjectField}},
			p:    testutil.MustBuildPredicate(t, `"_predicate"@[]`),
			o:    triple.NewPredicateObject(testutil.MustBuildPredicate(t, `"meet"@[2020-04-10T04:21:00Z]`)),
			want: map[string]int{"/_<bn>": 1},
		},
		{
			id:   "FILTER isImmutable predicate",
			lo:   &storage.LookupOptions{FilterOptions: &filter.StorageOptions{Operation: filter.IsImmutable, Field: filter.PredicateField}},
			p:    testutil.MustBuildPredicate(t, `"parent_of"@[]`),
			o:    triple.NewNodeObject(testutil.MustBuildNodeFromStrings(t, "/u", "paul")),
			want: map[string]int{"/u<john>": 1},
		},
		{
			id:   "FILTER isImmutable object",
			lo:   &storage.LookupOptions{FilterOptions: &filter.StorageOptions{Operation: filter.IsImmutable, Field: filter.ObjectField}},
			p:    testutil.MustBuildPredicate(t, `"_predicate"@[]`),
			o:    triple.NewPredicateObject(testutil.MustBuildPredicate(t, `"height_cm"@[]`)),
			want: map[string]int{"/_<bn>": 1},
		},
		{
			id:   "FILTER isTemporal predicate",
			lo:   &storage.LookupOptions{FilterOptions: &filter.StorageOptions{Operation: filter.IsTemporal, Field: filter.PredicateField}},
			p:    testutil.MustBuildPredicate(t, `"parent_of"@[]`),
			o:    triple.NewNodeObject(testutil.MustBuildNodeFromStrings(t, "/u", "paul")),
			want: map[string]int{},
		},
		{
			id:   "FILTER isTemporal object",
			lo:   &storage.LookupOptions{FilterOptions: &filter.StorageOptions{Operation: filter.IsTemporal, Field: filter.ObjectField}},
			p:    testutil.MustBuildPredicate(t, `"_predicate"@[]`),
			o:    triple.NewPredicateObject(testutil.MustBuildPredicate(t, `"height_cm"@[]`)),
			want: map[string]int{},
		},
		{
			id:   "FILTER latest between",
			lo:   &storage.LookupOptions{LowerAnchor: testutil.MustBuildTime(t, "2012-04-10T04:21:00Z"), UpperAnchor: testutil.MustBuildTime(t, "2013-04-10T04:21:00Z"), FilterOptions: &filter.StorageOptions{Operation: filter.Latest, Field: filter.PredicateField}},
			p:    testutil.MustBuildPredicate(t, `"meet"@[2013-04-10T04:21:00Z]`),
			o:    triple.NewNodeObject(testutil.MustBuildNodeFromStrings(t, "/u", "mary")),
			want: map[string]int{"/u<john>": 1},
		},
	}

	for _, entry := range testTable {
		t.Run(entry.id, func(t *testing.T) {
			// To avoid blocking on the test we use a buffered channel of size 100. On a real
			// usage of the driver you would like to call the graph operation on a separated
			// goroutine using a sync.WaitGroup to collect the error code eventually.
			ss := make(chan *node.Node, 100)
			p := entry.p
			o := entry.o
			if err := g.Subjects(ctx, p, o, entry.lo, ss); err != nil {
				t.Fatalf("g.Subjects(%s, %s, %s) = %v; want nil", p, o, entry.lo, err)
			}
			for s := range ss {
				sStr := s.String()
				if _, ok := entry.want[sStr]; !ok {
					t.Fatalf("g.Subjects(%s, %s, %s) retrieved unexpected %s", p, o, entry.lo, sStr)
				}
				entry.want[sStr] = entry.want[sStr] - 1
				if entry.want[sStr] == 0 {
					delete(entry.want, sStr)
				}
			}
			if len(entry.want) != 0 {
				t.Errorf("g.Subjects(%s, %s, %s) failed to retrieve some expected elements: %v", p, o, entry.lo, entry.want)
			}
		})
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

func TestPredicatesForSubjectAndObjectLatestAnchor(t *testing.T) {
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

func TestPredicatesForSubjectAndObjectFilter(t *testing.T) {
	ts, ctx := getTestTriplesFilter(t), context.Background()
	g, _ := NewStore().NewGraph(ctx, "test")
	if err := g.AddTriples(ctx, ts); err != nil {
		t.Fatalf("g.AddTriples(_) failed to add test triples with error: %v", err)
	}

	testTable := []struct {
		id   string
		lo   *storage.LookupOptions
		s    *node.Node
		o    *triple.Object
		want map[string]int
	}{
		{
			id:   "FILTER latest predicate",
			lo:   &storage.LookupOptions{FilterOptions: &filter.StorageOptions{Operation: filter.Latest, Field: filter.PredicateField}},
			s:    testutil.MustBuildNodeFromStrings(t, "/u", "john"),
			o:    triple.NewNodeObject(testutil.MustBuildNodeFromStrings(t, "/u", "mary")),
			want: map[string]int{`"meet"@[2014-04-10T04:21:00Z]`: 1},
		},
		{
			id:   "FILTER latest object",
			lo:   &storage.LookupOptions{FilterOptions: &filter.StorageOptions{Operation: filter.Latest, Field: filter.ObjectField}},
			s:    testutil.MustBuildNodeFromStrings(t, "/_", "bn"),
			o:    triple.NewPredicateObject(testutil.MustBuildPredicate(t, `"meet"@[2020-04-10T04:21:00Z]`)),
			want: map[string]int{`"_predicate"@[]`: 1},
		},
		{
			id:   "FILTER isImmutable predicate",
			lo:   &storage.LookupOptions{FilterOptions: &filter.StorageOptions{Operation: filter.IsImmutable, Field: filter.PredicateField}},
			s:    testutil.MustBuildNodeFromStrings(t, "/u", "john"),
			o:    triple.NewNodeObject(testutil.MustBuildNodeFromStrings(t, "/u", "bob")),
			want: map[string]int{},
		},
		{
			id:   "FILTER isImmutable object",
			lo:   &storage.LookupOptions{FilterOptions: &filter.StorageOptions{Operation: filter.IsImmutable, Field: filter.ObjectField}},
			s:    testutil.MustBuildNodeFromStrings(t, "/_", "bn"),
			o:    triple.NewPredicateObject(testutil.MustBuildPredicate(t, `"meet"@[2020-04-10T04:21:00Z]`)),
			want: map[string]int{},
		},
		{
			id:   "FILTER isTemporal predicate",
			lo:   &storage.LookupOptions{FilterOptions: &filter.StorageOptions{Operation: filter.IsTemporal, Field: filter.PredicateField}},
			s:    testutil.MustBuildNodeFromStrings(t, "/u", "john"),
			o:    triple.NewNodeObject(testutil.MustBuildNodeFromStrings(t, "/u", "bob")),
			want: map[string]int{`"meet"@[2014-04-10T04:21:00Z]`: 1},
		},
		{
			id:   "FILTER isTemporal object",
			lo:   &storage.LookupOptions{FilterOptions: &filter.StorageOptions{Operation: filter.IsTemporal, Field: filter.ObjectField}},
			s:    testutil.MustBuildNodeFromStrings(t, "/_", "bn"),
			o:    triple.NewPredicateObject(testutil.MustBuildPredicate(t, `"meet"@[2020-04-10T04:21:00Z]`)),
			want: map[string]int{`"_predicate"@[]`: 1},
		},
		{
			id:   "FILTER latest between",
			lo:   &storage.LookupOptions{LowerAnchor: testutil.MustBuildTime(t, "2012-04-10T04:21:00Z"), UpperAnchor: testutil.MustBuildTime(t, "2013-04-10T04:21:00Z"), FilterOptions: &filter.StorageOptions{Operation: filter.Latest, Field: filter.PredicateField}},
			s:    testutil.MustBuildNodeFromStrings(t, "/u", "john"),
			o:    triple.NewNodeObject(testutil.MustBuildNodeFromStrings(t, "/u", "mary")),
			want: map[string]int{`"meet"@[2013-04-10T04:21:00Z]`: 1},
		},
	}

	for _, entry := range testTable {
		t.Run(entry.id, func(t *testing.T) {
			// To avoid blocking on the test we use a buffered channel of size 100. On a real
			// usage of the driver you would like to call the graph operation on a separated
			// goroutine using a sync.WaitGroup to collect the error code eventually.
			pp := make(chan *predicate.Predicate, 100)
			s := entry.s
			o := entry.o
			if err := g.PredicatesForSubjectAndObject(ctx, s, o, entry.lo, pp); err != nil {
				t.Fatalf("g.PredicatesForSubjectAndObject(%s, %s, %s) = %v; want nil", s, o, entry.lo, err)
			}
			for p := range pp {
				pStr := p.String()
				if _, ok := entry.want[pStr]; !ok {
					t.Fatalf("g.PredicatesForSubjectAndObject(%s, %s, %s) retrieved unexpected %s", s, o, entry.lo, pStr)
				}
				entry.want[pStr] = entry.want[pStr] - 1
				if entry.want[pStr] == 0 {
					delete(entry.want, pStr)
				}
			}
			if len(entry.want) != 0 {
				t.Errorf("g.PredicatesForSubjectAndObject(%s, %s, %s) failed to retrieve some expected elements: %v", s, o, entry.lo, entry.want)
			}
		})
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

func TestPredicatesForSubjectLatestAnchor(t *testing.T) {
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

func TestPredicatesForSubjectFilter(t *testing.T) {
	ts, ctx := getTestTriplesFilter(t), context.Background()
	g, _ := NewStore().NewGraph(ctx, "test")
	if err := g.AddTriples(ctx, ts); err != nil {
		t.Fatalf("g.AddTriples(_) failed to add test triples with error: %v", err)
	}

	testTable := []struct {
		id   string
		lo   *storage.LookupOptions
		s    *node.Node
		want map[string]int
	}{
		{
			id:   "FILTER latest predicate",
			lo:   &storage.LookupOptions{FilterOptions: &filter.StorageOptions{Operation: filter.Latest, Field: filter.PredicateField}},
			s:    testutil.MustBuildNodeFromStrings(t, "/u", "john"),
			want: map[string]int{`"meet"@[2014-04-10T04:21:00Z]`: 2},
		},
		{
			id:   "FILTER latest object",
			lo:   &storage.LookupOptions{FilterOptions: &filter.StorageOptions{Operation: filter.Latest, Field: filter.ObjectField}},
			s:    testutil.MustBuildNodeFromStrings(t, "/_", "bn"),
			want: map[string]int{`"_predicate"@[]`: 1},
		},
		{
			id:   "FILTER isImmutable predicate",
			lo:   &storage.LookupOptions{FilterOptions: &filter.StorageOptions{Operation: filter.IsImmutable, Field: filter.PredicateField}},
			s:    testutil.MustBuildNodeFromStrings(t, "/u", "john"),
			want: map[string]int{`"parent_of"@[]`: 1},
		},
		{
			id:   "FILTER isImmutable object",
			lo:   &storage.LookupOptions{FilterOptions: &filter.StorageOptions{Operation: filter.IsImmutable, Field: filter.ObjectField}},
			s:    testutil.MustBuildNodeFromStrings(t, "/_", "bn"),
			want: map[string]int{`"_predicate"@[]`: 1},
		},
		{
			id:   "FILTER isTemporal predicate",
			lo:   &storage.LookupOptions{FilterOptions: &filter.StorageOptions{Operation: filter.IsTemporal, Field: filter.PredicateField}},
			s:    testutil.MustBuildNodeFromStrings(t, "/u", "john"),
			want: map[string]int{`"meet"@[2012-04-10T04:21:00Z]`: 1, `"meet"@[2013-04-10T04:21:00Z]`: 1, `"meet"@[2014-04-10T04:21:00Z]`: 2},
		},
		{
			id:   "FILTER isTemporal object",
			lo:   &storage.LookupOptions{FilterOptions: &filter.StorageOptions{Operation: filter.IsTemporal, Field: filter.ObjectField}},
			s:    testutil.MustBuildNodeFromStrings(t, "/_", "bn"),
			want: map[string]int{`"_predicate"@[]`: 2},
		},
		{
			id:   "FILTER latest between",
			lo:   &storage.LookupOptions{LowerAnchor: testutil.MustBuildTime(t, "2012-04-10T04:21:00Z"), UpperAnchor: testutil.MustBuildTime(t, "2013-04-10T04:21:00Z"), FilterOptions: &filter.StorageOptions{Operation: filter.Latest, Field: filter.PredicateField}},
			s:    testutil.MustBuildNodeFromStrings(t, "/u", "john"),
			want: map[string]int{`"meet"@[2013-04-10T04:21:00Z]`: 1},
		},
	}

	for _, entry := range testTable {
		t.Run(entry.id, func(t *testing.T) {
			// To avoid blocking on the test we use a buffered channel of size 100. On a real
			// usage of the driver you would like to call the graph operation on a separated
			// goroutine using a sync.WaitGroup to collect the error code eventually.
			pp := make(chan *predicate.Predicate, 100)
			s := entry.s
			if err := g.PredicatesForSubject(ctx, s, entry.lo, pp); err != nil {
				t.Fatalf("g.PredicatesForSubject(%s, %s) = %v; want nil", s, entry.lo, err)
			}
			for p := range pp {
				pStr := p.String()
				if _, ok := entry.want[pStr]; !ok {
					t.Fatalf("g.PredicatesForSubject(%s, %s) retrieved unexpected %s", s, entry.lo, pStr)
				}
				entry.want[pStr] = entry.want[pStr] - 1
				if entry.want[pStr] == 0 {
					delete(entry.want, pStr)
				}
			}
			if len(entry.want) != 0 {
				t.Errorf("g.PredicatesForSubject(%s, %s) failed to retrieve some expected elements: %v", s, entry.lo, entry.want)
			}
		})
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

func TestPredicatesForObjectLatestAnchor(t *testing.T) {
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

func TestPredicatesForObjectFilter(t *testing.T) {
	ts, ctx := getTestTriplesFilter(t), context.Background()
	g, _ := NewStore().NewGraph(ctx, "test")
	if err := g.AddTriples(ctx, ts); err != nil {
		t.Fatalf("g.AddTriples(_) failed to add test triples with error: %v", err)
	}

	testTable := []struct {
		id   string
		lo   *storage.LookupOptions
		o    *triple.Object
		want map[string]int
	}{
		{
			id:   "FILTER latest predicate",
			lo:   &storage.LookupOptions{FilterOptions: &filter.StorageOptions{Operation: filter.Latest, Field: filter.PredicateField}},
			o:    triple.NewNodeObject(testutil.MustBuildNodeFromStrings(t, "/u", "mary")),
			want: map[string]int{`"meet"@[2014-04-10T04:21:00Z]`: 1},
		},
		{
			id:   "FILTER latest object",
			lo:   &storage.LookupOptions{FilterOptions: &filter.StorageOptions{Operation: filter.Latest, Field: filter.ObjectField}},
			o:    triple.NewPredicateObject(testutil.MustBuildPredicate(t, `"meet"@[2020-04-10T04:21:00Z]`)),
			want: map[string]int{`"_predicate"@[]`: 1},
		},
		{
			id:   "FILTER isImmutable predicate",
			lo:   &storage.LookupOptions{FilterOptions: &filter.StorageOptions{Operation: filter.IsImmutable, Field: filter.PredicateField}},
			o:    triple.NewNodeObject(testutil.MustBuildNodeFromStrings(t, "/u", "paul")),
			want: map[string]int{`"parent_of"@[]`: 1},
		},
		{
			id:   "FILTER isImmutable object",
			lo:   &storage.LookupOptions{FilterOptions: &filter.StorageOptions{Operation: filter.IsImmutable, Field: filter.ObjectField}},
			o:    triple.NewPredicateObject(testutil.MustBuildPredicate(t, `"height_cm"@[]`)),
			want: map[string]int{`"_predicate"@[]`: 1},
		},
		{
			id:   "FILTER isTemporal predicate",
			lo:   &storage.LookupOptions{FilterOptions: &filter.StorageOptions{Operation: filter.IsTemporal, Field: filter.PredicateField}},
			o:    triple.NewNodeObject(testutil.MustBuildNodeFromStrings(t, "/u", "bob")),
			want: map[string]int{`"meet"@[2014-04-10T04:21:00Z]`: 1},
		},
		{
			id:   "FILTER isTemporal object",
			lo:   &storage.LookupOptions{FilterOptions: &filter.StorageOptions{Operation: filter.IsTemporal, Field: filter.ObjectField}},
			o:    triple.NewPredicateObject(testutil.MustBuildPredicate(t, `"meet"@[2020-04-10T04:21:00Z]`)),
			want: map[string]int{`"_predicate"@[]`: 1},
		},
		{
			id:   "FILTER latest between",
			lo:   &storage.LookupOptions{LowerAnchor: testutil.MustBuildTime(t, "2012-04-10T04:21:00Z"), UpperAnchor: testutil.MustBuildTime(t, "2013-04-10T04:21:00Z"), FilterOptions: &filter.StorageOptions{Operation: filter.Latest, Field: filter.PredicateField}},
			o:    triple.NewNodeObject(testutil.MustBuildNodeFromStrings(t, "/u", "mary")),
			want: map[string]int{`"meet"@[2013-04-10T04:21:00Z]`: 1},
		},
	}

	for _, entry := range testTable {
		t.Run(entry.id, func(t *testing.T) {
			// To avoid blocking on the test we use a buffered channel of size 100. On a real
			// usage of the driver you would like to call the graph operation on a separated
			// goroutine using a sync.WaitGroup to collect the error code eventually.
			pp := make(chan *predicate.Predicate, 100)
			o := entry.o
			if err := g.PredicatesForObject(ctx, o, entry.lo, pp); err != nil {
				t.Fatalf("g.PredicatesForObject(%s, %s) = %v; want nil", o, entry.lo, err)
			}
			for p := range pp {
				pStr := p.String()
				if _, ok := entry.want[pStr]; !ok {
					t.Fatalf("g.PredicatesForObject(%s, %s) retrieved unexpected %s", o, entry.lo, pStr)
				}
				entry.want[pStr] = entry.want[pStr] - 1
				if entry.want[pStr] == 0 {
					delete(entry.want, pStr)
				}
			}
			if len(entry.want) != 0 {
				t.Errorf("g.PredicatesForObject(%s, %s) failed to retrieve some expected elements: %v", o, entry.lo, entry.want)
			}
		})
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

func TestTriplesForSubjectLatestAnchor(t *testing.T) {
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

func TestTriplesForSubjectFilter(t *testing.T) {
	ts, ctx := getTestTriplesFilter(t), context.Background()
	g, _ := NewStore().NewGraph(ctx, "test")
	if err := g.AddTriples(ctx, ts); err != nil {
		t.Fatalf("g.AddTriples(_) failed to add test triples with error: %v", err)
	}

	testTable := []struct {
		id   string
		lo   *storage.LookupOptions
		s    *node.Node
		want map[string]int
	}{
		{
			id: "FILTER latest predicate",
			lo: &storage.LookupOptions{FilterOptions: &filter.StorageOptions{Operation: filter.Latest, Field: filter.PredicateField}},
			s:  testutil.MustBuildNodeFromStrings(t, "/u", "john"),
			want: map[string]int{`/u<john>	"meet"@[2014-04-10T04:21:00Z]	/u<mary>`: 1, `/u<john>	"meet"@[2014-04-10T04:21:00Z]	/u<bob>`: 1},
		},
		{
			id: "FILTER latest object",
			lo: &storage.LookupOptions{FilterOptions: &filter.StorageOptions{Operation: filter.Latest, Field: filter.ObjectField}},
			s:  testutil.MustBuildNodeFromStrings(t, "/_", "bn"),
			want: map[string]int{`/_<bn>	"_predicate"@[]	"meet"@[2021-04-10T04:21:00Z]`: 1},
		},
		{
			id: "FILTER isImmutable predicate",
			lo: &storage.LookupOptions{FilterOptions: &filter.StorageOptions{Operation: filter.IsImmutable, Field: filter.PredicateField}},
			s:  testutil.MustBuildNodeFromStrings(t, "/u", "john"),
			want: map[string]int{`/u<john>	"parent_of"@[]	/u<paul>`: 1},
		},
		{
			id: "FILTER isImmutable object",
			lo: &storage.LookupOptions{FilterOptions: &filter.StorageOptions{Operation: filter.IsImmutable, Field: filter.ObjectField}},
			s:  testutil.MustBuildNodeFromStrings(t, "/_", "bn"),
			want: map[string]int{`/_<bn>	"_predicate"@[]	"height_cm"@[]`: 1},
		},
		{
			id: "FILTER isTemporal predicate",
			lo: &storage.LookupOptions{FilterOptions: &filter.StorageOptions{Operation: filter.IsTemporal, Field: filter.PredicateField}},
			s:  testutil.MustBuildNodeFromStrings(t, "/u", "john"),
			want: map[string]int{`/u<john>	"meet"@[2012-04-10T04:21:00Z]	/u<mary>`: 1, `/u<john>	"meet"@[2013-04-10T04:21:00Z]	/u<mary>`: 1, `/u<john>	"meet"@[2014-04-10T04:21:00Z]	/u<mary>`: 1, `/u<john>	"meet"@[2014-04-10T04:21:00Z]	/u<bob>`: 1},
		},
		{
			id: "FILTER isTemporal object",
			lo: &storage.LookupOptions{FilterOptions: &filter.StorageOptions{Operation: filter.IsTemporal, Field: filter.ObjectField}},
			s:  testutil.MustBuildNodeFromStrings(t, "/_", "bn"),
			want: map[string]int{`/_<bn>	"_predicate"@[]	"meet"@[2020-04-10T04:21:00Z]`: 1, `/_<bn>	"_predicate"@[]	"meet"@[2021-04-10T04:21:00Z]`: 1},
		},
		{
			id: "FILTER latest between",
			lo: &storage.LookupOptions{LowerAnchor: testutil.MustBuildTime(t, "2012-04-10T04:21:00Z"), UpperAnchor: testutil.MustBuildTime(t, "2013-04-10T04:21:00Z"), FilterOptions: &filter.StorageOptions{Operation: filter.Latest, Field: filter.PredicateField}},
			s:  testutil.MustBuildNodeFromStrings(t, "/u", "john"),
			want: map[string]int{`/u<john>	"meet"@[2013-04-10T04:21:00Z]	/u<mary>`: 1},
		},
	}

	for _, entry := range testTable {
		t.Run(entry.id, func(t *testing.T) {
			// To avoid blocking on the test we use a buffered channel of size 100. On a real
			// usage of the driver you would like to call the graph operation on a separated
			// goroutine using a sync.WaitGroup to collect the error code eventually.
			trpls := make(chan *triple.Triple, 100)
			s := entry.s
			if err := g.TriplesForSubject(ctx, s, entry.lo, trpls); err != nil {
				t.Fatalf("g.TriplesForSubject(%s, %s) = %v; want nil", s, entry.lo, err)
			}
			for trpl := range trpls {
				tStr := trpl.String()
				if _, ok := entry.want[tStr]; !ok {
					t.Fatalf("g.TriplesForSubject(%s, %s) retrieved unexpected %s", s, entry.lo, tStr)
				}
				entry.want[tStr] = entry.want[tStr] - 1
				if entry.want[tStr] == 0 {
					delete(entry.want, tStr)
				}
			}
			if len(entry.want) != 0 {
				t.Errorf("g.TriplesForSubject(%s, %s) failed to retrieve some expected elements: %v", s, entry.lo, entry.want)
			}
		})
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

func TestTriplesForPredicateLatestAnchor(t *testing.T) {
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
		if !reflect.DeepEqual(rts.Predicate().UUID(), ts[0].Predicate().UUID()) {
			t.Errorf("g.TriplesForPredicate(%s) = %s for LatestAnchor; want %s", ts[0].Predicate(), rts.Predicate(), ts[0].Predicate())
		}
	}
	if cnt != 1 {
		t.Errorf("g.triplesForPredicate(%s) retrieved %d predicates; want 1", ts[0].Predicate(), cnt)
	}
}

func TestTriplesForPredicateFilter(t *testing.T) {
	ts, ctx := getTestTriplesFilter(t), context.Background()
	g, _ := NewStore().NewGraph(ctx, "test")
	if err := g.AddTriples(ctx, ts); err != nil {
		t.Fatalf("g.AddTriples(_) failed to add test triples with error: %v", err)
	}

	testTable := []struct {
		id   string
		lo   *storage.LookupOptions
		p    *predicate.Predicate
		want map[string]int
	}{
		{
			id: "FILTER latest predicate",
			lo: &storage.LookupOptions{FilterOptions: &filter.StorageOptions{Operation: filter.Latest, Field: filter.PredicateField}},
			p:  testutil.MustBuildPredicate(t, `"meet"@[2012-04-10T04:21:00Z]`),
			want: map[string]int{`/u<john>	"meet"@[2012-04-10T04:21:00Z]	/u<mary>`: 1},
		},
		{
			id: "FILTER latest predicate duplicate timestamp",
			lo: &storage.LookupOptions{FilterOptions: &filter.StorageOptions{Operation: filter.Latest, Field: filter.PredicateField}},
			p:  testutil.MustBuildPredicate(t, `"meet"@[2014-04-10T04:21:00Z]`),
			want: map[string]int{`/u<john>	"meet"@[2014-04-10T04:21:00Z]	/u<mary>`: 1, `/u<john>	"meet"@[2014-04-10T04:21:00Z]	/u<bob>`: 1},
		},
		{
			id: "FILTER latest object",
			lo: &storage.LookupOptions{FilterOptions: &filter.StorageOptions{Operation: filter.Latest, Field: filter.ObjectField}},
			p:  testutil.MustBuildPredicate(t, `"_predicate"@[]`),
			want: map[string]int{`/_<bn>	"_predicate"@[]	"meet"@[2021-04-10T04:21:00Z]`: 1},
		},
		{
			id:   "FILTER isImmutable predicate",
			lo:   &storage.LookupOptions{FilterOptions: &filter.StorageOptions{Operation: filter.IsImmutable, Field: filter.PredicateField}},
			p:    testutil.MustBuildPredicate(t, `"meet"@[2014-04-10T04:21:00Z]`),
			want: map[string]int{},
		},
		{
			id: "FILTER isImmutable object",
			lo: &storage.LookupOptions{FilterOptions: &filter.StorageOptions{Operation: filter.IsImmutable, Field: filter.ObjectField}},
			p:  testutil.MustBuildPredicate(t, `"_predicate"@[]`),
			want: map[string]int{`/_<bn>	"_predicate"@[]	"height_cm"@[]`: 1},
		},
		{
			id: "FILTER isTemporal predicate",
			lo: &storage.LookupOptions{FilterOptions: &filter.StorageOptions{Operation: filter.IsTemporal, Field: filter.PredicateField}},
			p:  testutil.MustBuildPredicate(t, `"meet"@[2014-04-10T04:21:00Z]`),
			want: map[string]int{`/u<john>	"meet"@[2014-04-10T04:21:00Z]	/u<mary>`: 1, `/u<john>	"meet"@[2014-04-10T04:21:00Z]	/u<bob>`: 1},
		},
		{
			id: "FILTER isTemporal object",
			lo: &storage.LookupOptions{FilterOptions: &filter.StorageOptions{Operation: filter.IsTemporal, Field: filter.ObjectField}},
			p:  testutil.MustBuildPredicate(t, `"_predicate"@[]`),
			want: map[string]int{`/_<bn>	"_predicate"@[]	"meet"@[2020-04-10T04:21:00Z]`: 1, `/_<bn>	"_predicate"@[]	"meet"@[2021-04-10T04:21:00Z]`: 1},
		},
		{
			id: "FILTER latest between",
			lo: &storage.LookupOptions{LowerAnchor: testutil.MustBuildTime(t, "2012-04-10T04:21:00Z"), UpperAnchor: testutil.MustBuildTime(t, "2013-04-10T04:21:00Z"), FilterOptions: &filter.StorageOptions{Operation: filter.Latest, Field: filter.PredicateField}},
			p:  testutil.MustBuildPredicate(t, `"meet"@[2013-04-10T04:21:00Z]`),
			want: map[string]int{`/u<john>	"meet"@[2013-04-10T04:21:00Z]	/u<mary>`: 1},
		},
	}

	for _, entry := range testTable {
		t.Run(entry.id, func(t *testing.T) {
			// To avoid blocking on the test we use a buffered channel of size 100. On a real
			// usage of the driver you would like to call the graph operation on a separated
			// goroutine using a sync.WaitGroup to collect the error code eventually.
			trpls := make(chan *triple.Triple, 100)
			p := entry.p
			if err := g.TriplesForPredicate(ctx, p, entry.lo, trpls); err != nil {
				t.Fatalf("g.TriplesForPredicate(%s, %s) = %v; want nil", p, entry.lo, err)
			}
			for trpl := range trpls {
				tStr := trpl.String()
				if _, ok := entry.want[tStr]; !ok {
					t.Fatalf("g.TriplesForPredicate(%s, %s) retrieved unexpected %s", p, entry.lo, tStr)
				}
				entry.want[tStr] = entry.want[tStr] - 1
				if entry.want[tStr] == 0 {
					delete(entry.want, tStr)
				}
			}
			if len(entry.want) != 0 {
				t.Errorf("g.TriplesForPredicate(%s, %s) failed to retrieve some expected elements: %v", p, entry.lo, entry.want)
			}
		})
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

func TestTriplesForObjectLatestAnchor(t *testing.T) {
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

func TestTriplesForObjectFilter(t *testing.T) {
	ts, ctx := getTestTriplesFilter(t), context.Background()
	g, _ := NewStore().NewGraph(ctx, "test")
	if err := g.AddTriples(ctx, ts); err != nil {
		t.Fatalf("g.AddTriples(_) failed to add test triples with error: %v", err)
	}

	testTable := []struct {
		id   string
		lo   *storage.LookupOptions
		o    *triple.Object
		want map[string]int
	}{
		{
			id: "FILTER latest predicate",
			lo: &storage.LookupOptions{FilterOptions: &filter.StorageOptions{Operation: filter.Latest, Field: filter.PredicateField}},
			o:  triple.NewNodeObject(testutil.MustBuildNodeFromStrings(t, "/u", "mary")),
			want: map[string]int{`/u<john>	"meet"@[2014-04-10T04:21:00Z]	/u<mary>`: 1},
		},
		{
			id: "FILTER latest object",
			lo: &storage.LookupOptions{FilterOptions: &filter.StorageOptions{Operation: filter.Latest, Field: filter.ObjectField}},
			o:  triple.NewPredicateObject(testutil.MustBuildPredicate(t, `"meet"@[2020-04-10T04:21:00Z]`)),
			want: map[string]int{`/_<bn>	"_predicate"@[]	"meet"@[2020-04-10T04:21:00Z]`: 1},
		},
		{
			id:   "FILTER isImmutable predicate",
			lo:   &storage.LookupOptions{FilterOptions: &filter.StorageOptions{Operation: filter.IsImmutable, Field: filter.PredicateField}},
			o:    triple.NewNodeObject(testutil.MustBuildNodeFromStrings(t, "/u", "bob")),
			want: map[string]int{},
		},
		{
			id: "FILTER isImmutable object",
			lo: &storage.LookupOptions{FilterOptions: &filter.StorageOptions{Operation: filter.IsImmutable, Field: filter.ObjectField}},
			o:  triple.NewPredicateObject(testutil.MustBuildPredicate(t, `"height_cm"@[]`)),
			want: map[string]int{`/_<bn>	"_predicate"@[]	"height_cm"@[]`: 1},
		},
		{
			id: "FILTER isTemporal predicate",
			lo: &storage.LookupOptions{FilterOptions: &filter.StorageOptions{Operation: filter.IsTemporal, Field: filter.PredicateField}},
			o:  triple.NewNodeObject(testutil.MustBuildNodeFromStrings(t, "/u", "bob")),
			want: map[string]int{`/u<john>	"meet"@[2014-04-10T04:21:00Z]	/u<bob>`: 1},
		},
		{
			id:   "FILTER isTemporal object",
			lo:   &storage.LookupOptions{FilterOptions: &filter.StorageOptions{Operation: filter.IsTemporal, Field: filter.ObjectField}},
			o:    triple.NewPredicateObject(testutil.MustBuildPredicate(t, `"height_cm"@[]`)),
			want: map[string]int{},
		},
		{
			id: "FILTER latest between",
			lo: &storage.LookupOptions{LowerAnchor: testutil.MustBuildTime(t, "2012-04-10T04:21:00Z"), UpperAnchor: testutil.MustBuildTime(t, "2013-04-10T04:21:00Z"), FilterOptions: &filter.StorageOptions{Operation: filter.Latest, Field: filter.PredicateField}},
			o:  triple.NewNodeObject(testutil.MustBuildNodeFromStrings(t, "/u", "mary")),
			want: map[string]int{`/u<john>	"meet"@[2013-04-10T04:21:00Z]	/u<mary>`: 1},
		},
	}

	for _, entry := range testTable {
		t.Run(entry.id, func(t *testing.T) {
			// To avoid blocking on the test we use a buffered channel of size 100. On a real
			// usage of the driver you would like to call the graph operation on a separated
			// goroutine using a sync.WaitGroup to collect the error code eventually.
			trpls := make(chan *triple.Triple, 100)
			o := entry.o
			if err := g.TriplesForObject(ctx, o, entry.lo, trpls); err != nil {
				t.Fatalf("g.TriplesForObject(%s, %s) = %v; want nil", o, entry.lo, err)
			}
			for trpl := range trpls {
				tStr := trpl.String()
				if _, ok := entry.want[tStr]; !ok {
					t.Fatalf("g.TriplesForObject(%s, %s) retrieved unexpected %s", o, entry.lo, tStr)
				}
				entry.want[tStr] = entry.want[tStr] - 1
				if entry.want[tStr] == 0 {
					delete(entry.want, tStr)
				}
			}
			if len(entry.want) != 0 {
				t.Errorf("g.TriplesForObject(%s, %s) failed to retrieve some expected elements: %v", o, entry.lo, entry.want)
			}
		})
	}
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
		LowerAnchor: testutil.MustBuildTime(t, "2015-04-01T00:00:00-08:00"),
		UpperAnchor: testutil.MustBuildTime(t, "2015-06-01T00:00:00-10:00"),
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

func TestTriplesForSubjectAndPredicateLatestAnchor(t *testing.T) {
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
		if !reflect.DeepEqual(rts.Predicate().UUID(), ts[0].Predicate().UUID()) {
			t.Errorf("g.TriplesForSubjectAndPredicate(%s, %s) = %s for LatestAnchor; want %s", ts[0].Subject(), ts[0].Predicate(), rts.Predicate(), ts[0].Predicate())
		}
	}
	if cnt != 1 {
		t.Errorf("g.TriplesForSubjectAndPredicate(%s, %s) retrieved %d predicates; want 1", ts[0].Subject(), ts[0].Predicate(), cnt)
	}
}

func TestTriplesForSubjectAndPredicateFilter(t *testing.T) {
	ts, ctx := getTestTriplesFilter(t), context.Background()
	g, _ := NewStore().NewGraph(ctx, "test")
	if err := g.AddTriples(ctx, ts); err != nil {
		t.Fatalf("g.AddTriples(_) failed to add test triples with error: %v", err)
	}

	testTable := []struct {
		id   string
		lo   *storage.LookupOptions
		s    *node.Node
		p    *predicate.Predicate
		want map[string]int
	}{
		{
			id: "FILTER latest predicate",
			lo: &storage.LookupOptions{FilterOptions: &filter.StorageOptions{Operation: filter.Latest, Field: filter.PredicateField}},
			s:  testutil.MustBuildNodeFromStrings(t, "/u", "john"),
			p:  testutil.MustBuildPredicate(t, `"meet"@[2012-04-10T04:21:00Z]`),
			want: map[string]int{`/u<john>	"meet"@[2012-04-10T04:21:00Z]	/u<mary>`: 1},
		},
		{
			id: "FILTER latest predicate duplicate timestamp",
			lo: &storage.LookupOptions{FilterOptions: &filter.StorageOptions{Operation: filter.Latest, Field: filter.PredicateField}},
			s:  testutil.MustBuildNodeFromStrings(t, "/u", "john"),
			p:  testutil.MustBuildPredicate(t, `"meet"@[2014-04-10T04:21:00Z]`),
			want: map[string]int{`/u<john>	"meet"@[2014-04-10T04:21:00Z]	/u<mary>`: 1, `/u<john>	"meet"@[2014-04-10T04:21:00Z]	/u<bob>`: 1},
		},
		{
			id: "FILTER latest object",
			lo: &storage.LookupOptions{FilterOptions: &filter.StorageOptions{Operation: filter.Latest, Field: filter.ObjectField}},
			s:  testutil.MustBuildNodeFromStrings(t, "/_", "bn"),
			p:  testutil.MustBuildPredicate(t, `"_predicate"@[]`),
			want: map[string]int{`/_<bn>	"_predicate"@[]	"meet"@[2021-04-10T04:21:00Z]`: 1},
		},
		{
			id:   "FILTER isImmutable predicate",
			lo:   &storage.LookupOptions{FilterOptions: &filter.StorageOptions{Operation: filter.IsImmutable, Field: filter.PredicateField}},
			s:    testutil.MustBuildNodeFromStrings(t, "/u", "john"),
			p:    testutil.MustBuildPredicate(t, `"meet"@[2014-04-10T04:21:00Z]`),
			want: map[string]int{},
		},
		{
			id: "FILTER isImmutable object",
			lo: &storage.LookupOptions{FilterOptions: &filter.StorageOptions{Operation: filter.IsImmutable, Field: filter.ObjectField}},
			s:  testutil.MustBuildNodeFromStrings(t, "/_", "bn"),
			p:  testutil.MustBuildPredicate(t, `"_predicate"@[]`),
			want: map[string]int{`/_<bn>	"_predicate"@[]	"height_cm"@[]`: 1},
		},
		{
			id: "FILTER isTemporal predicate",
			lo: &storage.LookupOptions{FilterOptions: &filter.StorageOptions{Operation: filter.IsTemporal, Field: filter.PredicateField}},
			s:  testutil.MustBuildNodeFromStrings(t, "/u", "john"),
			p:  testutil.MustBuildPredicate(t, `"meet"@[2014-04-10T04:21:00Z]`),
			want: map[string]int{`/u<john>	"meet"@[2014-04-10T04:21:00Z]	/u<mary>`: 1, `/u<john>	"meet"@[2014-04-10T04:21:00Z]	/u<bob>`: 1},
		},
		{
			id: "FILTER isTemporal object",
			lo: &storage.LookupOptions{FilterOptions: &filter.StorageOptions{Operation: filter.IsTemporal, Field: filter.ObjectField}},
			s:  testutil.MustBuildNodeFromStrings(t, "/_", "bn"),
			p:  testutil.MustBuildPredicate(t, `"_predicate"@[]`),
			want: map[string]int{`/_<bn>	"_predicate"@[]	"meet"@[2020-04-10T04:21:00Z]`: 1, `/_<bn>	"_predicate"@[]	"meet"@[2021-04-10T04:21:00Z]`: 1},
		},
		{
			id: "FILTER latest between",
			lo: &storage.LookupOptions{LowerAnchor: testutil.MustBuildTime(t, "2012-04-10T04:21:00Z"), UpperAnchor: testutil.MustBuildTime(t, "2013-04-10T04:21:00Z"), FilterOptions: &filter.StorageOptions{Operation: filter.Latest, Field: filter.PredicateField}},
			s:  testutil.MustBuildNodeFromStrings(t, "/u", "john"),
			p:  testutil.MustBuildPredicate(t, `"meet"@[2013-04-10T04:21:00Z]`),
			want: map[string]int{`/u<john>	"meet"@[2013-04-10T04:21:00Z]	/u<mary>`: 1},
		},
	}

	for _, entry := range testTable {
		t.Run(entry.id, func(t *testing.T) {
			// To avoid blocking on the test we use a buffered channel of size 100. On a real
			// usage of the driver you would like to call the graph operation on a separated
			// goroutine using a sync.WaitGroup to collect the error code eventually.
			trpls := make(chan *triple.Triple, 100)
			s := entry.s
			p := entry.p
			if err := g.TriplesForSubjectAndPredicate(ctx, s, p, entry.lo, trpls); err != nil {
				t.Fatalf("g.TriplesForSubjectAndPredicate(%s, %s, %s) = %v; want nil", s, p, entry.lo, err)
			}
			for trpl := range trpls {
				tStr := trpl.String()
				if _, ok := entry.want[tStr]; !ok {
					t.Fatalf("g.TriplesForSubjectAndPredicate(%s, %s, %s) retrieved unexpected %s", s, p, entry.lo, tStr)
				}
				entry.want[tStr] = entry.want[tStr] - 1
				if entry.want[tStr] == 0 {
					delete(entry.want, tStr)
				}
			}
			if len(entry.want) != 0 {
				t.Errorf("g.TriplesForSubjectAndPredicate(%s, %s, %s) failed to retrieve some expected elements: %v", s, p, entry.lo, entry.want)
			}
		})
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

func TestTriplesForPredicateAndObjectLatestAnchor(t *testing.T) {
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
		if !reflect.DeepEqual(rts.Predicate().UUID(), ts[0].Predicate().UUID()) {
			t.Errorf("g.TriplesForPredicateAndObject(%s, %s) = %s for LatestAnchor; want %s", ts[0].Predicate(), ts[0].Object(), rts.Predicate(), ts[0].Predicate())
		}
	}
	if cnt != 1 {
		t.Errorf("g.TriplesForPredicateAndObject(%s, %s) retrieved %d predicates; want 1", ts[0].Predicate(), ts[0].Object(), cnt)
	}
}

func TestTriplesForPredicateAndObjectFilter(t *testing.T) {
	ts, ctx := getTestTriplesFilter(t), context.Background()
	g, _ := NewStore().NewGraph(ctx, "test")
	if err := g.AddTriples(ctx, ts); err != nil {
		t.Fatalf("g.AddTriples(_) failed to add test triples with error: %v", err)
	}

	testTable := []struct {
		id   string
		lo   *storage.LookupOptions
		p    *predicate.Predicate
		o    *triple.Object
		want map[string]int
	}{
		{
			id: "FILTER latest predicate",
			lo: &storage.LookupOptions{FilterOptions: &filter.StorageOptions{Operation: filter.Latest, Field: filter.PredicateField}},
			p:  testutil.MustBuildPredicate(t, `"meet"@[2012-04-10T04:21:00Z]`),
			o:  triple.NewNodeObject(testutil.MustBuildNodeFromStrings(t, "/u", "mary")),
			want: map[string]int{`/u<john>	"meet"@[2012-04-10T04:21:00Z]	/u<mary>`: 1},
		},
		{
			id: "FILTER latest predicate duplicate timestamp",
			lo: &storage.LookupOptions{FilterOptions: &filter.StorageOptions{Operation: filter.Latest, Field: filter.PredicateField}},
			p:  testutil.MustBuildPredicate(t, `"meet"@[2014-04-10T04:21:00Z]`),
			o:  triple.NewNodeObject(testutil.MustBuildNodeFromStrings(t, "/u", "mary")),
			want: map[string]int{`/u<john>	"meet"@[2014-04-10T04:21:00Z]	/u<mary>`: 1},
		},
		{
			id: "FILTER latest object",
			lo: &storage.LookupOptions{FilterOptions: &filter.StorageOptions{Operation: filter.Latest, Field: filter.ObjectField}},
			p:  testutil.MustBuildPredicate(t, `"_predicate"@[]`),
			o:  triple.NewPredicateObject(testutil.MustBuildPredicate(t, `"meet"@[2020-04-10T04:21:00Z]`)),
			want: map[string]int{`/_<bn>	"_predicate"@[]	"meet"@[2020-04-10T04:21:00Z]`: 1},
		},
		{
			id: "FILTER isImmutable predicate",
			lo: &storage.LookupOptions{FilterOptions: &filter.StorageOptions{Operation: filter.IsImmutable, Field: filter.PredicateField}},
			p:  testutil.MustBuildPredicate(t, `"parent_of"@[]`),
			o:  triple.NewNodeObject(testutil.MustBuildNodeFromStrings(t, "/u", "paul")),
			want: map[string]int{`/u<john>	"parent_of"@[]	/u<paul>`: 1},
		},
		{
			id:   "FILTER isImmutable object",
			lo:   &storage.LookupOptions{FilterOptions: &filter.StorageOptions{Operation: filter.IsImmutable, Field: filter.ObjectField}},
			p:    testutil.MustBuildPredicate(t, `"_predicate"@[]`),
			o:    triple.NewPredicateObject(testutil.MustBuildPredicate(t, `"meet"@[2020-04-10T04:21:00Z]`)),
			want: map[string]int{},
		},
		{
			id: "FILTER isTemporal predicate",
			lo: &storage.LookupOptions{FilterOptions: &filter.StorageOptions{Operation: filter.IsTemporal, Field: filter.PredicateField}},
			p:  testutil.MustBuildPredicate(t, `"meet"@[2014-04-10T04:21:00Z]`),
			o:  triple.NewNodeObject(testutil.MustBuildNodeFromStrings(t, "/u", "mary")),
			want: map[string]int{`/u<john>	"meet"@[2014-04-10T04:21:00Z]	/u<mary>`: 1},
		},
		{
			id:   "FILTER isTemporal object",
			lo:   &storage.LookupOptions{FilterOptions: &filter.StorageOptions{Operation: filter.IsTemporal, Field: filter.ObjectField}},
			p:    testutil.MustBuildPredicate(t, `"_predicate"@[]`),
			o:    triple.NewPredicateObject(testutil.MustBuildPredicate(t, `"height_cm"@[]`)),
			want: map[string]int{},
		},
		{
			id: "FILTER latest between",
			lo: &storage.LookupOptions{LowerAnchor: testutil.MustBuildTime(t, "2012-04-10T04:21:00Z"), UpperAnchor: testutil.MustBuildTime(t, "2013-04-10T04:21:00Z"), FilterOptions: &filter.StorageOptions{Operation: filter.Latest, Field: filter.PredicateField}},
			p:  testutil.MustBuildPredicate(t, `"meet"@[2013-04-10T04:21:00Z]`),
			o:  triple.NewNodeObject(testutil.MustBuildNodeFromStrings(t, "/u", "mary")),
			want: map[string]int{`/u<john>	"meet"@[2013-04-10T04:21:00Z]	/u<mary>`: 1},
		},
	}

	for _, entry := range testTable {
		t.Run(entry.id, func(t *testing.T) {
			// To avoid blocking on the test we use a buffered channel of size 100. On a real
			// usage of the driver you would like to call the graph operation on a separated
			// goroutine using a sync.WaitGroup to collect the error code eventually.
			trpls := make(chan *triple.Triple, 100)
			p := entry.p
			o := entry.o
			if err := g.TriplesForPredicateAndObject(ctx, p, o, entry.lo, trpls); err != nil {
				t.Fatalf("g.TriplesForPredicateAndObject(%s, %s, %s) = %v; want nil", p, o, entry.lo, err)
			}
			for trpl := range trpls {
				tStr := trpl.String()
				if _, ok := entry.want[tStr]; !ok {
					t.Fatalf("g.TriplesForPredicateAndObject(%s, %s, %s) retrieved unexpected %s", p, o, entry.lo, tStr)
				}
				entry.want[tStr] = entry.want[tStr] - 1
				if entry.want[tStr] == 0 {
					delete(entry.want, tStr)
				}
			}
			if len(entry.want) != 0 {
				t.Errorf("g.TriplesForPredicateAndObject(%s, %s, %s) failed to retrieve some expected elements: %v", p, o, entry.lo, entry.want)
			}
		})
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

func TestTriplesLatestAnchor(t *testing.T) {
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

func TestTriplesFilter(t *testing.T) {
	ts, ctx := getTestTriplesFilter(t), context.Background()
	g, _ := NewStore().NewGraph(ctx, "test")
	if err := g.AddTriples(ctx, ts); err != nil {
		t.Fatalf("g.AddTriples(_) failed to add test triples with error: %v", err)
	}

	testTable := []struct {
		id   string
		lo   *storage.LookupOptions
		want map[string]int
	}{
		{
			id: "FILTER latest predicate",
			lo: &storage.LookupOptions{FilterOptions: &filter.StorageOptions{Operation: filter.Latest, Field: filter.PredicateField}},
			want: map[string]int{`/u<john>	"meet"@[2014-04-10T04:21:00Z]	/u<mary>`: 1, `/u<john>	"meet"@[2014-04-10T04:21:00Z]	/u<bob>`: 1},
		},
		{
			id: "FILTER latest object",
			lo: &storage.LookupOptions{FilterOptions: &filter.StorageOptions{Operation: filter.Latest, Field: filter.ObjectField}},
			want: map[string]int{`/_<bn>	"_predicate"@[]	"meet"@[2021-04-10T04:21:00Z]`: 1},
		},
		{
			id: "FILTER isImmutable predicate",
			lo: &storage.LookupOptions{FilterOptions: &filter.StorageOptions{Operation: filter.IsImmutable, Field: filter.PredicateField}},
			want: map[string]int{`/u<john>	"parent_of"@[]	/u<paul>`: 1, `/_<bn>	"_predicate"@[]	"meet"@[2020-04-10T04:21:00Z]`: 1, `/_<bn>	"_predicate"@[]	"meet"@[2021-04-10T04:21:00Z]`: 1, `/_<bn>	"_predicate"@[]	"height_cm"@[]`: 1},
		},
		{
			id: "FILTER isImmutable object",
			lo: &storage.LookupOptions{FilterOptions: &filter.StorageOptions{Operation: filter.IsImmutable, Field: filter.ObjectField}},
			want: map[string]int{`/_<bn>	"_predicate"@[]	"height_cm"@[]`: 1},
		},
		{
			id: "FILTER isTemporal predicate",
			lo: &storage.LookupOptions{FilterOptions: &filter.StorageOptions{Operation: filter.IsTemporal, Field: filter.PredicateField}},
			want: map[string]int{`/u<john>	"meet"@[2012-04-10T04:21:00Z]	/u<mary>`: 1, `/u<john>	"meet"@[2013-04-10T04:21:00Z]	/u<mary>`: 1, `/u<john>	"meet"@[2014-04-10T04:21:00Z]	/u<mary>`: 1, `/u<john>	"meet"@[2014-04-10T04:21:00Z]	/u<bob>`: 1},
		},
		{
			id: "FILTER isTemporal object",
			lo: &storage.LookupOptions{FilterOptions: &filter.StorageOptions{Operation: filter.IsTemporal, Field: filter.ObjectField}},
			want: map[string]int{`/_<bn>	"_predicate"@[]	"meet"@[2020-04-10T04:21:00Z]`: 1, `/_<bn>	"_predicate"@[]	"meet"@[2021-04-10T04:21:00Z]`: 1},
		},
		{
			id: "FILTER latest between",
			lo: &storage.LookupOptions{LowerAnchor: testutil.MustBuildTime(t, "2012-04-10T04:21:00Z"), UpperAnchor: testutil.MustBuildTime(t, "2013-04-10T04:21:00Z"), FilterOptions: &filter.StorageOptions{Operation: filter.Latest, Field: filter.PredicateField}},
			want: map[string]int{`/u<john>	"meet"@[2013-04-10T04:21:00Z]	/u<mary>`: 1},
		},
	}

	for _, entry := range testTable {
		t.Run(entry.id, func(t *testing.T) {
			// To avoid blocking on the test we use a buffered channel of size 100. On a real
			// usage of the driver you would like to call the graph operation on a separated
			// goroutine using a sync.WaitGroup to collect the error code eventually.
			trpls := make(chan *triple.Triple, 100)
			if err := g.Triples(ctx, entry.lo, trpls); err != nil {
				t.Fatalf("g.Triples(%s) = %v; want nil", entry.lo, err)
			}
			for trpl := range trpls {
				tStr := trpl.String()
				if _, ok := entry.want[tStr]; !ok {
					t.Fatalf("g.Triples(%s) retrieved unexpected %s", entry.lo, tStr)
				}
				entry.want[tStr] = entry.want[tStr] - 1
				if entry.want[tStr] == 0 {
					delete(entry.want, tStr)
				}
			}
			if len(entry.want) != 0 {
				t.Errorf("g.Triples(%s) failed to retrieve some expected elements: %v", entry.lo, entry.want)
			}
		})
	}
}
