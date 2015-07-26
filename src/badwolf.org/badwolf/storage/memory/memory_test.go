package memory

import (
	"testing"
	"time"

	"badwolf.org/badwolf/storage"
	"badwolf.org/badwolf/triple"
	"badwolf.org/badwolf/triple/literal"
	"badwolf.org/badwolf/triple/predicate"
)

func TestDefaultLookupChecker(t *testing.T) {
	dlu := storage.DefaultLookup
	c := newChecker(dlu)
	ip, tp := predicate.NewImmutable("foo"), predicate.NewTemporal("bar", time.Now())
	if !c.CheckAndUpdate(ip) {
		t.Errorf("Immutable predicates should always validate with default lookup %v", dlu)
	}
	if !c.CheckAndUpdate(tp) {
		t.Errorf("Temporal predicates should always validate with default lookup %v", dlu)
	}
}

func TestLimitedItemsLookupChecker(t *testing.T) {
	blu := &storage.LookupOptions{MaxElements: 1}
	c := newChecker(blu)
	ip := predicate.NewImmutable("foo")
	if !c.CheckAndUpdate(ip) {
		t.Errorf("The first predicate should always succeeed on bounded lookup %v", blu)
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
	clu := newChecker(blu)
	if !clu.CheckAndUpdate(mpa) {
		t.Errorf("Failed to reject invalid predicate %v by checker %v", mpa, clu)
	}
	lb, _ = mpa.TimeAnchor()
	blu = &storage.LookupOptions{LowerAnchor: lb}
	clu = newChecker(blu)
	if clu.CheckAndUpdate(lpa) {
		t.Errorf("Failed to reject invalid predicate %v by checker %v", mpa, clu)
	}
	// Check upper bound.
	ub, _ := upa.TimeAnchor()
	buu := &storage.LookupOptions{UpperAnchor: ub}
	cuu := newChecker(buu)
	if !cuu.CheckAndUpdate(mpa) {
		t.Errorf("Failed to reject invalid predicate %v by checker %v", mpa, cuu)
	}
	ub, _ = mpa.TimeAnchor()
	buu = &storage.LookupOptions{UpperAnchor: ub}
	cuu = newChecker(buu)
	if cuu.CheckAndUpdate(upa) {
		t.Errorf("Failed to reject invalid predicate %v by checker %v", mpa, cuu)
	}
}

func getTestTriples(t *testing.T) []*triple.Triple {
	ts := []*triple.Triple{}
	ss := []string{
		"/u<john>\t\"knows\"@[]\t/u<mary>",
		"/u<john>\t\"knows\"@[]\t/u<peter>",
		"/u<john>\t\"knows\"@[]\t/u<alice>",
		"/u<mary>\t\"knows\"@[]\t/u<andrew>",
		"/u<mary>\t\"knows\"@[]\t/u<kim>",
		"/u<mary>\t\"knows\"@[]\t/u<alice>",
	}
	for _, s := range ss {
		trpl, err := triple.ParseTriple(s, literal.DefaultBuilder())
		if err != nil {
			t.Errorf("triple.Parse failed to parse valid triple %s with error %v", s, err)
			continue
		}
		ts = append(ts, trpl)
	}
	return ts
}

func TestAddRemoveTriples(t *testing.T) {
	ts := getTestTriples(t)
	g, _ := DefaultStore.NewGraph("test")
	if err := g.AddTriples(ts); err != nil {
		t.Errorf("g.AddTriples(_) failed failed to add test triples with error %v", err)
	}
	if err := g.RemoveTriples(ts); err != nil {
		t.Errorf("g.RemoveTriples(_) failed failed to remove test triples with error %v", err)
	}
}
