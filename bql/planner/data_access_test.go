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
	"context"
	"errors"
	"fmt"
	"reflect"
	"sync"
	"testing"

	"github.com/google/badwolf/bql/semantic"
	"github.com/google/badwolf/bql/table"
	"github.com/google/badwolf/storage"
	"github.com/google/badwolf/storage/memory"
	"github.com/google/badwolf/triple"
	"github.com/google/badwolf/triple/literal"
	"github.com/google/badwolf/triple/node"
	"github.com/google/badwolf/triple/predicate"
)

var (
	testImmutatbleTriples []string
	// Added to test Issue 40 (https://github.com/google/badwolf/issues/40)
	testTemporalTriples []string
)

func init() {
	testImmutatbleTriples = []string{
		"/u<john>\t\"knows\"@[]\t/u<mary>",
		"/u<john>\t\"knows\"@[]\t/u<peter>",
		"/u<john>\t\"knows\"@[]\t/u<alice>",
		"/u<mary>\t\"knows\"@[]\t/u<andrew>",
		"/u<mary>\t\"knows\"@[]\t/u<kim>",
		"/u<mary>\t\"knows\"@[]\t/u<alice>",
	}
	testTemporalTriples = []string{
		// Issue 40 triples.
		"/item/book<000>\t\"in\"@[2016-04-10T4:21:00.000000000Z]\t/room<Hallway>",
		"/item/book<000>\t\"in\"@[2016-04-10T4:23:00.000000000Z]\t/room<Kitchen>",
		"/item/book<000>\t\"in\"@[2016-04-10T4:25:00.000000000Z]\t/room<Bedroom>",
	}
}

func getTestTriples(t *testing.T, trpls []string) []*triple.Triple {
	var ts []*triple.Triple
	for _, s := range trpls {
		trpl, err := triple.Parse(s, literal.DefaultBuilder())
		if err != nil {
			t.Fatalf("triple.Parse failed to parse valid triple %s with error %v", s, err)
			continue
		}
		ts = append(ts, trpl)
	}
	return ts
}

func getTestStore(t *testing.T, trpls []string) storage.Store {
	ts, ctx := getTestTriples(t, trpls), context.Background()
	s := memory.NewStore()
	g, err := s.NewGraph(ctx, "?test")
	if err != nil {
		t.Fatal(err)
	}
	if err := g.AddTriples(ctx, ts); err != nil {
		t.Fatalf("g.AddTriples(_) failed failed to add test triples with error %v", err)
	}
	return s
}

func TestDataAccessSimpleFetch(t *testing.T) {
	testBindings, ctx := []string{"?s", "?p", "?o"}, context.Background()
	cls := &semantic.GraphClause{
		SBinding: "?s",
		PBinding: "?p",
		OBinding: "?o",
	}
	g, err := getTestStore(t, testImmutatbleTriples).Graph(ctx, "?test")
	if err != nil {
		t.Fatal(err)
	}
	tbl, err := simpleFetch(ctx, []storage.Graph{g}, cls, &storage.LookupOptions{}, 0, 0, nil)
	if err != nil {
		t.Errorf("simpleFetch failed with errorf %v", err)
	}
	if got, want := len(tbl.Bindings()), len(testBindings); got != want {
		t.Errorf("simpleFetch returned a table with wrong bindings set; got %v, want %v", got, want)
	}
	if got, want := tbl.NumRows(), len(testImmutatbleTriples); got != want {
		t.Errorf("simpleFetch returned the wrong number of rows; got %d, want %d", got, want)
	}
	for _, r := range tbl.Rows() {
		if got, want := len(r), len(testBindings); got != want {
			t.Errorf("simpleFetch returned row %v with the incorrect number of bindings; got %d, want %d", r, got, want)
		}
	}
}

// Issue 40 (https://github.com/google/badwolf/issues/40)
func TestDataAccessSimpleFetchIssue40(t *testing.T) {
	testBindings, ctx := []string{"?itme", "?t"}, context.Background()
	n, err := node.Parse("/room<Bedroom>")
	if err != nil {
		t.Fatalf("node.Parse failed to parse \"/room<Bedroom>\", %v", err)
	}
	cls := &semantic.GraphClause{
		SBinding:       "?item",
		PID:            "in",
		PAnchorBinding: "?t",
		O:              triple.NewNodeObject(n),
	}
	g, err := getTestStore(t, testTemporalTriples).Graph(ctx, "?test")
	if err != nil {
		t.Fatal(err)
	}

	tbl, err := simpleFetch(ctx, []storage.Graph{g}, cls, &storage.LookupOptions{}, 0, 0, nil)
	if err != nil {
		t.Errorf("simpleFetch failed with errorf %v", err)
	}
	if got, want := len(tbl.Bindings()), len(testBindings); got != want {
		t.Errorf("simpleFetch returned a table with wrong bindings set; got %v, want %v", got, want)
	}
	if got, want := tbl.NumRows(), 1; got != want {
		t.Errorf("simpleFetch returned the wrong number of rows; got %d, want %d\n%s", got, want, tbl)
	}
	for _, r := range tbl.Rows() {
		if got, want := len(r), len(testBindings); got != want {
			t.Errorf("simpleFetch returned row %v with the incorrect number of bindings; got %d, want %d", r, got, want)
		}
	}
}
func TestDataAccessFeasibleSimpleExist(t *testing.T) {
	ctx := context.Background()
	g, err := getTestStore(t, testImmutatbleTriples).Graph(ctx, "?test")
	if err != nil {
		t.Fatal(err)
	}
	tt := getTestTriples(t, testImmutatbleTriples)
	s, p, o := tt[0].Subject(), tt[0].Predicate(), tt[0].Object()
	clsOK := &semantic.GraphClause{
		S: s,
		P: p,
		O: o,
	}
	unfeasible, tbl, err := simpleExist(ctx, []storage.Graph{g}, clsOK, tt[0], nil)
	if err != nil {
		t.Errorf("simpleExist should have not failed with error %v", err)
	}
	if unfeasible {
		t.Error(errors.New("simpleExist should have return a feasible table instead"))
	}
	if got, want := tbl.NumRows(), 0; got != want {
		t.Errorf("simpleExist failed to return the right number of rows: got %d, want %d", got, want)
	}
}

func TestDataAccessUnfeasibleSimpleExist(t *testing.T) {
	ctx := context.Background()
	g, err := getTestStore(t, testImmutatbleTriples).Graph(ctx, "?test")
	if err != nil {
		t.Fatal(err)
	}
	unknown, err := node.Parse("/unknown<unknown>")
	if err != nil {
		t.Fatal(err)
	}
	tt := getTestTriples(t, testImmutatbleTriples)
	s, p, o := unknown, tt[0].Predicate(), tt[0].Object()
	clsNotOK := &semantic.GraphClause{
		S: s,
		P: p,
		O: o,
	}
	tplNotOK, err := triple.New(s, p, o)
	if err != nil {
		t.Fatal(err)
	}
	unfeasible, tbl, err := simpleExist(ctx, []storage.Graph{g}, clsNotOK, tplNotOK, nil)
	if err != nil {
		t.Errorf("simpleExist should have not failed with error %v", err)
	}
	if !unfeasible {
		t.Error(errors.New("simpleExist should have return an unfeasible table instead"))
	}
	if got, want := tbl.NumRows(), 0; got != want {
		t.Errorf("simpleExist failed to return the right number of rows: got %d, want %d", got, want)
	}
}

func TestDataAccessAddTriples(t *testing.T) {
	ctx := context.Background()
	testBindings := []string{"?s", "?p", "?o"}
	cls := &semantic.GraphClause{
		SBinding: "?s",
		PBinding: "?p",
		OBinding: "?o",
	}
	g, err := getTestStore(t, testImmutatbleTriples).Graph(ctx, "?test")
	if err != nil {
		t.Fatal(err)
	}
	tbl, err := table.New([]string{})
	if err != nil {
		t.Fatal(err)
	}
	var wg sync.WaitGroup
	wg.Add(2)
	ts := make(chan *triple.Triple)
	go func() {
		defer wg.Done()
		if err := g.Triples(ctx, storage.DefaultLookup, ts); err != nil {
			t.Fatal(err)
		}
	}()
	go func() {
		defer wg.Done()
		if err := addTriples(ts, cls, tbl, nil); err != nil {
			t.Errorf("addTriple failed with errorf %v", err)
		}
	}()
	wg.Wait()
	if got, want := tbl.NumRows(), len(testImmutatbleTriples); got != want {
		t.Errorf("addTriples returned the wrong number of rows; got %d, want %d", got, want)
	}
	for _, r := range tbl.Rows() {
		if got, want := len(r), len(testBindings); got != want {
			t.Errorf("addTriples returned row %v with the incorrect number of bindings; got %d, want %d", r, got, want)
		}
	}
}

func testNodePredicateLiteral(t *testing.T) (*node.Node, *predicate.Predicate, *literal.Literal) {
	n, err := node.Parse(`/foo<bar>`)
	if err != nil {
		t.Fatal(err)
	}
	p, err := predicate.Parse(`"foo"@[]`)
	if err != nil {
		t.Fatal(err)
	}
	l, err := literal.DefaultBuilder().Parse(`"true"^^type:bool`)
	if err != nil {
		t.Fatal(err)
	}
	return n, p, l
}

func testNodeTemporalPredicateLiteral(t *testing.T) (*node.Node, *predicate.Predicate, *literal.Literal) {
	n, err := node.Parse(`/foo<bar>`)
	if err != nil {
		t.Fatal(err)
	}
	p, err := predicate.Parse(`"bar"@[1975-01-01T00:01:01.999999999Z]`)
	if err != nil {
		t.Fatal(err)
	}
	l, err := literal.DefaultBuilder().Parse(`"true"^^type:bool`)
	if err != nil {
		t.Fatal(err)
	}
	return n, p, l
}

func TestDataAccessObjeToCell(t *testing.T) {
	n, p, l := testNodePredicateLiteral(t)
	testTable := []struct {
		o *triple.Object
		c *table.Cell
	}{
		{
			o: triple.NewNodeObject(n),
			c: &table.Cell{N: n},
		},
		{
			o: triple.NewPredicateObject(p),
			c: &table.Cell{P: p},
		},
		{
			o: triple.NewLiteralObject(l),
			c: &table.Cell{L: l},
		},
	}
	for _, entry := range testTable {
		c, err := objectToCell(entry.o)
		if err != nil {
			t.Errorf("objecToCell for object %q failed with error %v", entry.o, err)
		}
		if got, want := c, entry.c; !reflect.DeepEqual(got, want) {
			t.Errorf("objectToCell failed to properly convert the object into a cell; got %#v, want %#v", got, want)
		}
	}
}

func TestDataAccessBasicBindings(t *testing.T) {
	n, p, l := testNodePredicateLiteral(t)
	cls := &semantic.GraphClause{
		SBinding: "?s",
		PBinding: "?p",
		OBinding: "?o",
	}

	testTable := []struct {
		t        string
		sBinding *table.Cell
		pBinding *table.Cell
		oBinding *table.Cell
	}{
		{
			t:        fmt.Sprintf("%s\t%s\t%s", n, p, n),
			sBinding: &table.Cell{N: n},
			pBinding: &table.Cell{P: p},
			oBinding: &table.Cell{N: n},
		},
		{
			t:        fmt.Sprintf("%s\t%s\t%s", n, p, p),
			sBinding: &table.Cell{N: n},
			pBinding: &table.Cell{P: p},
			oBinding: &table.Cell{P: p},
		},
		{
			t:        fmt.Sprintf("%s\t%s\t%s", n, p, l),
			sBinding: &table.Cell{N: n},
			pBinding: &table.Cell{P: p},
			oBinding: &table.Cell{L: l},
		},
	}

	for _, entry := range testTable {
		// Setup for test:
		tpl, err := triple.Parse(entry.t, literal.DefaultBuilder())
		if err != nil {
			t.Fatalf(`triple.Parse failed for triple "%s": %v`, entry.t, err)
		}

		// Actual test:
		r, err := tripleToRow(tpl, cls)
		if err != nil {
			t.Fatalf(`tripleToRow("%s", %q) = _, %v; want _, nil error`, tpl, cls, err)
		}
		bindings := []string{"?s", "?p", "?o"}
		entryCells := []*table.Cell{entry.sBinding, entry.pBinding, entry.oBinding}
		for i, binding := range bindings {
			if got, want := r[binding], entryCells[i]; !reflect.DeepEqual(got, want) {
				t.Errorf(`tripleToRow(%q) = "%s", _; want "%s", _`, binding, got, want)
			}
		}
	}
}

func TestDataAccessTripleToRowSubjectBindings(t *testing.T) {
	n, p, _ := testNodePredicateLiteral(t)
	testTable := []struct {
		t          string
		cls        *semantic.GraphClause
		sBinding   *table.Cell
		sAlias     *table.Cell
		sTypeAlias *table.Cell
		sIDAlias   *table.Cell
	}{
		{
			t: fmt.Sprintf("%s\t%s\t%s", n, p, n),
			cls: &semantic.GraphClause{
				SBinding:   "?s",
				SAlias:     "?alias",
				STypeAlias: "?type",
				SIDAlias:   "?id",
			},
			sBinding:   &table.Cell{N: n},
			sAlias:     &table.Cell{N: n},
			sTypeAlias: &table.Cell{S: table.CellString(n.Type().String())},
			sIDAlias:   &table.Cell{S: table.CellString(n.ID().String())},
		},
	}

	for _, entry := range testTable {
		// Setup for test:
		tpl, err := triple.Parse(entry.t, literal.DefaultBuilder())
		if err != nil {
			t.Fatalf(`triple.Parse failed for triple "%s": %v`, entry.t, err)
		}

		// Actual test:
		r, err := tripleToRow(tpl, entry.cls)
		if err != nil {
			t.Fatalf(`tripleToRow("%s", %q) = _, %v; want _, nil error`, tpl, entry.cls, err)
		}
		bindings := []string{"?s", "?alias", "?type", "?id"}
		entryCells := []*table.Cell{entry.sBinding, entry.sAlias, entry.sTypeAlias, entry.sIDAlias}
		for i, binding := range bindings {
			if got, want := r[binding], entryCells[i]; !reflect.DeepEqual(got, want) {
				t.Errorf(`tripleToRow(%q) = "%s", _; want "%s", _`, binding, got, want)
			}
		}
	}
}

func TestDataAccessTripleToRowPredicateBindings(t *testing.T) {
	n, pTemporal, _ := testNodeTemporalPredicateLiteral(t)
	ta, err := pTemporal.TimeAnchor()
	if err != nil {
		t.Fatal(err)
	}
	_, pImmutable, _ := testNodePredicateLiteral(t)

	testTable := []struct {
		t              string
		cls            *semantic.GraphClause
		pBinding       *table.Cell
		pAlias         *table.Cell
		pIDAlias       *table.Cell
		pAnchorBinding *table.Cell
		pAnchorAlias   *table.Cell
	}{
		{
			t: fmt.Sprintf("%s\t%s\t%s", n, pTemporal, n),
			cls: &semantic.GraphClause{
				PBinding:       "?p",
				PAlias:         "?alias",
				PIDAlias:       "?id",
				PAnchorBinding: "?anchorBinding",
				PAnchorAlias:   "?anchorAlias",
			},
			pBinding:       &table.Cell{P: pTemporal},
			pAlias:         &table.Cell{P: pTemporal},
			pIDAlias:       &table.Cell{S: table.CellString(string(pTemporal.ID()))},
			pAnchorBinding: &table.Cell{T: ta},
			pAnchorAlias:   &table.Cell{T: ta},
		},
		{
			t: fmt.Sprintf("%s\t%s\t%s", n, pImmutable, n),
			cls: &semantic.GraphClause{
				PBinding: "?p",
				PAlias:   "?alias",
				PIDAlias: "?id",
			},
			pBinding: &table.Cell{P: pImmutable},
			pAlias:   &table.Cell{P: pImmutable},
			pIDAlias: &table.Cell{S: table.CellString(string(pImmutable.ID()))},
		},
		{
			t: fmt.Sprintf("%s\t%s\t%s", n, pImmutable, n),
			cls: &semantic.GraphClause{
				PAnchorBinding: "?anchorBinding",
				PAnchorAlias:   "?anchorAlias",
				Optional:       true,
			},
			pAnchorBinding: &table.Cell{},
			pAnchorAlias:   &table.Cell{},
		},
	}

	for _, entry := range testTable {
		// Setup for test:
		tpl, err := triple.Parse(entry.t, literal.DefaultBuilder())
		if err != nil {
			t.Fatalf(`triple.Parse failed for triple "%s": %v`, entry.t, err)
		}

		// Actual test:
		r, err := tripleToRow(tpl, entry.cls)
		if err != nil {
			t.Fatalf(`tripleToRow("%s", %q) = _, %v; want _, nil error`, tpl, entry.cls, err)
		}
		bindings := []string{"?p", "?alias", "?id", "?anchorBinding", "?anchorAlias"}
		entryCells := []*table.Cell{entry.pBinding, entry.pAlias, entry.pIDAlias, entry.pAnchorBinding, entry.pAnchorAlias}
		for i, binding := range bindings {
			if got, want := r[binding], entryCells[i]; !reflect.DeepEqual(got, want) {
				t.Errorf(`tripleToRow(%q) = "%s", _; want "%s", _`, binding, got, want)
			}
		}
	}
}

func TestDataAccessTripleToRowPredicateBindingsError(t *testing.T) {
	n, pImmutable, _ := testNodePredicateLiteral(t)

	testTable := []struct {
		t   string
		cls *semantic.GraphClause
	}{
		{
			t: fmt.Sprintf("%s\t%s\t%s", n, pImmutable, n),
			cls: &semantic.GraphClause{
				PAnchorBinding: "?anchorBinding",
			},
		},
		{
			t: fmt.Sprintf("%s\t%s\t%s", n, pImmutable, n),
			cls: &semantic.GraphClause{
				PAnchorAlias: "?anchorAlias",
			},
		},
	}

	for _, entry := range testTable {
		// Setup for test:
		tpl, err := triple.Parse(entry.t, literal.DefaultBuilder())
		if err != nil {
			t.Fatalf(`triple.Parse failed for triple "%s": %v`, entry.t, err)
		}

		// Actual test:
		_, err = tripleToRow(tpl, entry.cls)
		if _, ok := err.(*skippableError); !ok {
			t.Errorf(`tripleToRow("%s", %q) = _, %v; want _, skippableError`, tpl, entry.cls, err)
		}
	}
}

func TestDataAccessTripleToRowObjectBindings(t *testing.T) {
	n, pTemporal, l := testNodeTemporalPredicateLiteral(t)
	ta, err := pTemporal.TimeAnchor()
	if err != nil {
		t.Fatal(err)
	}
	_, pImmutable, _ := testNodePredicateLiteral(t)

	testTable := []struct {
		t              string
		cls            *semantic.GraphClause
		oBinding       *table.Cell
		oAlias         *table.Cell
		oTypeAlias     *table.Cell
		oIDAlias       *table.Cell
		oAnchorBinding *table.Cell
		oAnchorAlias   *table.Cell
	}{
		{
			t: fmt.Sprintf("%s\t%s\t%s", n, pTemporal, n),
			cls: &semantic.GraphClause{
				OBinding:   "?o",
				OAlias:     "?alias",
				OTypeAlias: "?type",
				OIDAlias:   "?id",
			},
			oBinding:   &table.Cell{N: n},
			oAlias:     &table.Cell{N: n},
			oTypeAlias: &table.Cell{S: table.CellString(n.Type().String())},
			oIDAlias:   &table.Cell{S: table.CellString(n.ID().String())},
		},
		{
			t: fmt.Sprintf("%s\t%s\t%s", n, pTemporal, pTemporal),
			cls: &semantic.GraphClause{
				OBinding:       "?o",
				OAlias:         "?alias",
				OIDAlias:       "?id",
				OAnchorBinding: "?anchorBinding",
				OAnchorAlias:   "?anchorAlias",
			},
			oBinding:       &table.Cell{P: pTemporal},
			oAlias:         &table.Cell{P: pTemporal},
			oIDAlias:       &table.Cell{S: table.CellString(string(pTemporal.ID()))},
			oAnchorBinding: &table.Cell{T: ta},
			oAnchorAlias:   &table.Cell{T: ta},
		},
		{
			t: fmt.Sprintf("%s\t%s\t%s", n, pTemporal, pTemporal),
			cls: &semantic.GraphClause{
				OTypeAlias: "?type",
				Optional:   true,
			},
			oTypeAlias: &table.Cell{},
		},
		{
			t: fmt.Sprintf("%s\t%s\t%s", n, pImmutable, pImmutable),
			cls: &semantic.GraphClause{
				OTypeAlias: "?type",
				Optional:   true,
			},
			oTypeAlias: &table.Cell{},
		},
		{
			t: fmt.Sprintf("%s\t%s\t%s", n, pTemporal, n),
			cls: &semantic.GraphClause{
				OAnchorBinding: "?anchorBinding",
				OAnchorAlias:   "?anchorAlias",
				Optional:       true,
			},
			oAnchorBinding: &table.Cell{},
			oAnchorAlias:   &table.Cell{},
		},
		{
			t: fmt.Sprintf("%s\t%s\t%s", n, pTemporal, l),
			cls: &semantic.GraphClause{
				OTypeAlias:     "?type",
				OAnchorBinding: "?anchorBinding",
				OAnchorAlias:   "?anchorAlias",
				Optional:       true,
			},
			oTypeAlias:     &table.Cell{},
			oAnchorBinding: &table.Cell{},
			oAnchorAlias:   &table.Cell{},
		},
		{
			t: fmt.Sprintf("%s\t%s\t%s", n, pTemporal, l),
			cls: &semantic.GraphClause{
				OBinding: "?o",
				OAlias:   "?alias",
			},
			oBinding: &table.Cell{L: l},
			oAlias:   &table.Cell{L: l},
		},
		{
			t: fmt.Sprintf("%s\t%s\t%s", n, pImmutable, pImmutable),
			cls: &semantic.GraphClause{
				OBinding: "?o",
				OAlias:   "?alias",
				OIDAlias: "?id",
			},
			oBinding: &table.Cell{P: pImmutable},
			oAlias:   &table.Cell{P: pImmutable},
			oIDAlias: &table.Cell{S: table.CellString(string(pImmutable.ID()))},
		},
		{
			t: fmt.Sprintf("%s\t%s\t%s", n, pImmutable, pImmutable),
			cls: &semantic.GraphClause{
				OAnchorBinding: "?anchorBinding",
				OAnchorAlias:   "?anchorAlias",
				Optional:       true,
			},
			oAnchorBinding: &table.Cell{},
			oAnchorAlias:   &table.Cell{},
		},
	}

	for _, entry := range testTable {
		// Setup for test:
		tpl, err := triple.Parse(entry.t, literal.DefaultBuilder())
		if err != nil {
			t.Fatalf(`triple.Parse failed for triple "%s": %v`, entry.t, err)
		}

		// Actual test:
		r, err := tripleToRow(tpl, entry.cls)
		if err != nil {
			t.Fatalf(`tripleToRow("%s", %q) = _, %v; want _, nil error`, tpl, entry.cls, err)
		}
		bindings := []string{"?o", "?alias", "?type", "?id", "?anchorBinding", "?anchorAlias"}
		entryCells := []*table.Cell{entry.oBinding, entry.oAlias, entry.oTypeAlias, entry.oIDAlias, entry.oAnchorBinding, entry.oAnchorAlias}
		for i, binding := range bindings {
			if got, want := r[binding], entryCells[i]; !reflect.DeepEqual(got, want) {
				t.Errorf(`tripleToRow(%q) = "%s", _; want "%s", _`, binding, got, want)
			}
		}
	}
}

func TestDataAccessTripleToRowObjectBindingsError(t *testing.T) {
	n, pImmutable, l := testNodePredicateLiteral(t)
	_, pTemporal, _ := testNodeTemporalPredicateLiteral(t)

	testTable := []struct {
		t   string
		cls *semantic.GraphClause
	}{
		{
			t: fmt.Sprintf("%s\t%s\t%s", n, pImmutable, n),
			cls: &semantic.GraphClause{
				OAnchorBinding: "?anchorBinding",
			},
		},
		{
			t: fmt.Sprintf("%s\t%s\t%s", n, pImmutable, n),
			cls: &semantic.GraphClause{
				OAnchorAlias: "?anchorAlias",
			},
		},
		{
			t: fmt.Sprintf("%s\t%s\t%s", n, pImmutable, l),
			cls: &semantic.GraphClause{
				OAnchorBinding: "?anchorBinding",
			},
		},
		{
			t: fmt.Sprintf("%s\t%s\t%s", n, pImmutable, l),
			cls: &semantic.GraphClause{
				OAnchorAlias: "?anchorAlias",
			},
		},
		{
			t: fmt.Sprintf("%s\t%s\t%s", n, pImmutable, l),
			cls: &semantic.GraphClause{
				OTypeAlias: "?type",
			},
		},
		{
			t: fmt.Sprintf("%s\t%s\t%s", n, pImmutable, pImmutable),
			cls: &semantic.GraphClause{
				OTypeAlias: "?type",
			},
		},
		{
			t: fmt.Sprintf("%s\t%s\t%s", n, pTemporal, pTemporal),
			cls: &semantic.GraphClause{
				OTypeAlias: "?type",
			},
		},
		{
			t: fmt.Sprintf("%s\t%s\t%s", n, pImmutable, pImmutable),
			cls: &semantic.GraphClause{
				OAnchorBinding: "?anchorBinding",
			},
		},
		{
			t: fmt.Sprintf("%s\t%s\t%s", n, pImmutable, pImmutable),
			cls: &semantic.GraphClause{
				OAnchorAlias: "?anchorAlias",
			},
		},
	}

	for _, entry := range testTable {
		// Setup for test:
		tpl, err := triple.Parse(entry.t, literal.DefaultBuilder())
		if err != nil {
			t.Fatalf(`triple.Parse failed for triple "%s": %v`, entry.t, err)
		}

		// Actual test:
		_, err = tripleToRow(tpl, entry.cls)
		if _, ok := err.(*skippableError); !ok {
			t.Errorf(`tripleToRow("%s", %q) = _, %v; want _, skippableError`, tpl, entry.cls, err)
		}
	}
}

func TestDataAccessTripleToRowObjectBindingsDropping(t *testing.T) {
	n, p, _ := testNodeTemporalPredicateLiteral(t)

	testTable := []struct {
		t   string
		cls *semantic.GraphClause
	}{
		{
			t: fmt.Sprintf("%s\t%s\t%s", n, p, n),
			cls: &semantic.GraphClause{
				OBinding:   "?o",
				OAlias:     "?alias",
				OTypeAlias: "?o",
				OIDAlias:   "?id",
			},
		},
		{
			t: fmt.Sprintf("%s\t%s\t%s", n, p, p),
			cls: &semantic.GraphClause{
				OBinding:       "?o",
				OAlias:         "?alias",
				OIDAlias:       "?id",
				OAnchorBinding: "?anchorBinding",
				OAnchorAlias:   "?o",
			},
		},
	}

	for _, entry := range testTable {
		// Setup for test:
		tpl, err := triple.Parse(entry.t, literal.DefaultBuilder())
		if err != nil {
			t.Fatalf(`triple.Parse failed for triple "%s": %v`, entry.t, err)
		}

		// Actual test:
		r, err := tripleToRow(tpl, entry.cls)
		if err != nil {
			t.Fatalf(`tripleToRow("%s", %q) = _, %v; want _, nil error`, tpl, entry.cls, err)
		}
		if r != nil {
			t.Errorf(`tripleToRow("%s", %q) = %v, _; want nil row, _`, tpl, entry.cls, r)
		}
	}
}
