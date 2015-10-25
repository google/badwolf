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
	"reflect"
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

var testTextTriples []string

func init() {
	testTextTriples = []string{
		"/u<john>\t\"knows\"@[]\t/u<mary>",
		"/u<john>\t\"knows\"@[]\t/u<peter>",
		"/u<john>\t\"knows\"@[]\t/u<alice>",
		"/u<mary>\t\"knows\"@[]\t/u<andrew>",
		"/u<mary>\t\"knows\"@[]\t/u<kim>",
		"/u<mary>\t\"knows\"@[]\t/u<alice>",
	}
}

func getTestTriples(t *testing.T) []*triple.Triple {
	var ts []*triple.Triple
	for _, s := range testTextTriples {
		trpl, err := triple.ParseTriple(s, literal.DefaultBuilder())
		if err != nil {
			t.Fatalf("triple.Parse failed to parse valid triple %s with error %v", s, err)
			continue
		}
		ts = append(ts, trpl)
	}
	return ts
}

func getTestStore(t *testing.T) storage.Store {
	ts := getTestTriples(t)
	s := memory.NewStore()
	g, err := s.NewGraph("?test")
	if err != nil {
		t.Fatal(err)
	}
	if err := g.AddTriples(ts); err != nil {
		t.Fatalf("g.AddTriples(_) failed failed to add test triples with error %v", err)
	}
	return s
}

func TestSimpleFetch(t *testing.T) {
	testBindings := []string{"?s", "?p", "?o"}
	cls := &semantic.GraphClause{
		SBinding: "?s",
		PBinding: "?p",
		OBinding: "?o",
	}
	g, err := getTestStore(t).Graph("?test")
	if err != nil {
		t.Fatal(err)
	}
	tbl, err := simpleFetch([]storage.Graph{g}, cls)
	if err != nil {
		t.Errorf("addTriple failed with errorf %v", err)
	}
	if got, want := len(tbl.Bindings()), len(testBindings); got != want {
		t.Errorf("addTriples returned a table with wrong bindings set; got %v, want %v", got, want)
	}
	if got, want := tbl.NumRows(), len(testTextTriples); got != want {
		t.Errorf("addTriples returned the wrong number of rows; got %d, want %d", got, want)
	}
	for _, r := range tbl.Rows() {
		if got, want := len(r), len(testBindings); got != want {
			t.Errorf("addTriples returned row %v with the incorrect number of bindings; got %d, want %d", r, got, want)
		}
	}
}

func TestAddTriples(t *testing.T) {
	testBindings := []string{"?s", "?p", "?o"}
	cls := &semantic.GraphClause{
		SBinding: "?s",
		PBinding: "?p",
		OBinding: "?o",
	}
	g, err := getTestStore(t).Graph("?test")
	if err != nil {
		t.Fatal(err)
	}
	ts, err := g.Triples()
	if err != nil {
		t.Fatal(err)
	}
	tbl, err := table.New([]string{})
	if err != nil {
		t.Fatal(err)
	}
	if err := addTriples(ts, cls, tbl); err != nil {
		t.Errorf("addTriple failed with errorf %v", err)
	}
	if got, want := tbl.NumRows(), len(testTextTriples); got != want {
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

func TestObjeToCell(t *testing.T) {
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

func TestTripleToRowBasicBindings(t *testing.T) {
	n, p, l := testNodePredicateLiteral(t)
	cls := &semantic.GraphClause{
		SBinding: "?s",
		PBinding: "?p",
		OBinding: "?o",
	}
	testTable := []struct {
		t  string
		sc *table.Cell
		pc *table.Cell
		oc *table.Cell
	}{
		{
			t:  n.String() + "\t" + p.String() + "\t" + n.String(),
			sc: &table.Cell{N: n},
			pc: &table.Cell{P: p},
			oc: &table.Cell{N: n},
		},
		{
			t:  n.String() + "\t" + p.String() + "\t" + p.String(),
			sc: &table.Cell{N: n},
			pc: &table.Cell{P: p},
			oc: &table.Cell{P: p},
		},
		{
			t:  n.String() + "\t" + p.String() + "\t" + l.String(),
			sc: &table.Cell{N: n},
			pc: &table.Cell{P: p},
			oc: &table.Cell{L: l},
		},
	}
	for _, entry := range testTable {
		tpl, err := triple.ParseTriple(entry.t, literal.DefaultBuilder())
		if err != nil {
			t.Errorf("triple.Parse failed to parse valid triple %q with error %v", entry.t, err)
		}
		r, err := tripleToRow(tpl, cls)
		if err != nil {
			t.Errorf("tripleToRow for triple %q and clasuse %v, failed with error %v", tpl, cls, err)
		}
		if got, want := r["?s"], entry.sc; !reflect.DeepEqual(got, want) {
			t.Errorf("tripleToRow failed to return right value for binding \"?s\"; got %q, want %q", got, want)
		}
		if got, want := r["?p"], entry.pc; !reflect.DeepEqual(got, want) {
			t.Errorf("tripleToRow failed to return right value for binding \"?p\"; got %q, want %q", got, want)
		}
		if got, want := r["?o"], entry.oc; !reflect.DeepEqual(got, want) {
			t.Errorf("tripleToRow failed to return right value for binding \"?o\"; got %q, want %q", got, want)
		}
	}
}

func TestTripleToRowSubjectBindings(t *testing.T) {
	n, p, _ := testNodePredicateLiteral(t)
	testTable := []struct {
		t   string
		cls *semantic.GraphClause
		sc  *table.Cell
		ac  *table.Cell
		tc  *table.Cell
		ic  *table.Cell
	}{
		{
			t: n.String() + "\t" + p.String() + "\t" + n.String(),
			cls: &semantic.GraphClause{
				SBinding:   "?s",
				SAlias:     "?alias",
				STypeAlias: "?type",
				SIDAlias:   "?id",
			},
			sc: &table.Cell{N: n},
			ac: &table.Cell{N: n},
			tc: &table.Cell{S: n.Type().String()},
			ic: &table.Cell{S: n.ID().String()},
		},
	}
	for _, entry := range testTable {
		tpl, err := triple.ParseTriple(entry.t, literal.DefaultBuilder())
		if err != nil {
			t.Errorf("triple.Parse failed to parse valid triple %q with error %v", entry.t, err)
		}
		r, err := tripleToRow(tpl, entry.cls)
		if err != nil {
			t.Errorf("tripleToRow for triple %q and clasuse %v, failed with error %v", tpl, entry.cls, err)
		}
		if got, want := r["?s"], entry.sc; !reflect.DeepEqual(got, want) {
			t.Errorf("tripleToRow failed to return right value for binding \"?s\"; got %q, want %q", got, want)
		}
		if got, want := r["?alias"], entry.ac; !reflect.DeepEqual(got, want) {
			t.Errorf("tripleToRow failed to return right value for binding alias \"?alias\"; got %q, want %q", got, want)
		}
		if got, want := r["?type"], entry.tc; !reflect.DeepEqual(got, want) {
			t.Errorf("tripleToRow failed to return right value for binding \"?type\"; got %q, want %q", got, want)
		}
		if got, want := r["?id"], entry.ic; !reflect.DeepEqual(got, want) {
			t.Errorf("tripleToRow failed to return right value for binding \"?ud\"; got %q, want %q", got, want)
		}
	}
}

func TestTripleToRowPredicateBindings(t *testing.T) {
	n, p, _ := testNodeTemporalPredicateLiteral(t)
	ts, err := p.TimeAnchor()
	if err != nil {
		t.Fatal(err)
	}
	testTable := []struct {
		t   string
		cls *semantic.GraphClause
		bc  *table.Cell
		ac  *table.Cell
		ic  *table.Cell
		tc  *table.Cell
		atc *table.Cell
	}{
		{
			t: n.String() + "\t" + p.String() + "\t" + n.String(),
			cls: &semantic.GraphClause{
				PBinding:       "?p",
				PAlias:         "?alias",
				PIDAlias:       "?id",
				PAnchorBinding: "?ts",
				PAnchorAlias:   "?tsa",
			},
			bc:  &table.Cell{P: p},
			ac:  &table.Cell{P: p},
			ic:  &table.Cell{S: string(p.ID())},
			tc:  &table.Cell{T: ts},
			atc: &table.Cell{T: ts},
		},
	}
	for _, entry := range testTable {
		tpl, err := triple.ParseTriple(entry.t, literal.DefaultBuilder())
		if err != nil {
			t.Errorf("triple.Parse failed to parse valid triple %q with error %v", entry.t, err)
		}
		r, err := tripleToRow(tpl, entry.cls)
		if err != nil {
			t.Errorf("tripleToRow for triple %q and clasuse %v, failed with error %v", tpl, entry.cls, err)
		}
		if got, want := r["?p"], entry.bc; !reflect.DeepEqual(got, want) {
			t.Errorf("tripleToRow failed to return right value for binding \"?p\"; got %q, want %q", got, want)
		}
		if got, want := r["?alias"], entry.ac; !reflect.DeepEqual(got, want) {
			t.Errorf("tripleToRow failed to return right value for binding alias \"?alias\"; got %q, want %q", got, want)
		}
		if got, want := r["?id"], entry.ic; !reflect.DeepEqual(got, want) {
			t.Errorf("tripleToRow failed to return right value for binding \"?ud\"; got %q, want %q", got, want)
		}
		if got, want := r["?ts"], entry.tc; !reflect.DeepEqual(got, want) {
			t.Errorf("tripleToRow failed to return right value for binding \"?ts\"; got %q, want %q", got, want)
		}
		if got, want := r["?tsa"], entry.tc; !reflect.DeepEqual(got, want) {
			t.Errorf("tripleToRow failed to return right value for binding \"?tsa\"; got %q, want %q", got, want)
		}
	}
}
