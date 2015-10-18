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

package table

import (
	"bytes"
	"errors"
	"reflect"
	"testing"
	"time"

	"github.com/google/badwolf/triple/literal"
	"github.com/google/badwolf/triple/node"
	"github.com/google/badwolf/triple/predicate"
)

func TestNew(t *testing.T) {
	testTable := []struct {
		bs  []string
		err bool
	}{
		{[]string{}, false},
		{[]string{"?foo"}, false},
		{[]string{"?foo", "?bar"}, false},
		{[]string{"?foo", "?bar"}, false},
		{[]string{"?foo", "?bar", "?foo", "?bar"}, true},
	}
	for _, entry := range testTable {
		if _, err := New(entry.bs); (err == nil) == entry.err {
			t.Errorf("table.Name failed; want %v for %v ", entry.err, entry.bs)
		}
	}
}

func TestCellString(t *testing.T) {
	now := time.Now()
	n := node.NewBlankNode()
	p, err := predicate.NewImmutable("foo")
	if err != nil {
		t.Fatalf("failed to create predicate with error %v", err)
	}
	l, err := literal.DefaultBuilder().Parse(`"true"^^type:bool`)
	if err != nil {
		t.Fatalf("failed to create literal with error %v", err)
	}
	testTable := []struct {
		c    *Cell
		want string
	}{
		{c: &Cell{S: "foo"}, want: `foo`},
		{c: &Cell{N: n}, want: n.String()},
		{c: &Cell{P: p}, want: p.String()},
		{c: &Cell{L: l}, want: l.String()},
		{c: &Cell{T: &now}, want: now.Format(time.RFC3339Nano)},
	}
	for _, entry := range testTable {
		if got := entry.c.String(); got != entry.want {
			t.Errorf("Cell.String failed to return the right string; got %q, want %q", got, entry.want)
		}
	}
}

func TestRowToTextLine(t *testing.T) {
	r, b := make(Row), &bytes.Buffer{}
	r["?foo"] = &Cell{S: "foo"}
	r["?bar"] = &Cell{S: "bar"}
	err := r.ToTextLine(b, []string{"?foo", "?bar"}, "")
	if err != nil {
		t.Errorf("row.ToTextLine failed to serialize the row with error %v", err)
	}
	if got, want := b.String(), "foo\tbar"; got != want {
		t.Errorf("row.ToTextLine failed to serialize the row; got %q, want %q", got, want)
	}
}

func TestTableManipulation(t *testing.T) {
	newRow := func() Row {
		r := make(Row)
		r["?foo"] = &Cell{S: "foo"}
		r["?bar"] = &Cell{S: "bar"}
		return r
	}
	tbl, err := New([]string{"?foo", "?bar"})
	if err != nil {
		t.Fatal(errors.New("tbl.New failed to crate a new valid table"))
	}
	for i := 0; i < 10; i++ {
		tbl.AddRow(newRow())
	}
	if got, want := tbl.NumRows(), 10; got != want {
		t.Errorf("tbl.Number: got %d,  wanted %d instead", got, want)
	}
	c := newRow()
	for _, r := range tbl.Rows() {
		if !reflect.DeepEqual(c, r) {
			t.Errorf("tbl contains inconsitent row %v, want %v", r, c)
		}
	}
	for i := 0; i < 10; i++ {
		if r, ok := tbl.Row(i); !ok || !reflect.DeepEqual(c, r) {
			t.Errorf("tbl contains inconsitent row %v, want %v", r, c)
		}
	}
	if got, want := tbl.Bindings(), []string{"?foo", "?bar"}; !reflect.DeepEqual(got, want) {
		t.Errorf("tbl.Bindings() return inconsistent bindings; got %v, want %v", got, want)
	}
}

func TestBindingExtensions(t *testing.T) {
	testBindings := []string{"?foo", "?bar"}
	tbl, err := New(testBindings)
	if err != nil {
		t.Fatal(errors.New("tbl.New failed to crate a new valid table"))
	}
	for _, b := range testBindings {
		if !tbl.HasBinding(b) {
			t.Errorf("tbl.HasBinding(%q) returned false for an existing binding", b)
		}
	}
	newBindings := []string{"?new", "?biding"}
	for _, b := range newBindings {
		if tbl.HasBinding(b) {
			t.Errorf("tbl.HasBinding(%q) returned true for a non existing binding", b)
		}
	}
	mixedBindings := append(testBindings, testBindings...)
	mixedBindings = append(mixedBindings, newBindings...)
	tbl.AddBindings(mixedBindings)
	for _, b := range tbl.Bindings() {
		if !tbl.HasBinding(b) {
			t.Errorf("tbl.HasBinding(%q) returned false for an existing binding", b)
		}
	}
	if got, want := len(tbl.Bindings()), 4; got != want {
		t.Errorf("tbl.Bindings() returned the wrong number of bindings; got %d, want %d", got, want)
	}
}

func TestTableToText(t *testing.T) {
	newRow := func() Row {
		r := make(Row)
		r["?foo"] = &Cell{S: "foo"}
		r["?bar"] = &Cell{S: "bar"}
		return r
	}
	tbl, err := New([]string{"?foo", "?bar"})
	if err != nil {
		t.Fatal(errors.New("tbl.New failed to crate a new valid table"))
	}
	for i := 0; i < 3; i++ {
		tbl.AddRow(newRow())
	}
	want := "?foo, ?bar\nfoo, bar\nfoo, bar\nfoo, bar\n"
	if got, err := tbl.ToText(", "); err != nil || got.String() != want {
		t.Errorf("tbl.ToText failed to rerialize the text;\nGot:\n%s\nWant:\n%s", got, want)
	}
}
