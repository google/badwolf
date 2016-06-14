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
	"fmt"
	"reflect"
	"strings"
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
		{c: &Cell{S: CellString("foo")}, want: `foo`},
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
	r["?foo"] = &Cell{S: CellString("foo")}
	r["?bar"] = &Cell{S: CellString("bar")}
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
		r["?foo"] = &Cell{S: CellString("foo")}
		r["?bar"] = &Cell{S: CellString("bar")}
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
		r["?foo"] = &Cell{S: CellString("foo")}
		r["?bar"] = &Cell{S: CellString("bar")}
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

func TestEqualBindings(t *testing.T) {
	testTable := []struct {
		b1   map[string]bool
		b2   map[string]bool
		want bool
	}{
		{
			b1:   map[string]bool{},
			b2:   map[string]bool{},
			want: true,
		},
		{
			b1: map[string]bool{},
			b2: map[string]bool{
				"?foo": true,
				"?bar": true,
			},
			want: false,
		},
		{
			b1: map[string]bool{
				"?foo": true,
				"?bar": true,
			},
			b2:   map[string]bool{},
			want: false,
		},
		{
			b1: map[string]bool{
				"?foo": true,
				"?bar": true,
			},
			b2: map[string]bool{
				"?foo":   true,
				"?bar":   true,
				"?other": true,
			},
			want: false,
		},
	}
	for _, entry := range testTable {
		if got, want := equalBindings(entry.b1, entry.b2), entry.want; got != want {
			t.Errorf("equalBidings returned %v instead of %v for values %v, %v", got, want, entry.b1, entry.b2)
		}
	}
}

func testTable(t *testing.T) *Table {
	newRow := func() Row {
		r := make(Row)
		r["?foo"] = &Cell{S: CellString("foo")}
		r["?bar"] = &Cell{S: CellString("bar")}
		return r
	}
	tbl, err := New([]string{"?foo", "?bar"})
	if err != nil {
		t.Fatal(errors.New("tbl.New failed to crate a new valid table"))
	}
	for i := 0; i < 3; i++ {
		tbl.AddRow(newRow())
	}
	return tbl
}

func TestAppendTable(t *testing.T) {
	newEmpty := func() *Table {
		empty, err := New([]string{})
		if err != nil {
			t.Fatal(err)
		}
		return empty
	}
	newNonEmpty := func(twice bool) *Table {
		tbl := testTable(t)
		if twice {
			tbl.data = append(tbl.data, tbl.data...)
		}
		return tbl
	}
	testTable := []struct {
		t    *Table
		t2   *Table
		want *Table
	}{
		{
			t:    newEmpty(),
			t2:   newNonEmpty(false),
			want: newNonEmpty(false),
		},
		{
			t:    newNonEmpty(false),
			t2:   newNonEmpty(false),
			want: newNonEmpty(true),
		},
	}
	for _, entry := range testTable {
		if err := entry.t.AppendTable(entry.t2); err != nil {
			t.Errorf("Failed to append %s to %s with error %v", entry.t2, entry.t, err)
		}
		if got, want := len(entry.t.Bindings()), len(entry.want.Bindings()); got != want {
			t.Errorf("Append returned the wrong number of bindings; got %d, want %d", got, want)
		}
		if got, want := len(entry.t.Rows()), len(entry.want.Rows()); got != want {
			t.Errorf("Append returned the wrong number of rows; got %d, want %d", got, want)
		}
	}
}

func TestProjectBindings(t *testing.T) {
	testTable := []struct {
		t       *Table
		bs      []string
		success bool
		want    []string
	}{
		{
			t:       testTable(t),
			bs:      []string{},
			success: true,
			want:    []string{},
		},
		{
			t:       testTable(t),
			bs:      []string{"?bar", "?foo"},
			success: true,
			want:    []string{"?bar", "?foo"},
		},
		{
			t:       testTable(t),
			bs:      []string{"?bar"},
			success: true,
			want:    []string{"?bar"},
		},
		{
			t:       testTable(t),
			bs:      []string{"?foo"},
			success: true,
			want:    []string{"?foo"},
		},

		{
			t:       testTable(t),
			bs:      []string{"?bar", "?moo"},
			success: false,
		},
		{
			t:       testTable(t),
			bs:      []string{"?moo"},
			success: false,
		},
	}
	for _, entry := range testTable {
		if err := entry.t.ProjectBindings(entry.bs); (err != nil) == entry.success {
			verb := "accept"
			if !entry.success {
				verb = "reject"
			}
			t.Errorf("Failed to %s project table %s on %s with error %v", verb, entry.t, entry.bs, err)
		}
		if got, want := entry.t.Bindings(), entry.want; entry.success && !reflect.DeepEqual(got, want) {
			t.Errorf("Failed to return the proper bindings; got %s want %s", got, want)
		}
	}
}

func TestDisjointBinding(t *testing.T) {
	testTable := []struct {
		b1   map[string]bool
		b2   map[string]bool
		want bool
	}{
		{
			b1:   map[string]bool{},
			b2:   map[string]bool{},
			want: true,
		},
		{
			b1: map[string]bool{},
			b2: map[string]bool{
				"?foo": true,
				"?bar": true,
			},
			want: true,
		},
		{
			b1: map[string]bool{
				"?foo": true,
				"?bar": true,
			},
			b2:   map[string]bool{},
			want: true,
		},
		{
			b1: map[string]bool{
				"?foo": true,
				"?bar": true,
			},
			b2: map[string]bool{
				"?foo":   true,
				"?bar":   true,
				"?other": true,
			},
			want: false,
		},
	}
	for _, entry := range testTable {
		if got, want := disjointBinding(entry.b1, entry.b2), entry.want; got != want {
			t.Errorf("equalBidings returned %v instead of %v for values %v, %v", got, want, entry.b1, entry.b2)
		}
	}
}

func testDotTable(t *testing.T, bindings []string, size int) *Table {
	newRow := func(n int) Row {
		r := make(Row)
		for _, b := range bindings {
			r[b] = &Cell{S: CellString(fmt.Sprintf("%s_%d", b, n))}
			r[b] = &Cell{S: CellString(fmt.Sprintf("%s_%d", b, n))}
		}
		return r
	}
	tbl, err := New(bindings)
	if err != nil {
		t.Fatal(errors.New("tbl.New failed to crate a new valid table"))
	}
	for i := 0; i < size; i++ {
		tbl.AddRow(newRow(i))
	}
	return tbl
}

func TestDotProduct(t *testing.T) {
	testTable := []struct {
		t    *Table
		t2   *Table
		want *Table
	}{
		{
			t:    testDotTable(t, []string{"?foo"}, 3),
			t2:   testDotTable(t, []string{"?bar"}, 3),
			want: testDotTable(t, []string{"?foo", "?bar"}, 9),
		},
		{
			t:    testDotTable(t, []string{"?foo"}, 3),
			t2:   testDotTable(t, []string{"?bar", "?other"}, 6),
			want: testDotTable(t, []string{"?foo", "?bar", "?other"}, 18),
		},
	}
	for _, entry := range testTable {
		if err := entry.t.DotProduct(entry.t2); err != nil {
			t.Errorf("Failed to dot product %s to %s with error %v", entry.t2, entry.t, err)
		}
		if got, want := len(entry.t.Bindings()), len(entry.want.Bindings()); got != want {
			t.Errorf("Append returned the wrong number of bindings; got %d, want %d", got, want)
		}
		if got, want := len(entry.t.Rows()), len(entry.want.Rows()); got != want {
			t.Errorf("Append returned the wrong number of rows; got %d, want %d", got, want)
		}
	}
}

func TestDotProductContent(t *testing.T) {
	t1, t2 := testDotTable(t, []string{"?foo"}, 3), testDotTable(t, []string{"?bar"}, 3)
	if err := t1.DotProduct(t2); err != nil {
		t.Errorf("Failed to dot product %s to %s with error %v", t2, t1, err)
	}
	if len(t1.Rows()) != 9 {
		t.Errorf("DotProduct returned the wrong number of rows (%d)", len(t1.Rows()))
	}
	if len(t1.Bindings()) != 2 {
		t.Errorf("DotProduct returned the wrong number of bindings (%d)", len(t1.Bindings()))
	}
	fn := func(idx int) *Cell {
		return &Cell{S: CellString(fmt.Sprintf("?foo_%d", idx/3))}
	}
	bn := func(idx int) *Cell {
		return &Cell{S: CellString(fmt.Sprintf("?bar_%d", idx%3))}
	}
	for idx, r := range t1.Rows() {
		if gf, wf, gb, wb := r["?foo"], fn(idx), r["?bar"], bn(idx); !reflect.DeepEqual(gf, wf) || !reflect.DeepEqual(gb, wb) {
			t.Errorf("DotProduct returned the wrong row %v on position %d; %v %v %v %v", r, idx, gf, wf, gb, wb)
		}
	}
}

func TestDeleteRow(t *testing.T) {
	testTable := []struct {
		t   *Table
		idx int
		out bool
	}{
		{
			t:   testDotTable(t, []string{"?foo"}, 3),
			idx: -1,
			out: false,
		},
		{
			t:   testDotTable(t, []string{"?foo"}, 3),
			idx: 0,
			out: true,
		},
		{
			t:   testDotTable(t, []string{"?foo"}, 3),
			idx: 1,
			out: true,
		},
		{
			t:   testDotTable(t, []string{"?foo"}, 3),
			idx: 2,
			out: true,
		},
		{
			t:   testDotTable(t, []string{"?foo"}, 3),
			idx: 3,
			out: false,
		},
	}
	for _, entry := range testTable {
		if err := entry.t.DeleteRow(entry.idx); (err != nil) == entry.out {
			t.Errorf("Failed to delete row %d with error %v", entry.idx, err)
		}
		if entry.out && len(entry.t.Rows()) != 2 {
			t.Errorf("Failed successfully delete row %d ending with %d rows", entry.idx, len(entry.t.Rows()))
		}
	}
}

func TestTruncate(t *testing.T) {
	tbl := testDotTable(t, []string{"?foo"}, 3)

	if got, want := len(tbl.Rows()), 3; got != want {
		t.Errorf("Failed to create a table with %d rows instead of %v", got, want)
	}
	tbl.Truncate()
	if got, want := len(tbl.Rows()), 0; got != want {
		t.Errorf("Failed to truncate a table; got %d rows, want %v", got, want)
	}
}

func Limit(t *testing.T) {
	tbl := testDotTable(t, []string{"?foo"}, 3)
	if got, want := len(tbl.Rows()), 3; got != want {
		t.Errorf("Failed to create a table with %d rows instead of %v", got, want)
	}

	testTable := []struct {
		in   int64
		want int
	}{
		{100, 3},
		{4, 3},
		{3, 3},
		{2, 2},
		{1, 1},
		{0, 0},
	}
	for _, entry := range testTable {
		tbl.Limit(entry.in)
		if got, want := len(tbl.Rows()), entry.want; got != want {
			t.Errorf("Failed to limit a table correctly; want %d rows, got %v", got, want)
		}
	}
}

func TestStringLess(t *testing.T) {
	testTable := []struct {
		i    string
		j    string
		desc bool
		less int
	}{
		{"", "", false, 0},
		{"", "", true, 0},
		{" 1", "1 ", false, 0},
		{" 1", "1 ", true, 0},
		{" 1", "2 ", false, -1},
		{" 1", "2 ", true, 1},
		{" 2", "1 ", false, 1},
		{" 2", "1 ", true, -1},
	}
	for _, entry := range testTable {
		if got, want := stringLess(entry.i, entry.j, entry.desc), entry.less; got != want {
			t.Errorf("table.stringLess(%q, %q, %v) = %d, want %d", entry.i, entry.j, entry.desc, got, want)
		}
	}
}

func TestRowLess(t *testing.T) {
	r1 := Row{
		"?s": &Cell{S: CellString("1")},
		"?t": &Cell{S: CellString("1")},
	}
	r2 := Row{
		"?s": &Cell{S: CellString("2")},
		"?t": &Cell{S: CellString("1")},
	}
	testTable := []struct {
		ri   Row
		rj   Row
		cfg  SortConfig
		less bool
	}{
		{r1, r2, SortConfig{{"?s", false}}, true},
		{r1, r2, SortConfig{{"?s", true}}, false},
		{r1, r2, SortConfig{{"?t", false}}, false},
		{r1, r2, SortConfig{{"?t", true}}, false},
		{r1, r2, SortConfig{{"?t", false}, {"?s", false}}, true},
		{r1, r2, SortConfig{{"?t", false}, {"?s", true}}, false},
		{r1, r2, SortConfig{{"?t", false}, {"?t", false}}, false},
		{r1, r2, SortConfig{{"?t", false}, {"?t", true}}, false},
		{r1, r2, SortConfig{{"?t", true}, {"?t", false}}, false},
		{r1, r2, SortConfig{{"?t", true}, {"?t", true}}, false},
	}

	for _, entry := range testTable {
		if got, want := rowLess(entry.ri, entry.rj, entry.cfg), entry.less; got != want {
			t.Errorf("table.rowLess(%v, %v, %v) = %v; want %v", entry.ri, entry.rj, entry.cfg, got, want)
		}
	}
}

func TestSort(t *testing.T) {
	table := func() *Table {
		return &Table{
			bs: []string{"?s", "?t"},
			mbs: map[string]bool{
				"?s": true,
				"?t": true,
			},
			data: []Row{
				{
					"?s": &Cell{S: CellString("1")},
					"?t": &Cell{S: CellString("1")},
				},
				{
					"?s": &Cell{S: CellString("2")},
					"?t": &Cell{S: CellString("1")},
				},
			},
		}
	}
	testTable := []struct {
		t    *Table
		cfg  SortConfig
		desc bool
	}{
		{table(), SortConfig{{"?s", false}}, false},
		{table(), SortConfig{{"?s", true}}, true},
		{table(), SortConfig{{"?t", false}, {"?s", false}}, false},
		{table(), SortConfig{{"?t", true}, {"?s", false}}, false},
		{table(), SortConfig{{"?t", false}, {"?s", true}}, true},
		{table(), SortConfig{{"?t", true}, {"?s", true}}, true},
	}

	for _, entry := range testTable {
		entry.t.Sort(entry.cfg)
		s1, s2 := entry.t.data[0]["?s"].S, entry.t.data[1]["?s"].S
		b := *s1 < *s2
		if !entry.desc && !b || entry.desc && b {
			t.Errorf("table.Sort failed to sort table DESC=%v; returned\n%s", entry.desc, entry.t)
		}
	}
}

func TestSumAccumulators(t *testing.T) {
	// int64 sum accumulator.
	var (
		iv interface{}
		ia = NewSumInt64LiteralAccumulator(0)
	)
	for i := int64(0); i < 5; i++ {
		l, _ := literal.DefaultBuilder().Build(literal.Int64, i)
		iv, _ = ia.Accumulate(l)
	}
	if got, want := iv.(int64), int64(10); got != want {
		t.Errorf("Int64 sum accumulator failed; got %d, want %d", got, want)
	}
	// float64 sum accumulator.
	var (
		fv interface{}
		fa = NewSumFloat64LiteralAccumulator(0)
	)
	for i := float64(0); i < 5; i += 1.0 {
		l, _ := literal.DefaultBuilder().Build(literal.Float64, i)
		fv, _ = fa.Accumulate(l)
	}
	if got, want := fv.(float64), float64(10); got != want {
		t.Errorf("Int64 sum accumulator failed; got %f, want %f", got, want)
	}
}

func TestCountAccumulators(t *testing.T) {
	// Count accumulator.
	var (
		cv interface{}
		ca = NewCountAccumulator()
	)
	for i := int64(0); i < 5; i++ {
		cv, _ = ca.Accumulate(i)
	}
	if got, want := cv.(int64), int64(5); got != want {
		t.Errorf("Count accumulator failed; got %d, want %d", got, want)
	}
	// Count distinct accumulator
	var (
		dv interface{}
		da = NewCountDistinctAccumulator()
	)
	for i := int64(0); i < 10; i++ {
		l, _ := literal.DefaultBuilder().Build(literal.Int64, i%2)
		dv, _ = da.Accumulate(l)
	}
	if got, want := dv.(int64), int64(2); got != want {
		t.Errorf("Count distinct accumulator failed; got %d, want %d", got, want)
	}
}

func TestGroupRangeReduce(t *testing.T) {
	int64LiteralCell := func(i int64) *Cell {
		l, _ := literal.DefaultBuilder().Build(literal.Int64, i)
		return &Cell{L: l}
	}
	testTable := []struct {
		tbl   *Table
		alias map[string]string
		acc   map[string]Accumulator
		want  Row
	}{
		{
			tbl:   testTable(t),
			alias: map[string]string{"?bar": "?bar_alias"},
			acc:   map[string]Accumulator{"?bar": NewCountAccumulator()},
			want: Row{
				"?foo":       &Cell{S: CellString("foo")},
				"?bar_alias": int64LiteralCell(int64(3)),
			},
		},
		{
			tbl:   testTable(t),
			alias: map[string]string{"?foo": "?foo_alias"},
			acc:   map[string]Accumulator{"?foo": NewCountAccumulator()},
			want: Row{
				"?bar":       &Cell{S: CellString("bar")},
				"?foo_alias": int64LiteralCell(int64(3)),
			},
		},
		{
			tbl: testTable(t),
			alias: map[string]string{
				"?foo": "?foo_alias",
				"?bar": "?bar_alias",
			},
			acc: map[string]Accumulator{"?foo": NewCountAccumulator()},
			want: Row{
				"?bar_alias": &Cell{S: CellString("bar")},
				"?foo_alias": int64LiteralCell(int64(3)),
			},
		},
		// Proper rejection tests.
		{
			tbl:   testTable(t),
			alias: map[string]string{"?foo": "?foo_alias"},
			acc:   map[string]Accumulator{"?bar": NewCountAccumulator()},
		},
	}
	for _, entry := range testTable {
		got, err := entry.tbl.groupRangeReduce(0, entry.tbl.NumRows(), entry.alias, entry.acc)
		want := entry.want
		if want != nil && err != nil {
			t.Errorf("table.groupRangeReduce failed to compute reduced row with error %v", err)
		}
		if want == nil && err == nil {
			t.Errorf("table.groupRangeReduce should have failed to reduced row; instead it produced\n%v", got)
		}
		if want != nil && !reflect.DeepEqual(got, want) {
			t.Errorf("table.groupRangeReduce failed to produce correct reduce row; got\n%s, want\n%s", got, want)
		}
	}
}

func TestFullGroupRangeReduce(t *testing.T) {
	int64LiteralCell := func(i int64) *Cell {
		l, _ := literal.DefaultBuilder().Build(literal.Int64, i)
		return &Cell{L: l}
	}
	testTable := []struct {
		id   string
		tbl  *Table
		red  map[string]map[string]AliasAccPair
		want Row
	}{
		{
			id:  "group ?foo and pass ?bar",
			tbl: testTable(t),
			red: map[string]map[string]AliasAccPair{
				"?foo": map[string]AliasAccPair{
					"?foo_alias": AliasAccPair{
						InAlias:  "?foo",
						OutAlias: "?foo_alias",
						Acc:      NewCountAccumulator(),
					},
				},
				"?bar": map[string]AliasAccPair{
					"?bar_alias": AliasAccPair{
						InAlias:  "?bar",
						OutAlias: "?bar_alias",
					},
				},
			},
			want: Row{
				"?bar_alias": &Cell{S: CellString("bar")},
				"?foo_alias": int64LiteralCell(int64(3)),
			},
		},
		{
			id:  "group count ?foo and alias ?foo and ?bar",
			tbl: testTable(t),
			red: map[string]map[string]AliasAccPair{
				"?foo": map[string]AliasAccPair{
					"?foo_count": AliasAccPair{
						InAlias:  "?foo",
						OutAlias: "?foo_count",
						Acc:      NewCountAccumulator(),
					},
					"?foo_alias": AliasAccPair{
						InAlias:  "?foo",
						OutAlias: "?foo_alias",
					},
				},
				"?bar": map[string]AliasAccPair{
					"?bar_alias": AliasAccPair{
						InAlias:  "?bar",
						OutAlias: "?bar_alias",
					},
				},
			},
			want: Row{
				"?foo_alias": &Cell{S: CellString("foo")},
				"?bar_alias": &Cell{S: CellString("bar")},
				"?foo_count": int64LiteralCell(int64(3)),
			},
		},
		// Proper rejection tests.
		{
			id:  "reject query",
			tbl: testTable(t),
			red: map[string]map[string]AliasAccPair{
				"?other": map[string]AliasAccPair{
					"?other_alias": AliasAccPair{
						InAlias:  "?other",
						OutAlias: "?other_alias",
						Acc:      NewCountAccumulator(),
					},
				},
			},
		},
	}
	for _, entry := range testTable {
		got, err := entry.tbl.fullGroupRangeReduce(0, entry.tbl.NumRows(), entry.red)
		want := entry.want
		if want != nil && err != nil {
			t.Errorf("table.fullGroupRangeReduce failed %q to compute reduced row with error %v", entry.id, err)
		}
		if want == nil && err == nil {
			t.Errorf("table.fullGroupRangeReduce should have failed %q to reduced row; instead it produced\n%v", entry.id, got)
		}
		if want != nil && !reflect.DeepEqual(got, want) {
			t.Errorf("table.fullGroupRangeReduce failed to produce correct reduce row in %q; got\n%s, want\n%s", entry.id, got, want)
		}
	}
}

func TestTableReduce(t *testing.T) {
	int64LiteralCell := func(i int64) *Cell {
		l, _ := literal.DefaultBuilder().Build(literal.Int64, i)
		return &Cell{L: l}
	}
	testTable := []struct {
		tbl  *Table
		cfg  SortConfig
		aap  []AliasAccPair
		want *Table
	}{
		{
			tbl: &Table{
				bs: []string{"?foo", "?bar"},
				mbs: map[string]bool{
					"?foo": true,
					"?bar": true,
				},
				data: []Row{
					{
						"?foo": &Cell{S: CellString("foo")},
						"?bar": &Cell{S: CellString("bar")},
					},
					{
						"?foo": &Cell{S: CellString("foo")},
						"?bar": &Cell{S: CellString("bar")},
					},
					{
						"?foo": &Cell{S: CellString("foo")},
						"?bar": &Cell{S: CellString("bar")},
					},
				},
			},
			cfg: SortConfig{
				{"?foo", false},
				{"?bar", false},
			},
			aap: []AliasAccPair{
				{
					InAlias:  "?foo",
					OutAlias: "?foo_alias",
				},
				{
					InAlias:  "?bar",
					OutAlias: "?bar_alias",
					Acc:      NewCountAccumulator(),
				},
			},
			want: &Table{
				bs: []string{"?foo_alias", "?bar_alias"},
				mbs: map[string]bool{
					"?foo_alias": true,
					"?bar_alias": true,
				},
				data: []Row{
					{
						"?foo_alias": &Cell{S: CellString("foo")},
						"?bar_alias": int64LiteralCell(int64(3)),
					},
				},
			},
		},
		{
			tbl: &Table{
				bs: []string{"?foo", "?bar"},
				mbs: map[string]bool{
					"?foo": true,
					"?bar": true,
				},
				data: []Row{
					{
						"?foo": &Cell{S: CellString("foo")},
						"?bar": &Cell{S: CellString("bar")},
					},
					{
						"?foo": &Cell{S: CellString("foo")},
						"?bar": &Cell{S: CellString("bar")},
					},
					{
						"?foo": &Cell{S: CellString("foo")},
						"?bar": &Cell{S: CellString("bar")},
					},
					{
						"?foo": &Cell{S: CellString("foo2")},
						"?bar": &Cell{S: CellString("bar2")},
					},
					{
						"?foo": &Cell{S: CellString("foo2")},
						"?bar": &Cell{S: CellString("bar2")},
					},
					{
						"?foo": &Cell{S: CellString("foo3")},
						"?bar": &Cell{S: CellString("bar3")},
					},
				},
			},
			cfg: SortConfig{{"?foo", false}},
			aap: []AliasAccPair{
				{
					InAlias:  "?foo",
					OutAlias: "?foo_alias",
				},
				{
					InAlias:  "?bar",
					OutAlias: "?bar_alias",
					Acc:      NewCountAccumulator(),
				},
			},
			want: &Table{
				bs: []string{"?foo_alias", "?bar_alias"},
				mbs: map[string]bool{
					"?foo_alias": true,
					"?bar_alias": true,
				},
				data: []Row{
					{
						"?foo_alias": &Cell{S: CellString("foo")},
						"?bar_alias": int64LiteralCell(int64(3)),
					},
					{
						"?foo_alias": &Cell{S: CellString("foo2")},
						"?bar_alias": int64LiteralCell(int64(2)),
					},
					{
						"?foo_alias": &Cell{S: CellString("foo3")},
						"?bar_alias": int64LiteralCell(int64(1)),
					},
				},
			},
		},
		{
			tbl: &Table{
				bs: []string{"?foo", "?bar"},
				mbs: map[string]bool{
					"?foo": true,
					"?bar": true,
				},
				data: []Row{
					{
						"?foo": &Cell{S: CellString("foo")},
						"?bar": &Cell{S: CellString("bar")},
					},
					{
						"?foo": &Cell{S: CellString("foo")},
						"?bar": &Cell{S: CellString("bar")},
					},
					{
						"?foo": &Cell{S: CellString("foo")},
						"?bar": &Cell{S: CellString("bar")},
					},
					{
						"?foo": &Cell{S: CellString("foo2")},
						"?bar": &Cell{S: CellString("bar2")},
					},
					{
						"?foo": &Cell{S: CellString("foo2")},
						"?bar": &Cell{S: CellString("bar2")},
					},
					{
						"?foo": &Cell{S: CellString("foo3")},
						"?bar": &Cell{S: CellString("bar3")},
					},
				},
			},
			cfg: SortConfig{{"?foo", true}},
			aap: []AliasAccPair{
				{
					InAlias:  "?foo",
					OutAlias: "?foo_alias",
				},
				{
					InAlias:  "?bar",
					OutAlias: "?bar_alias",
					Acc:      NewCountAccumulator(),
				},
			},
			want: &Table{
				bs: []string{"?foo_alias", "?bar_alias"},
				mbs: map[string]bool{
					"?foo_alias": true,
					"?bar_alias": true,
				},
				data: []Row{
					{
						"?foo_alias": &Cell{S: CellString("foo3")},
						"?bar_alias": int64LiteralCell(int64(1)),
					},
					{
						"?foo_alias": &Cell{S: CellString("foo2")},
						"?bar_alias": int64LiteralCell(int64(2)),
					},
					{
						"?foo_alias": &Cell{S: CellString("foo")},
						"?bar_alias": int64LiteralCell(int64(3)),
					},
				},
			},
		},
	}
	for _, entry := range testTable {
		err := entry.tbl.Reduce(entry.cfg, entry.aap)
		got, want := entry.tbl, entry.want
		if want != nil && err != nil {
			t.Errorf("table.Reduce failed to compute reduced row with error %v", err)
		}
		if want == nil && err == nil {
			t.Errorf("table.Reduce(%v, %v) should have failed to reduced table; instead it produced\n%v", entry.cfg, entry.aap, got)
		}
		if want != nil && !reflect.DeepEqual(got, want) {
			t.Errorf("table.Reduce failed to produce correct reduce row; got\n%s, want\n%s", got, want)
		}
	}
}

func TestFilter(t *testing.T) {
	table := func() *Table {
		return &Table{
			bs: []string{"?s", "?t"},
			mbs: map[string]bool{
				"?s": true,
				"?t": true,
			},
			data: []Row{
				{
					"?s": &Cell{S: CellString("1s")},
					"?t": &Cell{S: CellString("1t")},
				},
				{
					"?s": &Cell{S: CellString("2s")},
					"?t": &Cell{S: CellString("2t")},
				},
				{
					"?s": &Cell{S: CellString("3s")},
					"?t": &Cell{S: CellString("3t")},
				},
			},
		}
	}
	testTable := []struct {
		t    *Table
		f    func(Row) bool
		want int
	}{
		{
			t: table(),
			f: func(Row) bool {
				return true
			},
			want: 0,
		},
		{
			t: table(),
			f: func(Row) bool {
				return false
			},
			want: 3,
		},
		{
			t: table(),
			f: func(r Row) bool {
				return r["?s"].String() == "1s"
			},
			want: 2,
		},
		{
			t: table(),
			f: func(r Row) bool {
				return strings.Index(r["?s"].String(), "1s") != -1
			},
			want: 2,
		},
		{
			t: table(),
			f: func(r Row) bool {
				return strings.Index(r["?s"].String(), "t") != -1
			},
			want: 3,
		},
		{
			t: table(),
			f: func(r Row) bool {
				return strings.Index(r["?t"].String(), "t") != -1
			},
			want: 0,
		},
	}
	for _, entry := range testTable {
		entry.t.Filter(entry.f)
		if got, want := entry.t.NumRows(), entry.want; got != want {
			t.Errorf("table.Filter failed to remove entries from table; got %d, want %d, output\n%v", got, want, entry.t)
		}
	}
}
