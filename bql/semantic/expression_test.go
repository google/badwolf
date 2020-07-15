// Copyright 2016 Google Inc. All rights reserved.
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

package semantic

import (
	"fmt"
	"testing"

	"github.com/google/badwolf/bql/lexer"
	"github.com/google/badwolf/bql/table"
	"github.com/google/badwolf/triple/literal"
	"github.com/google/badwolf/triple/node"
)

func TestEvaluationNode(t *testing.T) {
	testTable := []struct {
		eval Evaluator
		r    table.Row
		want bool
		err  bool
	}{
		{
			eval: &evaluationNode{EQ, "?foo", "?wrong_binding"},
			r: table.Row{
				"?foo": &table.Cell{S: table.CellString("foo")},
				"?bar": &table.Cell{S: table.CellString("foo")},
			},
			want: false,
			err:  true,
		},
		{
			eval: &evaluationNode{EQ, "?foo", "?bar"},
			r: table.Row{
				"?foo": &table.Cell{S: table.CellString("foo")},
				"?bar": &table.Cell{S: table.CellString("bar")},
			},
			want: false,
			err:  false,
		},
		{
			eval: &evaluationNode{EQ, "", "?bar"},
			r: table.Row{
				"?foo": &table.Cell{S: table.CellString("foo")},
				"?bar": &table.Cell{S: table.CellString("bar")},
			},
			want: false,
			err:  true,
		},
		{
			eval: &evaluationNode{EQ, "?foo", ""},
			r: table.Row{
				"?foo": &table.Cell{S: table.CellString("foo")},
				"?bar": &table.Cell{S: table.CellString("bar")},
			},
			want: false,
			err:  true,
		},
		{
			eval: &evaluationNode{EQ, "?foo", "?bar"},
			r: table.Row{
				"?foo": &table.Cell{S: table.CellString("foo")},
				"?bar": &table.Cell{S: table.CellString("foo")},
			},
			want: true,
			err:  false,
		},
		{
			eval: &evaluationNode{LT, "?foo", "?bar"},
			r: table.Row{
				"?foo": &table.Cell{S: table.CellString("foo")},
				"?bar": &table.Cell{S: table.CellString("bar")},
			},
			want: false,
			err:  false,
		},
		{
			eval: &evaluationNode{GT, "?foo", "?bar"},
			r: table.Row{
				"?foo": &table.Cell{S: table.CellString("foo")},
				"?bar": &table.Cell{S: table.CellString("bar")},
			},
			want: true,
			err:  false,
		},
	}
	for _, entry := range testTable {
		got, err := entry.eval.Evaluate(entry.r)
		if !entry.err && err != nil {
			t.Errorf("failed to evaluate op %q for %v on row %v with error %v", entry.eval.(*evaluationNode).op, entry.eval, entry.r, err)
		}
		if want := entry.want; got != want {
			t.Errorf("failed to evaluate op %q for %v on row %v; got %v, want %v", entry.eval.(*evaluationNode).op, entry.eval, entry.r, got, want)
		}
	}
}

func TestBooleanEvaluationNode(t *testing.T) {
	testTable := []struct {
		eval Evaluator
		want bool
		err  bool
	}{
		{
			eval: &booleanNode{op: NOT, lS: true, lE: &AlwaysReturn{true}},
			want: false,
			err:  false,
		},
		{
			eval: &booleanNode{op: NOT, lS: true, lE: &AlwaysReturn{false}},
			want: true,
			err:  false,
		},
		{
			eval: &booleanNode{op: NOT, lS: false, lE: &AlwaysReturn{false}},
			want: false,
			err:  true,
		},
		{
			eval: &booleanNode{op: OR, lS: true, lE: &AlwaysReturn{false}, rS: true, rE: &AlwaysReturn{false}},
			want: false,
			err:  false,
		},
		{
			eval: &booleanNode{op: OR, lS: true, lE: &AlwaysReturn{false}, rS: true, rE: &AlwaysReturn{true}},
			want: true,
			err:  false,
		},
		{
			eval: &booleanNode{op: OR, lS: true, lE: &AlwaysReturn{true}, rS: true, rE: &AlwaysReturn{false}},
			want: true,
			err:  false,
		},
		{
			eval: &booleanNode{op: OR, lS: true, lE: &AlwaysReturn{true}, rS: true, rE: &AlwaysReturn{true}},
			want: true,
			err:  false,
		},
		{
			eval: &booleanNode{op: AND, lS: true, lE: &AlwaysReturn{false}, rS: true, rE: &AlwaysReturn{false}},
			want: false,
			err:  false,
		},
		{
			eval: &booleanNode{op: AND, lS: true, lE: &AlwaysReturn{false}, rS: true, rE: &AlwaysReturn{true}},
			want: false,
			err:  false,
		},
		{
			eval: &booleanNode{op: AND, lS: true, lE: &AlwaysReturn{true}, rS: true, rE: &AlwaysReturn{false}},
			want: false,
			err:  false,
		},
		{
			eval: &booleanNode{op: AND, lS: true, lE: &AlwaysReturn{true}, rS: true, rE: &AlwaysReturn{true}},
			want: true,
			err:  false,
		},
		{
			eval: &booleanNode{op: AND, lS: false, lE: &AlwaysReturn{true}, rS: true, rE: &AlwaysReturn{true}},
			want: false,
			err:  true,
		},
		{
			eval: &booleanNode{op: AND, lS: true, lE: &AlwaysReturn{true}, rS: false, rE: &AlwaysReturn{true}},
			want: false,
			err:  true,
		},
	}
	for _, entry := range testTable {
		got, err := entry.eval.Evaluate(table.Row{})
		if !entry.err && err != nil {
			t.Errorf("failed to evaluate op %q for %v with error %v", entry.eval.(*booleanNode).op, entry.eval, err)
		}
		if want := entry.want; got != want {
			t.Errorf("failed to evaluate op %q for %v; got %v, want %v", entry.eval.(*booleanNode).op, entry.eval, got, want)
		}
	}
}

func buildLiteralOrDie(textLiteral string) *literal.Literal {
	lit, err := literal.DefaultBuilder().Parse(textLiteral)
	if err != nil {
		panic("Could not parse text literal got err: " + err.Error())
	}
	return lit
}

func newNodeFromStringOrDie(nodeType, nodeID string) *node.Node {
	n, err := node.NewNodeFromStrings(nodeType, nodeID)
	if err != nil {
		panic(fmt.Sprintf("Could not build node from type %s and value %s", nodeType, nodeID))
	}
	return n
}

func TestNewEvaluator(t *testing.T) {
	testTable := []struct {
		id   string
		in   []ConsumedElement
		r    table.Row
		err  bool
		want bool
	}{
		{
			id: "?foo = ?bar",
			in: []ConsumedElement{
				NewConsumedToken(&lexer.Token{
					Type: lexer.ItemBinding,
					Text: "?foo",
				}),
				NewConsumedToken(&lexer.Token{
					Type: lexer.ItemEQ,
				}),
				NewConsumedToken(&lexer.Token{
					Type: lexer.ItemBinding,
					Text: "?bar",
				}),
			},
			r: table.Row{
				"?foo": &table.Cell{S: table.CellString("VALUE")},
				"?bar": &table.Cell{S: table.CellString("VALUE")},
			},
			err:  false,
			want: true,
		},
		{
			id: "?foo < ?bar",
			in: []ConsumedElement{
				NewConsumedToken(&lexer.Token{
					Type: lexer.ItemBinding,
					Text: "?foo",
				}),
				NewConsumedToken(&lexer.Token{
					Type: lexer.ItemLT,
				}),
				NewConsumedToken(&lexer.Token{
					Type: lexer.ItemBinding,
					Text: "?bar",
				}),
			},
			r: table.Row{
				"?foo": &table.Cell{S: table.CellString("foo")},
				"?bar": &table.Cell{S: table.CellString("bar")},
			},
			err:  false,
			want: false,
		},
		{
			id: "?foo > ?bar",
			in: []ConsumedElement{
				NewConsumedToken(&lexer.Token{
					Type: lexer.ItemBinding,
					Text: "?foo",
				}),
				NewConsumedToken(&lexer.Token{
					Type: lexer.ItemGT,
				}),
				NewConsumedToken(&lexer.Token{
					Type: lexer.ItemBinding,
					Text: "?bar",
				}),
			},
			r: table.Row{
				"?foo": &table.Cell{S: table.CellString("foo")},
				"?bar": &table.Cell{S: table.CellString("bar")},
			},
			err:  false,
			want: true,
		},
		{
			id: "not(?foo = ?bar)",
			in: []ConsumedElement{
				NewConsumedToken(&lexer.Token{
					Type: lexer.ItemNot,
				}),
				NewConsumedToken(&lexer.Token{
					Type: lexer.ItemLPar,
				}),
				NewConsumedToken(&lexer.Token{
					Type: lexer.ItemBinding,
					Text: "?foo",
				}),
				NewConsumedToken(&lexer.Token{
					Type: lexer.ItemEQ,
				}),
				NewConsumedToken(&lexer.Token{
					Type: lexer.ItemBinding,
					Text: "?bar",
				}),
				NewConsumedToken(&lexer.Token{
					Type: lexer.ItemRPar,
				}),
			},
			r: table.Row{
				"?foo": &table.Cell{S: table.CellString("VALUE")},
				"?bar": &table.Cell{S: table.CellString("VALUE")},
			},
			err:  false,
			want: false,
		},
		{
			id: "(?foo < ?bar) or (?foo > ?bar)",
			in: []ConsumedElement{
				NewConsumedToken(&lexer.Token{
					Type: lexer.ItemLPar,
				}),
				NewConsumedToken(&lexer.Token{
					Type: lexer.ItemBinding,
					Text: "?foo",
				}),
				NewConsumedToken(&lexer.Token{
					Type: lexer.ItemLT,
				}),
				NewConsumedToken(&lexer.Token{
					Type: lexer.ItemBinding,
					Text: "?bar",
				}),
				NewConsumedToken(&lexer.Token{
					Type: lexer.ItemRPar,
				}),
				NewConsumedToken(&lexer.Token{
					Type: lexer.ItemOr,
				}),
				NewConsumedToken(&lexer.Token{
					Type: lexer.ItemLPar,
				}),
				NewConsumedToken(&lexer.Token{
					Type: lexer.ItemBinding,
					Text: "?foo",
				}),
				NewConsumedToken(&lexer.Token{
					Type: lexer.ItemGT,
				}),
				NewConsumedToken(&lexer.Token{
					Type: lexer.ItemBinding,
					Text: "?bar",
				}),
				NewConsumedToken(&lexer.Token{
					Type: lexer.ItemRPar,
				}),
			},
			r: table.Row{
				"?foo": &table.Cell{S: table.CellString("foo")},
				"?bar": &table.Cell{S: table.CellString("bar")},
			},
			err:  false,
			want: true,
		},
		{
			id: "(?foo < ?bar) and (?foo > ?bar)",
			in: []ConsumedElement{
				NewConsumedToken(&lexer.Token{
					Type: lexer.ItemLPar,
				}),
				NewConsumedToken(&lexer.Token{
					Type: lexer.ItemBinding,
					Text: "?foo",
				}),
				NewConsumedToken(&lexer.Token{
					Type: lexer.ItemLT,
				}),
				NewConsumedToken(&lexer.Token{
					Type: lexer.ItemBinding,
					Text: "?bar",
				}),
				NewConsumedToken(&lexer.Token{
					Type: lexer.ItemRPar,
				}),
				NewConsumedToken(&lexer.Token{
					Type: lexer.ItemAnd,
				}),
				NewConsumedToken(&lexer.Token{
					Type: lexer.ItemLPar,
				}),
				NewConsumedToken(&lexer.Token{
					Type: lexer.ItemBinding,
					Text: "?foo",
				}),
				NewConsumedToken(&lexer.Token{
					Type: lexer.ItemGT,
				}),
				NewConsumedToken(&lexer.Token{
					Type: lexer.ItemBinding,
					Text: "?bar",
				}),
				NewConsumedToken(&lexer.Token{
					Type: lexer.ItemRPar,
				}),
			},
			r: table.Row{
				"?foo": &table.Cell{S: table.CellString("foo")},
				"?bar": &table.Cell{S: table.CellString("bar")},
			},
			err:  false,
			want: false,
		},
		{
			id: `?foo = "abc"^^type:text`,
			in: []ConsumedElement{
				NewConsumedToken(&lexer.Token{
					Type: lexer.ItemBinding,
					Text: "?foo",
				}),
				NewConsumedToken(&lexer.Token{
					Type: lexer.ItemEQ,
				}),
				NewConsumedToken(&lexer.Token{
					Type: lexer.ItemLiteral,
					Text: `"abc"^^type:text`,
				}),
			},
			r: table.Row{
				"?foo": &table.Cell{
					L: buildLiteralOrDie(`"abc"^^type:text`),
				},
			},
			err:  false,
			want: true,
		},
		{
			id: `?s ID ?id = "abc"^^type:text`,
			in: []ConsumedElement{
				NewConsumedToken(&lexer.Token{
					Type: lexer.ItemBinding,
					Text: "?id",
				}),
				NewConsumedToken(&lexer.Token{
					Type: lexer.ItemEQ,
				}),
				NewConsumedToken(&lexer.Token{
					Type: lexer.ItemLiteral,
					Text: `"abc"^^type:text`,
				}),
			},
			r: table.Row{
				"?id": &table.Cell{S: table.CellString("abc")},
			},
			err:  false,
			want: true,
		},
		{
			id: `?s ID ?id < "bbb"^^type:text`,
			in: []ConsumedElement{
				NewConsumedToken(&lexer.Token{
					Type: lexer.ItemBinding,
					Text: "?id",
				}),
				NewConsumedToken(&lexer.Token{
					Type: lexer.ItemLT,
				}),
				NewConsumedToken(&lexer.Token{
					Type: lexer.ItemLiteral,
					Text: `"bbb"^^type:text`,
				}),
			},
			r: table.Row{
				"?id": &table.Cell{S: table.CellString("aaa")},
			},
			err:  false,
			want: true,
		},
		{
			id: `?s ID ?id > "ccc"^^type:text`,
			in: []ConsumedElement{
				NewConsumedToken(&lexer.Token{
					Type: lexer.ItemBinding,
					Text: "?id",
				}),
				NewConsumedToken(&lexer.Token{
					Type: lexer.ItemGT,
				}),
				NewConsumedToken(&lexer.Token{
					Type: lexer.ItemLiteral,
					Text: `"ccc"^^type:text`,
				}),
			},
			r: table.Row{
				"?id": &table.Cell{S: table.CellString("bbb")},
			},
			err:  false,
			want: false,
		},
		{
			id: `?foo = "99.0"^^type:float64`,
			in: []ConsumedElement{
				NewConsumedToken(&lexer.Token{
					Type: lexer.ItemBinding,
					Text: "?foo",
				}),
				NewConsumedToken(&lexer.Token{
					Type: lexer.ItemEQ,
				}),
				NewConsumedToken(&lexer.Token{
					Type: lexer.ItemLiteral,
					Text: `"99.0"^^type:float64`,
				}),
			},
			r: table.Row{
				"?foo": &table.Cell{L: buildLiteralOrDie(`"99.0"^^type:float64`)},
			},
			err:  false,
			want: true,
		},
		{
			id: `?foo > "10"^^type:int64`,
			in: []ConsumedElement{
				NewConsumedToken(&lexer.Token{
					Type: lexer.ItemBinding,
					Text: "?foo",
				}),
				NewConsumedToken(&lexer.Token{
					Type: lexer.ItemGT,
				}),
				NewConsumedToken(&lexer.Token{
					Type: lexer.ItemLiteral,
					Text: `"10"^^type:int64`,
				}),
			},
			r: table.Row{
				"?foo": &table.Cell{L: buildLiteralOrDie(`"100"^^type:int64`)},
			},
			err:  false,
			want: true,
		},
		{
			id: `?foo < "10"^^type:int64`,
			in: []ConsumedElement{
				NewConsumedToken(&lexer.Token{
					Type: lexer.ItemBinding,
					Text: "?foo",
				}),
				NewConsumedToken(&lexer.Token{
					Type: lexer.ItemLT,
				}),
				NewConsumedToken(&lexer.Token{
					Type: lexer.ItemLiteral,
					Text: `"10"^^type:int64`,
				}),
			},
			r: table.Row{
				"?foo": &table.Cell{L: buildLiteralOrDie(`"100"^^type:int64`)},
			},
			err:  false,
			want: false,
		},
		{
			id: "?foo = /_<meowth>",
			in: []ConsumedElement{
				NewConsumedToken(&lexer.Token{
					Type: lexer.ItemBinding,
					Text: "?foo",
				}),
				NewConsumedToken(&lexer.Token{
					Type: lexer.ItemEQ,
				}),
				NewConsumedToken(&lexer.Token{
					Type: lexer.ItemNode,
					Text: "/_<meowth>",
				}),
			},
			r: table.Row{
				"?foo": &table.Cell{N: newNodeFromStringOrDie("/_", "meowth")},
			},
			err:  false,
			want: true,
		},
	}
	for _, entry := range testTable {
		eval, err := NewEvaluator(entry.in)
		if !entry.err && err != nil {
			t.Fatalf("test %q should have never failed to process %v with error %v", entry.id, entry.in, err)
		}
		got, err := eval.Evaluate(entry.r)
		if err != nil {
			t.Errorf("test %q the created evaluator failed to evaluate row %v with error %v", entry.id, entry.r, err)
		}
		if want := entry.want; got != want {
			t.Errorf("test %q failed to evaluate the proper value; got %v, want %v", entry.id, got, want)
		}
	}
}
