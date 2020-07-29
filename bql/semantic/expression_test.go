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
			t.Errorf("failed to evaluate op %q for %v on row %v with error: %v", entry.eval.(*evaluationNode).op.String(), entry.eval, entry.r, err)
		}
		if want := entry.want; got != want {
			t.Errorf("failed to evaluate op %q for %v on row %v; got %v, want %v", entry.eval.(*evaluationNode).op.String(), entry.eval, entry.r, got, want)
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
			t.Errorf("failed to evaluate op %q for %v with error: %v", entry.eval.(*booleanNode).op.String(), entry.eval, err)
		}
		if want := entry.want; got != want {
			t.Errorf("failed to evaluate op %q for %v; got %v, want %v", entry.eval.(*booleanNode).op.String(), entry.eval, got, want)
		}
	}
}

func mustBuildLiteral(textLiteral string) *literal.Literal {
	lit, err := literal.DefaultBuilder().Parse(textLiteral)
	if err != nil {
		panic(fmt.Sprintf("could not parse text literal %q, got error: %v", textLiteral, err))
	}
	return lit
}

func mustBuildNodeFromStrings(nodeType, nodeID string) *node.Node {
	n, err := node.NewNodeFromStrings(nodeType, nodeID)
	if err != nil {
		panic(fmt.Sprintf("could not build node from type %q and ID %q, got error: %v", nodeType, nodeID, err))
	}
	return n
}

func TestNewEvaluator(t *testing.T) {
	testTable := []struct {
		id   string
		in   []ConsumedElement
		r    table.Row
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
					L: mustBuildLiteral(`"abc"^^type:text`),
				},
			},
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
			want: false,
		},
		{
			id: `?s TYPE ?s_type = "/u"^^type:text`,
			in: []ConsumedElement{
				NewConsumedToken(&lexer.Token{
					Type: lexer.ItemBinding,
					Text: "?s_type",
				}),
				NewConsumedToken(&lexer.Token{
					Type: lexer.ItemEQ,
				}),
				NewConsumedToken(&lexer.Token{
					Type: lexer.ItemLiteral,
					Text: `"/u"^^type:text`,
				}),
			},
			r: table.Row{
				"?s_type": &table.Cell{S: table.CellString("/u")},
			},
			want: true,
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
				"?foo": &table.Cell{L: mustBuildLiteral(`"99.0"^^type:float64`)},
			},
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
				"?foo": &table.Cell{L: mustBuildLiteral(`"100"^^type:int64`)},
			},
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
				"?foo": &table.Cell{L: mustBuildLiteral(`"100"^^type:int64`)},
			},
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
				"?foo": &table.Cell{N: mustBuildNodeFromStrings("/_", "meowth")},
			},
			want: true,
		},
		{
			id: `?o > "37"^^type:int64`,
			in: []ConsumedElement{
				NewConsumedToken(&lexer.Token{
					Type: lexer.ItemBinding,
					Text: "?o",
				}),
				NewConsumedToken(&lexer.Token{
					Type: lexer.ItemGT,
				}),
				NewConsumedToken(&lexer.Token{
					Type: lexer.ItemLiteral,
					Text: `"37"^^type:int64`,
				}),
			},
			r: table.Row{
				"?o": &table.Cell{N: mustBuildNodeFromStrings("/u", "paul")},
			},
			want: false,
		},
		{
			id: `?o = /u<peter>`,
			in: []ConsumedElement{
				NewConsumedToken(&lexer.Token{
					Type: lexer.ItemBinding,
					Text: "?o",
				}),
				NewConsumedToken(&lexer.Token{
					Type: lexer.ItemEQ,
				}),
				NewConsumedToken(&lexer.Token{
					Type: lexer.ItemNode,
					Text: "/u<peter>",
				}),
			},
			r: table.Row{
				"?o": &table.Cell{L: mustBuildLiteral(`"73"^^type:int64`)},
			},
			want: false,
		},
		{
			id: `?foo = "10"^^type:text`,
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
					Text: `"10"^^type:text`,
				}),
			},
			r: table.Row{
				"?foo": &table.Cell{L: mustBuildLiteral(`"10"^^type:int64`)},
			},
			want: false,
		},
	}
	for _, entry := range testTable {
		eval, err := NewEvaluator(entry.in)
		if err != nil {
			t.Fatalf("test %q should have never failed when creating a new evaluator from %v, got error: %v", entry.id, entry.in, err)
		}

		got, err := eval.Evaluate(entry.r)
		if err != nil {
			t.Errorf("%s: eval.Evaluate(%v) = _, %v; want nil error", entry.id, entry.r, err)
		}
		if want := entry.want; got != want {
			t.Errorf("%s: eval.Evaluate(%v) = %v, _; want %v, _", entry.id, entry.r, got, want)
		}
	}
}

func TestNewEvaluatorError(t *testing.T) {
	testTable := []struct {
		id   string
		in   []ConsumedElement
		r    table.Row
		want bool
	}{
		{
			id: `?s ID ?id > "37"^^type:int64`,
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
					Text: `"37"^^type:int64`,
				}),
			},
			r: table.Row{
				"?id": &table.Cell{S: table.CellString("peter")},
			},
			want: false,
		},
		{
			id: `?s ID ?id = /u<peter>`,
			in: []ConsumedElement{
				NewConsumedToken(&lexer.Token{
					Type: lexer.ItemBinding,
					Text: "?id",
				}),
				NewConsumedToken(&lexer.Token{
					Type: lexer.ItemEQ,
				}),
				NewConsumedToken(&lexer.Token{
					Type: lexer.ItemNode,
					Text: "/u<peter>",
				}),
			},
			r: table.Row{
				"?id": &table.Cell{S: table.CellString("peter")},
			},
			want: false,
		},
	}

	for _, entry := range testTable {
		eval, err := NewEvaluator(entry.in)
		if err != nil {
			t.Fatalf("test %q should have never failed when creating a new evaluator from %v, got error: %v", entry.id, entry.in, err)
		}

		got, err := eval.Evaluate(entry.r)
		if err == nil {
			t.Errorf("%s: eval.Evaluate(%v) = _, nil; want non-nil error", entry.id, entry.r)
		}
		if want := entry.want; got != want {
			t.Errorf("%s: eval.Evaluate(%v) = %v, _; want %v, _", entry.id, entry.r, got, want)
		}
	}
}
