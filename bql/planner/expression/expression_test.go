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

package expression

import (
	"testing"

	"github.com/google/badwolf/bql/table"
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
				"?foo": &table.Cell{S: "foo"},
				"?bar": &table.Cell{S: "foo"},
			},
			want: false,
			err:  true,
		},
		{
			eval: &evaluationNode{EQ, "?foo", "?bar"},
			r: table.Row{
				"?foo": &table.Cell{S: "foo"},
				"?bar": &table.Cell{S: "bar"},
			},
			want: false,
			err:  false,
		},
		{
			eval: &evaluationNode{EQ, "", "?bar"},
			r: table.Row{
				"?foo": &table.Cell{S: "foo"},
				"?bar": &table.Cell{S: "bar"},
			},
			want: false,
			err:  true,
		},
		{
			eval: &evaluationNode{EQ, "?foo", ""},
			r: table.Row{
				"?foo": &table.Cell{S: "foo"},
				"?bar": &table.Cell{S: "bar"},
			},
			want: false,
			err:  true,
		},
		{
			eval: &evaluationNode{EQ, "?foo", "?bar"},
			r: table.Row{
				"?foo": &table.Cell{S: "foo"},
				"?bar": &table.Cell{S: "foo"},
			},
			want: true,
			err:  false,
		},
		{
			eval: &evaluationNode{LT, "?foo", "?bar"},
			r: table.Row{
				"?foo": &table.Cell{S: "foo"},
				"?bar": &table.Cell{S: "bar"},
			},
			want: false,
			err:  false,
		},
		{
			eval: &evaluationNode{GT, "?foo", "?bar"},
			r: table.Row{
				"?foo": &table.Cell{S: "foo"},
				"?bar": &table.Cell{S: "bar"},
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

type alwaysReturn struct {
	v bool
}

func (a *alwaysReturn) Evaluate(r table.Row) (bool, error) {
	return a.v, nil
}

func TestBooleanEvaluationNode(t *testing.T) {
	testTable := []struct {
		eval Evaluator
		want bool
		err  bool
	}{
		{
			eval: &booleanNode{op: NOT, lS: true, lE: &alwaysReturn{true}},
			want: false,
			err:  false,
		},
		{
			eval: &booleanNode{op: NOT, lS: true, lE: &alwaysReturn{false}},
			want: true,
			err:  false,
		},
		{
			eval: &booleanNode{op: NOT, lS: false, lE: &alwaysReturn{false}},
			want: false,
			err:  true,
		},
		{
			eval: &booleanNode{op: OR, lS: true, lE: &alwaysReturn{false}, rS: true, rE: &alwaysReturn{false}},
			want: false,
			err:  false,
		},
		{
			eval: &booleanNode{op: OR, lS: true, lE: &alwaysReturn{false}, rS: true, rE: &alwaysReturn{true}},
			want: true,
			err:  false,
		},
		{
			eval: &booleanNode{op: OR, lS: true, lE: &alwaysReturn{true}, rS: true, rE: &alwaysReturn{false}},
			want: true,
			err:  false,
		},
		{
			eval: &booleanNode{op: OR, lS: true, lE: &alwaysReturn{true}, rS: true, rE: &alwaysReturn{true}},
			want: true,
			err:  false,
		},
		{
			eval: &booleanNode{op: AND, lS: true, lE: &alwaysReturn{false}, rS: true, rE: &alwaysReturn{false}},
			want: false,
			err:  false,
		},
		{
			eval: &booleanNode{op: AND, lS: true, lE: &alwaysReturn{false}, rS: true, rE: &alwaysReturn{true}},
			want: false,
			err:  false,
		},
		{
			eval: &booleanNode{op: AND, lS: true, lE: &alwaysReturn{true}, rS: true, rE: &alwaysReturn{false}},
			want: false,
			err:  false,
		},
		{
			eval: &booleanNode{op: AND, lS: true, lE: &alwaysReturn{true}, rS: true, rE: &alwaysReturn{true}},
			want: true,
			err:  false,
		},
		{
			eval: &booleanNode{op: AND, lS: false, lE: &alwaysReturn{true}, rS: true, rE: &alwaysReturn{true}},
			want: false,
			err:  true,
		},
		{
			eval: &booleanNode{op: AND, lS: true, lE: &alwaysReturn{true}, rS: false, rE: &alwaysReturn{true}},
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
