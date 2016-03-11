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

// Package tree contains the data generator to build the tree benchmark data.
package tree

import (
	"reflect"
	"testing"

	"github.com/google/badwolf/tools/benchmark/generator"
)

func TesEmpty(t *testing.T) {
}

func TestNewNode(t *testing.T) {
	tg, err := New(2)
	if err != nil {
		t.Fatal(err)
	}
	n, err := tg.(*treeGenerator).newNode(1, "0")
	if err != nil {
		t.Error(err)
	}
	if got, want := n.String(), "/tn<1/0>"; got != want {
		t.Errorf("treeGenerator.newNode(0, 0) returned wrong node; got %q, want %q", got, want)
	}
}

func TestGenerate(t *testing.T) {
	tg2, err := New(2)
	if err != nil {
		t.Fatal(err)
	}
	tg5, err := New(5)
	if err != nil {
		t.Fatal(err)
	}
	testData := []struct {
		g    generator.Generator
		n    int
		want []string
	}{
		{
			g:    tg2,
			n:    0,
			want: nil,
		},
		{
			g: tg2,
			n: 1,
			want: []string{
				"/tn<0>\t\"parent_of\"@[]\t/tn<0/0>",
			},
		},
		{
			g: tg2,
			n: 2,
			want: []string{
				"/tn<0>\t\"parent_of\"@[]\t/tn<0/0>",
				"/tn<0>\t\"parent_of\"@[]\t/tn<1/0>",
			},
		},
		{
			g: tg2,
			n: 3,
			want: []string{
				"/tn<0>\t\"parent_of\"@[]\t/tn<0/0>",
				"/tn<0/0>\t\"parent_of\"@[]\t/tn<0/0/0>",
				"/tn<0/0>\t\"parent_of\"@[]\t/tn<1/0/0>",
			},
		},
		{
			g: tg2,
			n: 4,
			want: []string{
				"/tn<0>\t\"parent_of\"@[]\t/tn<0/0>",
				"/tn<0/0>\t\"parent_of\"@[]\t/tn<0/0/0>",
				"/tn<0/0/0>\t\"parent_of\"@[]\t/tn<0/0/0/0>",
				"/tn<0/0/0>\t\"parent_of\"@[]\t/tn<1/0/0/0>",
			},
		},
		{
			g: tg2,
			n: 5,
			want: []string{
				"/tn<0>\t\"parent_of\"@[]\t/tn<0/0>",
				"/tn<0/0>\t\"parent_of\"@[]\t/tn<0/0/0>",
				"/tn<0/0/0>\t\"parent_of\"@[]\t/tn<0/0/0/0>",
				"/tn<0/0/0>\t\"parent_of\"@[]\t/tn<1/0/0/0>",
				"/tn<0/0>\t\"parent_of\"@[]\t/tn<1/0/0>",
			},
		},
		{
			g: tg2,
			n: 6,
			want: []string{
				"/tn<0>\t\"parent_of\"@[]\t/tn<0/0>",
				"/tn<0/0>\t\"parent_of\"@[]\t/tn<0/0/0>",
				"/tn<0/0/0>\t\"parent_of\"@[]\t/tn<0/0/0/0>",
				"/tn<0/0/0>\t\"parent_of\"@[]\t/tn<1/0/0/0>",
				"/tn<0/0>\t\"parent_of\"@[]\t/tn<1/0/0>",
				"/tn<1/0/0>\t\"parent_of\"@[]\t/tn<0/1/0/0>",
			},
		},
		{
			g: tg2,
			n: 7,
			want: []string{
				"/tn<0>\t\"parent_of\"@[]\t/tn<0/0>",
				"/tn<0/0>\t\"parent_of\"@[]\t/tn<0/0/0>",
				"/tn<0/0/0>\t\"parent_of\"@[]\t/tn<0/0/0/0>",
				"/tn<0/0/0>\t\"parent_of\"@[]\t/tn<1/0/0/0>",
				"/tn<0/0>\t\"parent_of\"@[]\t/tn<1/0/0>",
				"/tn<1/0/0>\t\"parent_of\"@[]\t/tn<0/1/0/0>",
				"/tn<1/0/0>\t\"parent_of\"@[]\t/tn<1/1/0/0>",
			},
		},
		{
			g: tg2,
			n: 8,
			want: []string{
				"/tn<0>\t\"parent_of\"@[]\t/tn<0/0>",
				"/tn<0/0>\t\"parent_of\"@[]\t/tn<0/0/0>",
				"/tn<0/0/0>\t\"parent_of\"@[]\t/tn<0/0/0/0>",
				"/tn<0/0/0/0>\t\"parent_of\"@[]\t/tn<0/0/0/0/0>",
				"/tn<0/0/0/0>\t\"parent_of\"@[]\t/tn<1/0/0/0/0>",
				"/tn<0/0/0>\t\"parent_of\"@[]\t/tn<1/0/0/0>",
				"/tn<1/0/0/0>\t\"parent_of\"@[]\t/tn<0/1/0/0/0>",
				"/tn<1/0/0/0>\t\"parent_of\"@[]\t/tn<1/1/0/0/0>",
			},
		},
		{
			g:    tg5,
			n:    0,
			want: nil,
		},
		{
			g: tg5,
			n: 1,
			want: []string{
				"/tn<0>\t\"parent_of\"@[]\t/tn<0/0>",
			},
		},
		{
			g: tg5,
			n: 2,
			want: []string{
				"/tn<0>\t\"parent_of\"@[]\t/tn<0/0>",
				"/tn<0>\t\"parent_of\"@[]\t/tn<1/0>",
			},
		},
		{
			g: tg5,
			n: 3,
			want: []string{
				"/tn<0>\t\"parent_of\"@[]\t/tn<0/0>",
				"/tn<0>\t\"parent_of\"@[]\t/tn<1/0>",
				"/tn<0>\t\"parent_of\"@[]\t/tn<2/0>",
			},
		},
		{
			g: tg5,
			n: 4,
			want: []string{
				"/tn<0>\t\"parent_of\"@[]\t/tn<0/0>",
				"/tn<0>\t\"parent_of\"@[]\t/tn<1/0>",
				"/tn<0>\t\"parent_of\"@[]\t/tn<2/0>",
				"/tn<0>\t\"parent_of\"@[]\t/tn<3/0>",
			},
		},
		{
			g: tg5,
			n: 5,
			want: []string{
				"/tn<0>\t\"parent_of\"@[]\t/tn<0/0>",
				"/tn<0>\t\"parent_of\"@[]\t/tn<1/0>",
				"/tn<0>\t\"parent_of\"@[]\t/tn<2/0>",
				"/tn<0>\t\"parent_of\"@[]\t/tn<3/0>",
				"/tn<0>\t\"parent_of\"@[]\t/tn<4/0>",
			},
		},
		{
			g: tg5,
			n: 6,
			want: []string{
				"/tn<0>\t\"parent_of\"@[]\t/tn<0/0>",
				"/tn<0/0>\t\"parent_of\"@[]\t/tn<0/0/0>",
				"/tn<0/0>\t\"parent_of\"@[]\t/tn<1/0/0>",
				"/tn<0/0>\t\"parent_of\"@[]\t/tn<2/0/0>",
				"/tn<0/0>\t\"parent_of\"@[]\t/tn<3/0/0>",
				"/tn<0/0>\t\"parent_of\"@[]\t/tn<4/0/0>",
			},
		},
		{
			g: tg5,
			n: 7,
			want: []string{
				"/tn<0>\t\"parent_of\"@[]\t/tn<0/0>",
				"/tn<0/0>\t\"parent_of\"@[]\t/tn<0/0/0>",
				"/tn<0/0>\t\"parent_of\"@[]\t/tn<1/0/0>",
				"/tn<0/0>\t\"parent_of\"@[]\t/tn<2/0/0>",
				"/tn<0/0>\t\"parent_of\"@[]\t/tn<3/0/0>",
				"/tn<0/0>\t\"parent_of\"@[]\t/tn<4/0/0>",
				"/tn<0>\t\"parent_of\"@[]\t/tn<1/0>",
			},
		},
		{
			g: tg5,
			n: 8,
			want: []string{
				"/tn<0>\t\"parent_of\"@[]\t/tn<0/0>",
				"/tn<0/0>\t\"parent_of\"@[]\t/tn<0/0/0>",
				"/tn<0/0>\t\"parent_of\"@[]\t/tn<1/0/0>",
				"/tn<0/0>\t\"parent_of\"@[]\t/tn<2/0/0>",
				"/tn<0/0>\t\"parent_of\"@[]\t/tn<3/0/0>",
				"/tn<0/0>\t\"parent_of\"@[]\t/tn<4/0/0>",
				"/tn<0>\t\"parent_of\"@[]\t/tn<1/0>",
				"/tn<1/0>\t\"parent_of\"@[]\t/tn<0/1/0>",
			},
		},
		{
			g: tg5,
			n: 9,
			want: []string{
				"/tn<0>\t\"parent_of\"@[]\t/tn<0/0>",
				"/tn<0/0>\t\"parent_of\"@[]\t/tn<0/0/0>",
				"/tn<0/0>\t\"parent_of\"@[]\t/tn<1/0/0>",
				"/tn<0/0>\t\"parent_of\"@[]\t/tn<2/0/0>",
				"/tn<0/0>\t\"parent_of\"@[]\t/tn<3/0/0>",
				"/tn<0/0>\t\"parent_of\"@[]\t/tn<4/0/0>",
				"/tn<0>\t\"parent_of\"@[]\t/tn<1/0>",
				"/tn<1/0>\t\"parent_of\"@[]\t/tn<0/1/0>",
				"/tn<1/0>\t\"parent_of\"@[]\t/tn<1/1/0>",
			},
		},
		{
			g: tg5,
			n: 10,
			want: []string{
				"/tn<0>\t\"parent_of\"@[]\t/tn<0/0>",
				"/tn<0/0>\t\"parent_of\"@[]\t/tn<0/0/0>",
				"/tn<0/0>\t\"parent_of\"@[]\t/tn<1/0/0>",
				"/tn<0/0>\t\"parent_of\"@[]\t/tn<2/0/0>",
				"/tn<0/0>\t\"parent_of\"@[]\t/tn<3/0/0>",
				"/tn<0/0>\t\"parent_of\"@[]\t/tn<4/0/0>",
				"/tn<0>\t\"parent_of\"@[]\t/tn<1/0>",
				"/tn<1/0>\t\"parent_of\"@[]\t/tn<0/1/0>",
				"/tn<1/0>\t\"parent_of\"@[]\t/tn<1/1/0>",
				"/tn<1/0>\t\"parent_of\"@[]\t/tn<2/1/0>",
			},
		},
		{
			g: tg5,
			n: 11,
			want: []string{
				"/tn<0>\t\"parent_of\"@[]\t/tn<0/0>",
				"/tn<0/0>\t\"parent_of\"@[]\t/tn<0/0/0>",
				"/tn<0/0>\t\"parent_of\"@[]\t/tn<1/0/0>",
				"/tn<0/0>\t\"parent_of\"@[]\t/tn<2/0/0>",
				"/tn<0/0>\t\"parent_of\"@[]\t/tn<3/0/0>",
				"/tn<0/0>\t\"parent_of\"@[]\t/tn<4/0/0>",
				"/tn<0>\t\"parent_of\"@[]\t/tn<1/0>",
				"/tn<1/0>\t\"parent_of\"@[]\t/tn<0/1/0>",
				"/tn<1/0>\t\"parent_of\"@[]\t/tn<1/1/0>",
				"/tn<1/0>\t\"parent_of\"@[]\t/tn<2/1/0>",
				"/tn<1/0>\t\"parent_of\"@[]\t/tn<3/1/0>",
			},
		},
		{
			g: tg5,
			n: 12,
			want: []string{
				"/tn<0>\t\"parent_of\"@[]\t/tn<0/0>",
				"/tn<0/0>\t\"parent_of\"@[]\t/tn<0/0/0>",
				"/tn<0/0>\t\"parent_of\"@[]\t/tn<1/0/0>",
				"/tn<0/0>\t\"parent_of\"@[]\t/tn<2/0/0>",
				"/tn<0/0>\t\"parent_of\"@[]\t/tn<3/0/0>",
				"/tn<0/0>\t\"parent_of\"@[]\t/tn<4/0/0>",
				"/tn<0>\t\"parent_of\"@[]\t/tn<1/0>",
				"/tn<1/0>\t\"parent_of\"@[]\t/tn<0/1/0>",
				"/tn<1/0>\t\"parent_of\"@[]\t/tn<1/1/0>",
				"/tn<1/0>\t\"parent_of\"@[]\t/tn<2/1/0>",
				"/tn<1/0>\t\"parent_of\"@[]\t/tn<3/1/0>",
				"/tn<1/0>\t\"parent_of\"@[]\t/tn<4/1/0>",
			},
		},
		{
			g: tg5,
			n: 13,
			want: []string{
				"/tn<0>\t\"parent_of\"@[]\t/tn<0/0>",
				"/tn<0/0>\t\"parent_of\"@[]\t/tn<0/0/0>",
				"/tn<0/0>\t\"parent_of\"@[]\t/tn<1/0/0>",
				"/tn<0/0>\t\"parent_of\"@[]\t/tn<2/0/0>",
				"/tn<0/0>\t\"parent_of\"@[]\t/tn<3/0/0>",
				"/tn<0/0>\t\"parent_of\"@[]\t/tn<4/0/0>",
				"/tn<0>\t\"parent_of\"@[]\t/tn<1/0>",
				"/tn<1/0>\t\"parent_of\"@[]\t/tn<0/1/0>",
				"/tn<1/0>\t\"parent_of\"@[]\t/tn<1/1/0>",
				"/tn<1/0>\t\"parent_of\"@[]\t/tn<2/1/0>",
				"/tn<1/0>\t\"parent_of\"@[]\t/tn<3/1/0>",
				"/tn<1/0>\t\"parent_of\"@[]\t/tn<4/1/0>",
				"/tn<0>\t\"parent_of\"@[]\t/tn<2/0>",
			},
		},
		{
			g: tg5,
			n: 14,
			want: []string{
				"/tn<0>\t\"parent_of\"@[]\t/tn<0/0>",
				"/tn<0/0>\t\"parent_of\"@[]\t/tn<0/0/0>",
				"/tn<0/0>\t\"parent_of\"@[]\t/tn<1/0/0>",
				"/tn<0/0>\t\"parent_of\"@[]\t/tn<2/0/0>",
				"/tn<0/0>\t\"parent_of\"@[]\t/tn<3/0/0>",
				"/tn<0/0>\t\"parent_of\"@[]\t/tn<4/0/0>",
				"/tn<0>\t\"parent_of\"@[]\t/tn<1/0>",
				"/tn<1/0>\t\"parent_of\"@[]\t/tn<0/1/0>",
				"/tn<1/0>\t\"parent_of\"@[]\t/tn<1/1/0>",
				"/tn<1/0>\t\"parent_of\"@[]\t/tn<2/1/0>",
				"/tn<1/0>\t\"parent_of\"@[]\t/tn<3/1/0>",
				"/tn<1/0>\t\"parent_of\"@[]\t/tn<4/1/0>",
				"/tn<0>\t\"parent_of\"@[]\t/tn<2/0>",
				"/tn<2/0>\t\"parent_of\"@[]\t/tn<0/2/0>",
			},
		},
	}
	for _, entry := range testData {
		trpls, err := entry.g.Generate(entry.n)
		if err != nil {
			t.Fatal(err)
		}
		var res []string
		for _, trpl := range trpls {
			res = append(res, trpl.String())
		}
		if got, want := len(res), entry.n; got != want {
			t.Errorf("treeGenrator.Generate(%d) failed to produce the expected number of triples; got %d, want %d", entry.n, got, want)
		}
		if got, want := res, entry.want; !reflect.DeepEqual(got, want) {
			t.Errorf("treeGenrator.Generate(%d) failed to produce the expected triples; got %v, want %v", entry.n, got, want)
		}
	}
}
