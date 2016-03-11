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

// Package graph contains the data generator to build arbitrary graph
// benchmark data.
package graph

import (
	"reflect"
	"sort"
	"testing"
)

func TestInvalidTripleGraphGenerator(t *testing.T) {
	if _, err := NewRandomGraph(0); err == nil {
		t.Fatalf("graph.NewRandomGraph(0) should have never succeeded.")
	}
	g, err := NewRandomGraph(2)
	if err != nil {
		t.Fatalf("graph.NewRandomGraph(2) should have never failed, %v.", err)
	}
	trpls, err := g.Generate(2)
	if err != nil {
		t.Fatalf("g.Generate(2) should have never failed, %v.", err)
	}
	if got, want := len(trpls), 2; got != want {
		t.Fatalf("g.Generate(2) returned the wrong number of triples; got %d, want %d.", got, want)
	}
	if _, err := g.Generate(5); err == nil {
		t.Fatalf("g.Generate(5) should have never succeeded.")
	}
	if trpl, err := g.Generate(0); err != nil || trpl != nil {
		t.Fatalf("g.Generate(0) should have never succeeded with %v, %v.", t, err)
	}
}

func TestGraphGenerator(t *testing.T) {
	testData := []struct {
		size int
		n    int
		want []string
	}{
		{
			size: 1,
			n:    0,
			want: nil,
		},
		{
			size: 1,
			n:    1,
			want: []string{
				"/gn<0>\t\"follow\"@[]\t/gn<0>",
			},
		},
		{
			size: 2,
			n:    4,
			want: []string{
				"/gn<0>\t\"follow\"@[]\t/gn<0>",
				"/gn<0>\t\"follow\"@[]\t/gn<1>",
				"/gn<1>\t\"follow\"@[]\t/gn<0>",
				"/gn<1>\t\"follow\"@[]\t/gn<1>",
			},
		},
	}
	for _, entry := range testData {
		g, err := NewRandomGraph(entry.size)
		if err != nil {
			t.Fatalf("graph.NewRandomGraph(%d) should have never failed, %v.", entry.size, err)
		}
		trpls, err := g.Generate(entry.n)
		if err != nil {
			t.Fatalf("graph.NewRandomGraph(%d) should have never failed, %v.", entry.size, err)
		}
		if got, want := len(trpls), len(entry.want); got != want {
			t.Fatalf("g.Generate(%d) returned the wrong number of triples; got %d, want %d.", entry.n, got, want)
		}
		var tts []string
		for _, t := range trpls {
			tts = append(tts, t.String())
		}
		sort.Strings(tts)
		if got, want := tts, entry.want; !reflect.DeepEqual(got, want) {
			t.Fatalf("g.Generate(%d) returned the wrong number of triples; got %v, want %v.", entry.n, got, want)
		}
	}
}
