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

package triple

import (
	"testing"

	"github.com/google/badwolf/triple/literal"
	"github.com/google/badwolf/triple/node"
	"github.com/google/badwolf/triple/predicate"
)

func getTestData(t *testing.T) (*node.Node, *predicate.Predicate, *Object) {
	s, err := node.Parse("/some/type<some id>")
	if err != nil {
		t.Fatalf("Failed to create test node")
	}
	p, err := predicate.Parse("\"foo\"@[]")
	if err != nil {
		t.Fatalf("Failed to create test predicate")
	}
	o := NewNodeObject(s)
	return s, p, o
}

func TestEmptyTripleFail(t *testing.T) {
	s, p, o := getTestData(t)
	table := []struct {
		s *node.Node
		p *predicate.Predicate
		o *Object
	}{
		{nil, nil, nil},
		{s, nil, nil},
		{nil, p, nil},
		{nil, nil, o},
		{s, p, nil},
		{s, nil, o},
		{nil, p, o},
	}
	for _, tc := range table {
		if tr, err := New(tc.s, tc.p, tc.o); err == nil {
			t.Errorf("triple.New should have never created a partial triple as %s", tr)
		}
	}
}

func TestPrettyTriple(t *testing.T) {
	s, p, o := getTestData(t)
	tr, err := New(s, p, o)
	if err != nil {
		t.Fatalf("triple.New shoulds not fail to create triple wih error %v", err)
	}
	if got, want := tr.String(), "/some/type<some id>\t\"foo\"@[]\t/some/type<some id>"; got != want {
		t.Errorf("triple.String failed to return a valid prety printed string; got %s, want %s", got, err)
	}
}

func TestParsetriple(t *testing.T) {
	ss := []string{
		"/some/type<some id>\t\"foo\"@[]\t/some/type<some id>",
		"/some/type<some id>\t\"foo\"@[]\t\"bar\"@[]",
	}
	for _, s := range ss {
		if _, err := ParseTriple(s, literal.DefaultBuilder()); err != nil {
			t.Errorf("triple.Parse failed to parse valid triple %s with error %v", s, err)
		}
	}
}

func TestReify(t *testing.T) {
	tr, err := ParseTriple("/some/type<some id>\t\"foo\"@[]\t\"bar\"@[]", literal.DefaultBuilder())
	if err != nil {
		t.Fatalf("triple.ParseTriple failed to parse valid triple with error %v", err)
	}
	rts, bn, err := tr.Reify()
	if err != nil {
		t.Errorf("triple.Reify failed to reify %v with error %v", tr, err)
	}
	if len(rts) != 4 || bn == nil {
		t.Errorf("triple.Reify failed to create 4 valid triples and a valid blank node; returned %v, %s instead", rts, bn)
	}
}
