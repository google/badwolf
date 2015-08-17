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

package grammar

import "testing"

func TestAcceptByParse(t *testing.T) {
	table := []string{
		// Test multiple var bindings are accepted.
		"select ?a from ?b;",
		"select ?a, ?b from ?c;",
		"select ?a, ?b, ?c from ?d;",
		// Test aliases and functions.
		"select ?a as ?b from ?c;",
		"select ?a as ?b, ?c as ?d from ?e;",
		"select count(?a) as ?b, sum(?c) as ?d, ?e as ?f from ?g;",
		"select count(distinct ?a) as ?b from ?c;",
		// Test multiple graphs are accepted.
		"select ?a from ?b;",
		"select ?a from ?b, ?c;",
		"select ?a from ?b, ?c, ?d;",
	}
	p, err := NewParser(&BQL)
	if err != nil {
		t.Errorf("grammar.NewParser: should have produced a valid BQL parser")
	}
	for _, input := range table {
		if err := p.Parse(NewLLk(input, 1)); err != nil {
			t.Errorf("Parser.consume: failed to accept input %q with error %v", input, err)
		}
	}
}

func TestRejectByParse(t *testing.T) {
	table := []string{
		// Reject missing comas on var bindings or missing bindings.
		"select ?a ?wrong from ?b;",
		"select ?a , from ?b;",
		"select ?a as from ?b;",
		"select ?a as ?b, from ?b;",
		"select count(?a as ?b, from ?b;",
		"select count(distinct) as ?a, from ?c;",
		// Reject missing comas on var bindings or missing graphs.
		"select ?a from ?b ?c;",
		"select ?a from ?b,;",
	}
	p, err := NewParser(&BQL)
	if err != nil {
		t.Errorf("grammar.NewParser: should have produced a valid BQL parser")
	}
	for _, input := range table {
		if err := p.Parse(NewLLk(input, 1)); err == nil {
			t.Errorf("Parser.consume: failed to reject input %q with error %v", input, err)
		}
	}
}
