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
		"select ?a from ?b where{?s ?p ?o};",
		"select ?a, ?b from ?c where{?s ?p ?o};",
		"select ?a, ?b, ?c from ?d where{?s ?p ?o};",
		// Test aliases and functions.
		"select ?a as ?b from ?c where{?s ?p ?o};",
		"select ?a as ?b, ?c as ?d from ?e where{?s ?p ?o};",
		"select count(?a) as ?b, sum(?c) as ?d, ?e as ?f from ?g where{?s ?p ?o};",
		"select count(distinct ?a) as ?b from ?c where{?s ?p ?o};",
		// Test multiple graphs are accepted.
		"select ?a from ?b where{?s ?p ?o};",
		"select ?a from ?b, ?c where{?s ?p ?o};",
		"select ?a from ?b, ?c, ?d where{?s ?p ?o};",
		// Test non empty clause.
		"select ?a from ?b where{?s ?p ?o};",
		"select ?a from ?b where{?s as ?x ?p ?o};",
		"select ?a from ?b where{?s as ?x type ?y ?p ?o};",
		"select ?a from ?b where{?s as ?x type ?y id ?z ?p ?o};",
		"select ?a from ?b where{?s ?p as ?x ?o};",
		"select ?a from ?b where{?s ?p as ?x id ?y ?o};",
		"select ?a from ?b where{?s ?p as ?x id ?y at ?z ?o};",
		"select ?a from ?b where{?s ?p ?o as ?x};",
		"select ?a from ?b where{?s ?p ?o as ?x type ?y};",
		"select ?a from ?b where{?s ?p ?o as ?x type ?y id ?z};",
		"select ?a from ?b where{?s ?p ?o as ?x type ?y id ?z at ?t};",
		// Test multiple clauses.
		"select ?a from ?b where{?s ?p ?o};",
		"select ?a from ?b where{?s ?p ?o . ?s ?p ?o};",
		"select ?a from ?b where{?s ?p ?o . ?s ?p ?o . ?s ?p ?o};",
		// Test group by.
		"select ?a from ?b where{?s ?p ?o} group by ?a;",
		"select ?a from ?b where{?s ?p ?o} group by ?a, ?b;",
		"select ?a from ?b where{?s ?p ?o} group by ?a, ?b, ?c;",
		// Test order by.
		"select ?a from ?b where{?s ?p ?o} order by ?a;",
		"select ?a from ?b where{?s ?p ?o} order by ?a asc;",
		"select ?a from ?b where{?s ?p ?o} order by ?a desc;",
		"select ?a from ?b where{?s ?p ?o} order by ?a asc, ?b desc;",
		"select ?a from ?b where{?s ?p ?o} order by ?a desc, ?b desc, ?c asc;",
		// Test global time bounds.
		"select ?a from ?b where {?s ?p ?o} before \"foo\"@[\"123\"];",
		"select ?a from ?b where {?s ?p ?o} after \"foo\"@[\"123\"];",
		"select ?a from ?b where {?s ?p ?o} between \"foo\"@[\"123\"], \"bar\"@[\"123\"];",
		"select ?a from ?b where {?s ?p ?o} (before \"foo\"@[\"123\"]);",
		"select ?a from ?b where {?s ?p ?o} before \"foo\"@[\"123\"] and before \"foo\"@[\"123\"];",
		"select ?a from ?b where {?s ?p ?o} before \"foo\"@[\"123\"] or before \"foo\"@[\"123\"];",
		"select ?a from ?b where {?s ?p ?o} before \"foo\"@[\"123\"] or (before \"foo\"@[\"123\"] and before \"foo\"@[\"123\"]);",
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
		// Reject empty where clause.
		"select ?a from ?b where{};",
		// Reject incomplete empty where clause.
		"select ?a from ?b where {;",
		"select ?a from ?b where };",
		// Reject incomplete clauses.
		"select ?a from ?b where {?s ?p};",
		"select ?a from ?b where {?s ?p ?o . ?};",
		// Reject imcomplete clause aliasing.
		"select ?a from ?b where {?s id ?b as ?c ?d ?o};",
		"select ?a from ?b where {?s ?p at ?t as ?a ?o};",
		"select ?a from ?b where {?s ?p ?o at ?t id ?i};",
		// Reject incomplete group by.
		"select ?a from ?b where{?s ?p ?o} group by;",
		"select ?a from ?b where{?s ?p ?o} group ?a;",
		"select ?a from ?b where{?s ?p ?o} by ?a;",
		// Reject incomplete order by.
		"select ?a from ?b where{?s ?p ?o} order by;",
		"select ?a from ?b where{?s ?p ?o} order ?a;",
		"select ?a from ?b where{?s ?p ?o} by ?a;",
		"select ?a from ?b where{?s ?p ?o} order by ?a, a;",
		"select ?a from ?b where{?s ?p ?o} order by ?a, ?b, desc;",
		// Reject invalid global time bounds.
		"select ?a from ?b where {?s ?p ?o} before ;",
		"select ?a from ?b where {?s ?p ?o} after ;",
		"select ?a from ?b where {?s ?p ?o} between \"foo\"@[\"123\"], ;",
		"select ?a from ?b where {?s ?p ?o} before \"foo\"@[\"123\"]);",
		"select ?a from ?b where {?s ?p ?o} before \"foo\"@[\"123\"]  before \"foo\"@[\"123\"];",
		"select ?a from ?b where {?s ?p ?o} before \"foo\"@[\"123\"] or before \"foo\"@[\"123\"] ,;",
		"select ?a from ?b where {?s ?p ?o} before \"foo\"@[\"123\"] or before \"foo\"@[\"123\"] and before \"foo\"@[\"123\"]);",
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
