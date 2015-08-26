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

package planner

import (
	"testing"

	"github.com/google/badwolf/bql/grammar"
	"github.com/google/badwolf/bql/semantic"
	"github.com/google/badwolf/storage/memory"
)

func insertTest(t *testing.T) {
	bql := `insert data into ?a {/_<foo> "bar"@[] /_<foo> .
                               /_<foo> "bar"@[] "bar"@[1975-01-01T00:01:01.999999999Z] .
                               /_<foo> "bar"@[] "yeah"^^type:text};`
	p, err := grammar.NewParser(&grammar.SemanticBQL)
	if err != nil {
		t.Errorf("grammar.NewParser: should have produced a valid BQL parser")
	}
	stm := &semantic.Statement{}
	if err := p.Parse(grammar.NewLLk(bql, 1), stm); err != nil {
		t.Errorf("Parser.consume: failed to accept BQL %q with error %v", bql, err)
	}
	pln, err := New(memory.DefaultStore, stm)
	if err != nil {
		t.Errorf("planner.New: should have not failed to create a plan using memory.DefaultStorage for statement %v", stm)
	}
	if err := pln.Excecute(); err != nil {
		t.Errorf("planner.Execute: failed to execute insert plan with error %v", err)
	}
	g, err := memory.DefaultStore.Graph("?a")
	if err != nil {
		t.Errorf("memory.DefaultStore.Graph(%q) should have not fail with error %v", "?a", err)
	}
	i := 0
	for _ = range g.Triples() {
		i++
	}
	if i != 3 {
		t.Errorf("g.Triples should have returned 3 triples, returned %d instead", i)
	}
}

func deleteTest(t *testing.T) {
	bql := `delete data from ?a {/_<foo> "bar"@[] /_<foo> .
                               /_<foo> "bar"@[] "bar"@[1975-01-01T00:01:01.999999999Z] .
                               /_<foo> "bar"@[] "yeah"^^type:text};`
	p, err := grammar.NewParser(&grammar.SemanticBQL)
	if err != nil {
		t.Errorf("grammar.NewParser: should have produced a valid BQL parser")
	}
	stm := &semantic.Statement{}
	if err := p.Parse(grammar.NewLLk(bql, 1), stm); err != nil {
		t.Errorf("Parser.consume: failed to accept BQL %q with error %v", bql, err)
	}
	pln, err := New(memory.DefaultStore, stm)
	if err != nil {
		t.Errorf("planner.New: should have not failed to create a plan using memory.DefaultStorage for statement %v", stm)
	}
	if err := pln.Excecute(); err != nil {
		t.Errorf("planner.Execute: failed to execute insert plan with error %v", err)
	}
	g, err := memory.DefaultStore.Graph("?a")
	if err != nil {
		t.Errorf("memory.DefaultStore.Graph(%q) should have not fail with error %v", "?a", err)
	}
	i := 0
	for _ = range g.Triples() {
		i++
	}
	if i != 0 {
		t.Errorf("g.Triples should have returned 3 triples, returned %d instead", i)
	}
}

func TestInsertDoesNotFail(t *testing.T) {
	if _, err := memory.DefaultStore.NewGraph("?a"); err != nil {
		t.Errorf("memory.DefaultStore.NewGraph(%q) should have not failed with error %v", "?a", err)
	}
	insertTest(t)
	if err := memory.DefaultStore.DeleteGraph("?a"); err != nil {
		t.Errorf("memory.DefaultStore.DeleteGraph(%q) should have not failed with error %v", "?a", err)
	}
}

func TestDeleteDoesNotFail(t *testing.T) {
	if _, err := memory.DefaultStore.NewGraph("?a"); err != nil {
		t.Errorf("memory.DefaultStore.NewGraph(%q) should have not failed with error %v", "?a", err)
	}
	deleteTest(t)
	if err := memory.DefaultStore.DeleteGraph("?a"); err != nil {
		t.Errorf("memory.DefaultStore.DeleteGraph(%q) should have not failed with error %v", "?a", err)
	}
}

func TestInsertDeleteDoesNotFail(t *testing.T) {
	if _, err := memory.DefaultStore.NewGraph("?a"); err != nil {
		t.Errorf("memory.DefaultStore.NewGraph(%q) should have not failed with error %v", "?a", err)
	}
	deleteTest(t)
	if err := memory.DefaultStore.DeleteGraph("?a"); err != nil {
		t.Errorf("memory.DefaultStore.DeleteGraph(%q) should have not failed with error %v", "?a", err)
	}
}
