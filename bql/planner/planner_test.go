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
	"bytes"
	"testing"

	"github.com/google/badwolf/bql/grammar"
	"github.com/google/badwolf/bql/semantic"
	"github.com/google/badwolf/io"
	"github.com/google/badwolf/storage"
	"github.com/google/badwolf/storage/memory"
	"github.com/google/badwolf/triple/literal"
)

func insertTest(t *testing.T) {
	bql := `insert data into ?a {/_<foo> "bar"@[] /_<foo> .
                               /_<foo> "bar"@[] "bar"@[1975-01-01T00:01:01.999999999Z] .
                               /_<foo> "bar"@[] "yeah"^^type:text};`
	p, err := grammar.NewParser(grammar.SemanticBQL())
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
	if _, err := pln.Excecute(); err != nil {
		t.Errorf("planner.Execute: failed to execute insert plan with error %v", err)
	}
	g, err := memory.DefaultStore.Graph("?a")
	if err != nil {
		t.Errorf("memory.DefaultStore.Graph(%q) should have not fail with error %v", "?a", err)
	}
	i := 0
	ts, err := g.Triples()
	if err != nil {
		t.Error(err)
	}
	for _ = range ts {
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
	p, err := grammar.NewParser(grammar.SemanticBQL())
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
	if _, err := pln.Excecute(); err != nil {
		t.Errorf("planner.Execute: failed to execute insert plan with error %v", err)
	}
	g, err := memory.DefaultStore.Graph("?a")
	if err != nil {
		t.Errorf("memory.DefaultStore.Graph(%q) should have not fail with error %v", "?a", err)
	}
	i := 0
	ts, err := g.Triples()
	if err != nil {
		t.Error(err)
	}
	for _ = range ts {
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

func TestCreateGraph(t *testing.T) {
	memory.DefaultStore.DeleteGraph("?foo")
	memory.DefaultStore.DeleteGraph("?bar")

	bql := `create graph ?foo, ?bar;`
	p, err := grammar.NewParser(grammar.SemanticBQL())
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
	if _, err := pln.Excecute(); err != nil {
		t.Errorf("planner.Execute: failed to execute insert plan with error %v", err)
	}
	if _, err := memory.DefaultStore.Graph("?foo"); err != nil {
		t.Errorf("planner.Execute: failed to create graph %q with error %v", "?foo", err)
	}
	if _, err := memory.DefaultStore.Graph("?bar"); err != nil {
		t.Errorf("planner.Execute: failed to create graph %q with error %v", "?bar", err)
	}
}

func TestDropGraph(t *testing.T) {
	memory.DefaultStore.DeleteGraph("?foo")
	memory.DefaultStore.DeleteGraph("?bar")
	memory.DefaultStore.NewGraph("?foo")
	memory.DefaultStore.NewGraph("?bar")

	bql := `drop graph ?foo, ?bar;`
	p, err := grammar.NewParser(grammar.SemanticBQL())
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
	if _, err := pln.Excecute(); err != nil {
		t.Errorf("planner.Execute: failed to execute insert plan with error %v", err)
	}
	if g, err := memory.DefaultStore.Graph("?foo"); err == nil {
		t.Errorf("planner.Execute: failed to drop graph %q; returned %v", "?foo", g)
	}
	if g, err := memory.DefaultStore.Graph("?bar"); err == nil {
		t.Errorf("planner.Execute: failed to drop graph %q; returned %v", "?bar", g)
	}
}

const testTriples = `
	/u<joe> "parent_of"@[] /u<mary>
  /u<joe> "parent_of"@[] /u<peter>
  /u<peter> "parent_of"@[] /u<john>
  /u<peter> "parent_of"@[] /u<eve>
	/u<peter> "bought"@[2016-01-01T00:00:00-08:00] /c<mini>
	/u<peter> "bought"@[2016-03-01T00:00:00-08:00] /c<model s>
	/u<peter> "bought"@[2016-03-01T00:00:00-08:00] /c<model x>
	/u<peter> "bought"@[2016-03-01T00:00:00-08:00] /c<model y>
	/c<mini> "is_a"@[] /t<car>
	/c<model s> "is_a"@[] /t<car>
	/c<model x> "is_a"@[] /t<car>
	/c<model y> "is_a"@[] /t<car>
`

func populateTestStore(t *testing.T) storage.Store {
	s := memory.NewStore()
	g, err := s.NewGraph("?test_graph")
	if err != nil {
		t.Fatalf("memory.NewGraph failed to create \"?test_graph\" with error %v", err)
	}
	b := bytes.NewBufferString(testTriples)
	if _, err := io.ReadIntoGraph(g, b, literal.DefaultBuilder()); err != nil {
		t.Fatalf("io.ReadIntoGraph failed to read test graph with error %v", err)
	}
	return s
}

func TestQuery(t *testing.T) {
	testTable := []struct {
		q string
		r string
	}{
		{
			q: `select ?o from ?test_graph where {/u<joe> "parent_of"@[] ?o}`,
			r: "\n",
		},
	}

	s := populateTestStore(t)
	p, err := grammar.NewParser(grammar.SemanticBQL())
	if err != nil {
		t.Fatalf("grammar.NewParser: should have produced a valid BQL parser with error %v", err)
	}
	for _, entry := range testTable {
		st := &semantic.Statement{}
		if err := p.Parse(grammar.NewLLk(entry.q, 1), st); err == nil {
			t.Errorf("Parser.consume: failed to reject invalid semantic entry %q", entry)
		}
		plnr, err := New(s, st)
		if err != nil {
			t.Errorf("planner.New failed to create a valid query plan with error %v", err)
		}
		tbl, err := plnr.Excecute()
		if err != nil {
			t.Errorf("planner.Excecute failed for query %q with error %v", entry.q, err)
		}
		stbl, err := tbl.ToText(", ")
		if err != nil {
			t.Errorf("tbl.ToText failed to serialize table with error %v", err)
		}
		if got, want := stbl.String(), entry.r; got != want {
			t.Errorf("planner.Excecute failed to reture the expected output for query %q; got\n%q\nwant\n%q", entry.q, got, want)
		}
	}
}
