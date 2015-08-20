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

import (
	"testing"

	"github.com/google/badwolf/bql/lexer"
)

func TestEmptyGrammarFailed(t *testing.T) {
	_, err := NewParser(&Grammar{
		"START": []Clause{
			{
				Elements: []Element{},
			},
			{
				Elements: []Element{},
			},
		},
	})
	if err == nil {
		t.Errorf("grammar.NewParse: should have failed given invalid derivation grammar")
	}
}

func TestNonLeftFactorizedGrammarFailed(t *testing.T) {
	_, err := NewParser(&Grammar{
		"START": []Clause{
			{
				Elements: []Element{
					NewSymbol("Foo"),
				},
			},
		},
	})
	if err == nil {
		t.Errorf("grammar.NewParse: should have failed given a non left factorized grammar")
	}
}

func TestValidGrammarCreatesAParser(t *testing.T) {
	_, err := NewParser(&Grammar{
		"START": []Clause{
			{
				Elements: []Element{
					NewTokenType(lexer.ItemQuery),
				},
			},
		},
	})
	if err != nil {
		t.Errorf("grammar.NewParse: should have produced a valid parser")
	}
}

func TestSimpleGrammarExpect(t *testing.T) {
	g := Grammar{
		"START": []Clause{
			{
				Elements: []Element{
					NewTokenType(lexer.ItemQuery),
					NewTokenType(lexer.ItemSemicolon),
				},
			},
		},
	}
	p, err := NewParser(&g)
	if err != nil {
		t.Errorf("grammar.NewParser: should have produced a valid parser")
	}
	b, err := p.expect(NewLLk("select;", 1), "START", g["START"][0])
	if !b || err != nil {
		t.Errorf("Parser.expect: failed to accept derivation tokens; %v, %v", b, err)
	}
}

func TestSimpleGrammarConsume(t *testing.T) {
	g := Grammar{
		"START": []Clause{
			{
				Elements: []Element{
					NewTokenType(lexer.ItemQuery),
					NewTokenType(lexer.ItemSemicolon),
				},
			},
		},
	}
	p, err := NewParser(&g)
	if err != nil {
		t.Errorf("grammar.NewParser: should have produced a valid parser")
	}
	b, err := p.consume(NewLLk("select;", 1), "START")
	if !b || err != nil {
		t.Errorf("Parser.consume: failed to accept derivation tokens; %v, %v", b, err)
	}
}

func TestComplexGrammarConsume(t *testing.T) {
	g := Grammar{
		"START": []Clause{
			{
				Elements: []Element{
					NewTokenType(lexer.ItemQuery),
					NewSymbol("END"),
				},
			},
		},
		"END": []Clause{
			{
				Elements: []Element{
					NewTokenType(lexer.ItemSemicolon),
				},
			},
		},
	}
	p, err := NewParser(&g)
	if err != nil {
		t.Errorf("grammar.NewParser: should have produced a valid parser")
	}
	b, err := p.consume(NewLLk("select;", 1), "START")
	if !b || err != nil {
		t.Errorf("Parser.consume: failed to accept derivation tokens; %v, %v", b, err)
	}
}

func TestGrammarHooks(t *testing.T) {
	s, p, e := 0, 0, 0
	start := func(Symbol) error {
		s++
		return nil
	}
	process := func(Element) error {
		p++
		return nil
	}
	end := func(Symbol) error {
		e++
		return nil
	}
	g := Grammar{
		"START": []Clause{
			{
				Elements: []Element{
					NewTokenType(lexer.ItemQuery),
					NewSymbol("END"),
				},
				ProcessStart:     start,
				ProcessedElement: process,
				ProcessEnd:       end,
			},
		},
		"END": []Clause{
			{
				Elements: []Element{
					NewTokenType(lexer.ItemSemicolon),
				},
				ProcessStart:     start,
				ProcessedElement: process,
				ProcessEnd:       end,
			},
		},
	}
	prsr, err := NewParser(&g)
	if err != nil {
		t.Errorf("grammar.NewParser: should have produced a valid parser")
	}
	b, err := prsr.consume(NewLLk("select;", 1), "START")
	if !b || err != nil {
		t.Errorf("Parser.consume: failed to accept derivation tokens; %v, %v", b, err)
	}
	if s != 2 {
		t.Errorf("Parser.consue: should have started 2 derivations, got %d instead", s)
	}
	if p != 3 {
		t.Errorf("Parser.consue: should have processed 3 elements, got %d instead", p)
	}
	if e != 2 {
		t.Errorf("Parser.consue: should have ended 2 derivations, got %d instead", e)
	}
}

func TestComplexGrammarParse(t *testing.T) {
	g := Grammar{
		"START": []Clause{
			{
				Elements: []Element{
					NewTokenType(lexer.ItemQuery),
					NewSymbol("END"),
				},
			},
		},
		"END": []Clause{
			{
				Elements: []Element{
					NewTokenType(lexer.ItemSemicolon),
				},
			},
		},
	}
	p, err := NewParser(&g)
	if err != nil {
		t.Errorf("grammar.NewParser: should have produced a valid parser")
	}
	if err := p.Parse(NewLLk("select;", 1)); err != nil {
		t.Errorf("Parser.consume: failed to accept derivation tokens; %v", err)
	}
}
