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
	"fmt"

	"github.com/google/badwolf/bql/lexer"
	"github.com/google/badwolf/bql/semantic"
)

// Element are the main components that define a derivation rule.
type Element struct {
	isSymbol  bool
	symbol    semantic.Symbol
	tokenType lexer.TokenType
}

// NewSymbol creates a new element from a symbol.
func NewSymbol(s semantic.Symbol) Element {
	return Element{
		isSymbol: true,
		symbol:   s,
	}
}

// NewTokenType creates a new element from a token.
func NewTokenType(t lexer.TokenType) Element {
	return Element{
		isSymbol:  false,
		tokenType: t,
	}
}

// Symbol returns the symbol box for the given element.
func (e Element) Symbol() semantic.Symbol {
	return e.symbol
}

// Token returns the value of the token box for the given element.
func (e Element) Token() lexer.TokenType {
	return e.tokenType
}

// ClauseHook is a function hook for the parser that gets called on clause wide
// events.
type ClauseHook func(*semantic.Statement, semantic.Symbol) error

// ElementHook is a function hook for the parser that gets called after an
// Element is confused.
type ElementHook func(*semantic.Statement, semantic.ConsumedElement) error

// Clause contains on clause of the derivation rule.
type Clause struct {
	Elements         []Element
	ProcessStart     ClauseHook
	ProcessEnd       ClauseHook
	ProcessedElement ElementHook
}

// Grammar contains the left factory LLk grammar to be parsed. All provided
// grammars *must* have the "START" symbol to initialte the parsing of input
// text.
type Grammar map[semantic.Symbol][]Clause

// Parser implements a LLk recursive decend parser for left factorized grammars.
type Parser struct {
	grammar *Grammar
}

// NewParser creates a new recursive decend parser for a left factorized
// grammar.
func NewParser(grammar *Grammar) (*Parser, error) {
	// Check that the grammar is left factorized.
	for _, clauses := range *grammar {
		idx := 0
		for _, cls := range clauses {
			if len(cls.Elements) == 0 {
				if idx == 0 {
					idx++
					continue
				}
				return nil, fmt.Errorf("grammar.NewParser: invalid extra empty clause derivation %v", clauses)
			}
			if cls.Elements[0].isSymbol {
				return nil, fmt.Errorf("grammar.NewParser: not left factored grammar in %v", clauses)
			}
		}
	}
	return &Parser{
		grammar: grammar,
	}, nil
}

// Parse attempts to run the parser for the given input.
func (p *Parser) Parse(llk *LLk, st *semantic.Statement) error {
	b, err := p.consume(llk, st, "START")
	if err != nil {
		return err
	}
	if !b {
		return fmt.Errorf("Parser.Parse: inconsitent parser, no error found, and no tokens were consumed")
	}
	return nil
}

// consume attempts to consume all input tokens for the provided symbols given
// the parser grammar.
func (p *Parser) consume(llk *LLk, st *semantic.Statement, s semantic.Symbol) (bool, error) {
	for _, clause := range (*p.grammar)[s] {
		if len(clause.Elements) == 0 {
			return true, nil
		}
		elem := clause.Elements[0]
		if elem.isSymbol {
			return false, fmt.Errorf("Parser.consume: not left factored grammar in %v", clause)
		}
		if llk.CanAccept(elem.Token()) {
			return p.expect(llk, st, s, clause)
		}
	}
	return false, fmt.Errorf("Parser.consume: could not consume token %s in production %s", llk.Current(), s)
}

// expect given the input, symbol, and clause attemps to satisfy all elements.
func (p *Parser) expect(llk *LLk, st *semantic.Statement, s semantic.Symbol, cls Clause) (bool, error) {
	if cls.ProcessStart != nil {
		if err := cls.ProcessStart(st, s); err != nil {
			return false, nil
		}
	}
	for _, elem := range cls.Elements {
		tkn := llk.Current()
		if elem.isSymbol {
			if b, err := p.consume(llk, st, elem.Symbol()); !b {
				return b, err
			}
		} else {
			if !llk.Consume(elem.Token()) {
				return false, fmt.Errorf("Parser.parse: Failed to consume %s, got %s instead", elem.Token(), llk.Current().Type)
			}
		}
		if cls.ProcessedElement != nil {
			var ce semantic.ConsumedElement
			if elem.isSymbol {
				ce = semantic.NewConsumedSymbol(ce.Symbol())
			} else {
				ce = semantic.NewConsumedToken(tkn)
			}
			if err := cls.ProcessedElement(st, ce); err != nil {
				return false, err
			}
		}
	}
	if cls.ProcessEnd != nil {
		if err := cls.ProcessEnd(st, s); err != nil {
			return false, err
		}
	}
	return true, nil
}
