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
)

// Symbol of the LLk left factored grammar.
type Symbol string

// String returns a string representation of the symbol
func (s Symbol) String() string {
	return string(s)
}

// Element are the main components that define a derivation rule.
type Element struct {
	isSymbol  bool
	symbol    Symbol
	tokenType lexer.TokenType
}

// NewSymbol creates a new element from a symbol.
func NewSymbol(s Symbol) Element {
	return Element{
		isSymbol: true,
		symbol:   s,
	}
}

// NewToken creates a new element from a token.
func NewToken(t lexer.TokenType) Element {
	return Element{
		isSymbol:  false,
		tokenType: t,
	}
}

// Symbol returns the symbol box for the given element.
func (e Element) Symbol() Symbol {
	return e.symbol
}

// Token returns the value of the token box for the given element.
func (e Element) Token() lexer.TokenType {
	return e.tokenType
}

// ClauseHook is a function hook for the parser that gets called on clause wide
// events.
type ClauseHook func(Symbol)

// ElementHook is a function hook for the parser that gets called after an
// Element is confused.
type ElementHook func(Element)

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
type Grammar map[Symbol][]Clause

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
func (p *Parser) Parse(llk *LLk) error {
	b, err := p.consume(llk, "START")
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
func (p *Parser) consume(llk *LLk, s Symbol) (bool, error) {
	for _, clause := range (*p.grammar)[s] {
		if len(clause.Elements) == 0 {
			return true, nil
		}
		elem := clause.Elements[0]
		if elem.isSymbol {
			return false, fmt.Errorf("Parser.consume: not left factored grammar in %v", clause)
		}
		if llk.CanAccept(elem.Token()) {
			return p.expect(llk, s, clause)
		}
	}
	return false, fmt.Errorf("Parser.consume: could not consume token %s in production %s", llk.Current(), s)
}

// expect given the input, symbol, and clause attemps to satisfy all elements.
func (p *Parser) expect(llk *LLk, s Symbol, cls Clause) (bool, error) {
	if cls.ProcessStart != nil {
		cls.ProcessStart(s)
	}
	for _, elem := range cls.Elements {
		if elem.isSymbol {
			if b, err := p.consume(llk, elem.Symbol()); !b {
				return b, err
			}
		} else {
			if !llk.Consume(elem.Token()) {
				return false, fmt.Errorf("Parser.parse: Failed to consume %s, got %s instead", elem.Token(), llk.Current().Type)
			}
		}
		if cls.ProcessedElement != nil {
			cls.ProcessedElement(elem)
		}
	}
	if cls.ProcessEnd != nil {
		cls.ProcessEnd(s)
	}
	return true, nil
}
