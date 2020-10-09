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

package semantic

import (
	"fmt"

	"github.com/google/badwolf/bql/lexer"
	"github.com/google/badwolf/triple/literal"
	"github.com/google/badwolf/triple/node"
	"github.com/google/badwolf/triple/predicate"
)

// Symbol of the LLk left factored grammar.
type Symbol string

// String returns a string representation of the symbol
func (s Symbol) String() string {
	return string(s)
}

// ConsumedElement groups the current element being processed by the parser.
type ConsumedElement struct {
	isSymbol bool
	symbol   Symbol
	token    *lexer.Token
}

// NewConsumedSymbol create a new consumed element that boxes a symbol.
func NewConsumedSymbol(s Symbol) ConsumedElement {
	return ConsumedElement{
		isSymbol: true,
		symbol:   s,
	}
}

// NewConsumedToken create a new consumed element that boxes a token.
func NewConsumedToken(tkn *lexer.Token) ConsumedElement {
	return ConsumedElement{
		isSymbol: false,
		token:    tkn,
	}
}

// IsSymbol returns true if the boxed element is a symbol; false otherwise.
func (c ConsumedElement) IsSymbol() bool {
	return c.isSymbol
}

// Symbol returns the boxed symbol.
func (c ConsumedElement) Symbol() Symbol {
	return c.symbol
}

// Token returns the boxed token.
func (c ConsumedElement) Token() *lexer.Token {
	return c.token
}

// String returns a string representation of the ConsumedElement.
func (c ConsumedElement) String() string {
	return fmt.Sprintf("{isSymbol=%v, symbol=%s, token=%s}", c.isSymbol, c.symbol, c.token)
}

// ToNode converts the node found by the lexer and converts it into a BadWolf
// node.
func ToNode(ce ConsumedElement) (*node.Node, error) {
	if ce.IsSymbol() {
		return nil, fmt.Errorf("semantic.ToNode cannot convert symbol %v to a node", ce)
	}
	tkn := ce.Token()
	if tkn.Type != lexer.ItemNode && tkn.Type != lexer.ItemBlankNode {
		return nil, fmt.Errorf("semantic.ToNode cannot convert token type %s to a node", tkn.Type)
	}
	return node.Parse(tkn.Text)
}

// ToPredicate converts the node found by the lexer and converts it into a
// BadWolf predicate.
func ToPredicate(ce ConsumedElement) (*predicate.Predicate, error) {
	if ce.IsSymbol() {
		return nil, fmt.Errorf("semantic.ToPredicate cannot convert symbol %v to a predicate", ce)
	}
	tkn := ce.Token()
	if tkn.Type != lexer.ItemPredicate {
		return nil, fmt.Errorf("semantic.ToPredicate cannot convert token type %s to a predicate", tkn.Type)
	}
	return predicate.Parse(tkn.Text)
}

// ToLiteral converts the node found by the lexer and converts it into a
// BadWolf literal.
func ToLiteral(ce ConsumedElement) (*literal.Literal, error) {
	if ce.IsSymbol() {
		return nil, fmt.Errorf("semantic.ToLiteral cannot convert symbol %v to a literal", ce)
	}
	tkn := ce.Token()
	if tkn.Type != lexer.ItemLiteral {
		return nil, fmt.Errorf("semantic.ToLiteral cannot convert token type %s to a literal", tkn.Type)
	}
	return literal.DefaultBuilder().Parse(tkn.Text)
}
