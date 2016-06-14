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

// LLk provide the basic lookahead mechanisms required to implement a recursive
// descent LLk parser.
type LLk struct {
	k    int
	c    <-chan lexer.Token
	tkns []lexer.Token
}

// NewLLk creates a LLk structure for the given string to parse and the
// indicated k lookahead.
func NewLLk(input string, k int) *LLk {
	c := lexer.New(input, 2*k) // +2 to keep a bit of buffer available.
	l := &LLk{
		k: k,
		c: c,
	}
	for i := 0; i < k+1; i++ {
		appendNextToken(l)
	}
	return l
}

// appendNextToken tries to append a new token. If not tokens are available
// it appends ItemEOF token.
func appendNextToken(l *LLk) {
	for t := range l.c {
		l.tkns = append(l.tkns, t)
		return
	}
	l.tkns = append(l.tkns, lexer.Token{Type: lexer.ItemEOF})
}

// Current returns the current token being processed.
func (l *LLk) Current() *lexer.Token {
	return &l.tkns[0]
}

// Peek returns the token for the k look ahead. It will return nil and failed
// fail with an error if the provided k is bigger than the declared look ahead
// on creation.
func (l *LLk) Peek(k int) (*lexer.Token, error) {
	if k > l.k {
		return nil, fmt.Errorf("grammar.LLk: cannot look ahead %d beyond defined %d", k, l.k)
	}
	if k <= 0 {
		return nil, fmt.Errorf("grammar.LLk: invalid look ahead value %d", k)
	}
	return &l.tkns[k], nil
}

// CanAccept returns true if the provided token matches the current on being
// processed, false otherwise.
func (l *LLk) CanAccept(tt lexer.TokenType) bool {
	return l.tkns[0].Type == tt
}

// Consume will consume the current token and move to the next one if it matches
// the provided token, false otherwise.
func (l *LLk) Consume(tt lexer.TokenType) bool {
	if l.tkns[0].Type != tt {
		return false
	}
	l.tkns = l.tkns[1:]
	appendNextToken(l)
	return true
}
