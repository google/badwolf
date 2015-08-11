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

// Package lexer implements the lexer used bye the BadWolf query language.
// The lexer is losely written after the parsel model described by Rob Pike
// in his presentation "Lexical Scanning in Go". Slides can be found at
// http://cuddle.googlecode.com/hg/talk/lex.html#landing-slide.
package lexer

import (
	"strings"
	"unicode"
	"unicode/utf8"
)

// TokenType list all the possible tokens returned by a lexer.
type TokenType int

const (
	// ItemError contains information about an error triggered while scanning.
	ItemError TokenType = iota
	// ItemEOF indicates end of input to be scanned in BQL.
	ItemEOF

	// ItemQuery represents the select keyword in BQL.
	ItemQuery
	// ItemFrom represents the from keyword in BQL.
	ItemFrom
	// ItemWhere represents the where keyword in BQL.
	ItemWhere
	// ItemAs represents the as keyword in BQL.
	ItemAs
	// ItemBefore represents the before keyword in BQL.
	ItemBefore
	// ItemAfter represents the after keyword in BQL.
	ItemAfter
	// ItemBetween represents the betwen keyword in BQL.
	ItemBetween

	// ItemBinding respresents a variable binding in BQL.
	ItemBinding

	/*
		  ItemNode
			ItemPredicate
	*/

	// ItemLBracket representes the left opening bracket token in BQL.
	ItemLBracket
	// ItemRBracket representes the right opening bracket token in BQL.
	ItemRBracket
	// ItemLPar representes the left opening parentesis token in BQL.
	ItemLPar
	// ItemRPar representes the right closing parentesis token in BQL.
	ItemRPar
	// ItemDot represents the graph clause separator . in BQL.
	ItemDot
	// ItemSemicolon represents the final statement semicolon in BQL.
	ItemSemicolon
)

// Text constants that represent primitive types.
const (
	eof          = rune(-1)
	binding      = rune('?')
	leftBracket  = rune('{')
	rightBracket = rune('}')
	leftPar      = rune('(')
	rightPar     = rune(')')
	dot          = rune('.')
	semicolon    = rune(';')
	query        = "select"
	from         = "from"
	where        = "where"
	as           = "as"
	before       = "before"
	after        = "after"
	between      = "between"
)

// Token contains the type and text collected around the captured token.
type Token struct {
	Type         TokenType
	Text         string
	ErrorMessage string
}

// stateFn represents the state of the scanner  as a function that returns
// the next state.
type stateFn func(*lexer) stateFn

// lexer holds the state of the scanner.
type lexer struct {
	input  string     // the string being scanned.
	start  int        // start position of this item.
	pos    int        // current position in the input.
	width  int        // width of last rune read from input.
	tokens chan Token // channel of scanned items.
}

// lex creates a new lexer for the givne input
func lex(input string) (*lexer, <-chan Token) {
	l := &lexer{
		input:  input,
		tokens: make(chan Token),
	}
	go l.run() // Concurrently run state machine.
	return l, l.tokens
}

// lexToken represents the initial state for token identification.
func lexToken(l *lexer) stateFn {
	for {
		{
			r := l.peek()
			if r == binding {
				l.next()
				return lexBinding
			}
			if unicode.IsLetter(r) {
				return lexKeyword
			}
		}
		if state := isSingleSymboToken(l, ItemLBracket, leftBracket); state != nil {
			return state
		}
		if state := isSingleSymboToken(l, ItemRBracket, rightBracket); state != nil {
			return state
		}
		if state := isSingleSymboToken(l, ItemLPar, leftPar); state != nil {
			return state
		}
		if state := isSingleSymboToken(l, ItemRPar, rightPar); state != nil {
			return state
		}
		if state := isSingleSymboToken(l, ItemSemicolon, semicolon); state != nil {
			return state
		}
		if state := isSingleSymboToken(l, ItemDot, dot); state != nil {
			return state
		}
		if l.next() == eof {
			break
		}
	}
	l.emit(ItemEOF) // Useful to make EOF a token.
	return nil      // Stop the run loop.
}

// isSingleSymboToken check if a single char should be lexed.
func isSingleSymboToken(l *lexer, tt TokenType, symbol rune) stateFn {
	if r := l.peek(); r == symbol {
		l.next()
		l.emit(tt)
		return lexToken // Next state.
	}
	return nil
}

// lexBinding lexes a binding variable.
func lexBinding(l *lexer) stateFn {
	for {
		if r := l.next(); unicode.IsSpace(r) || r == eof {
			l.backup()
			l.emit(ItemBinding)
			break
		}
	}
	return lexSpace
}

// lexSpace consumes spaces without emiting any token.
func lexSpace(l *lexer) stateFn {
	for {
		if r := l.next(); !unicode.IsSpace(r) || r == eof {
			break
		}
	}
	l.backup()
	l.ignore()
	return lexToken
}

// lexKeywork lexes the BQL keywords.
func lexKeyword(l *lexer) stateFn {
	input := l.input[l.pos:]
	if idx := strings.IndexFunc(input, unicode.IsSpace); idx >= 0 {
		input = input[:idx]
	}
	if strings.EqualFold(input, query) {
		consumeKeyword(l, ItemQuery)
		return lexSpace
	}
	if strings.EqualFold(input, from) {
		consumeKeyword(l, ItemFrom)
		return lexSpace
	}
	if strings.EqualFold(input, where) {
		consumeKeyword(l, ItemWhere)
		return lexSpace
	}
	if strings.EqualFold(input, as) {
		consumeKeyword(l, ItemAs)
		return lexSpace
	}
	if strings.EqualFold(input, before) {
		consumeKeyword(l, ItemBefore)
		return lexSpace
	}
	if strings.EqualFold(input, after) {
		consumeKeyword(l, ItemAfter)
		return lexSpace
	}
	if strings.EqualFold(input, between) {
		consumeKeyword(l, ItemBetween)
		return lexSpace
	}
	for {
		r := l.next()
		if unicode.IsSpace(r) || r == eof {
			l.backup()
			break
		}
	}
	l.emitError("Found unknown keyword")
	return lexSpace
}

// consumeKeyword consume and emits a valid token
func consumeKeyword(l *lexer, t TokenType) {
	for {
		if r := l.next(); unicode.IsSpace(r) || r == eof {
			l.backup()
			l.emit(t)
			break
		}
	}
}

// run lexes the input by executing state functions until the state is nil.
func (l *lexer) run() {
	for state := lexToken(l); state != nil; {
		state = state(l)
	}
	close(l.tokens) // No more tokens will be delivered.
}

// emit passes an item back to the client.
func (l *lexer) emit(t TokenType) {
	l.tokens <- Token{
		Type: t,
		Text: l.input[l.start:l.pos],
	}
	l.start = l.pos
}

// emitError passes and error to the client with proper error messaging.
func (l *lexer) emitError(msg string) {
	l.tokens <- Token{
		Type:         ItemError,
		Text:         l.input[l.start:l.pos],
		ErrorMessage: msg,
	}
	l.start = l.pos
}

// ignore skips over the pending input before this point.
func (l *lexer) ignore() {
	l.start = l.pos
}

// backup steps back one rune. Can be called only once per call of next.
func (l *lexer) backup() {
	l.pos -= l.width
}

// next returns the next rune in the input.
func (l *lexer) next() rune {
	if l.pos >= len(l.input) {
		l.width = 0
		return eof
	}
	var r rune
	r, l.width = utf8.DecodeRuneInString(l.input[l.pos:])
	l.pos += l.width
	return r
}

// peek returns but does not consume the next rune in the input.
func (l *lexer) peek() rune {
	r := l.next()
	l.backup()
	return r
}

// accept consumes the next rune if it's from the valid set.
func (l *lexer) accept(valid string) bool {
	if strings.IndexRune(valid, l.next()) >= 0 {
		return true
	}
	l.backup()
	return false
}
