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
	"unicode/utf8"
)

// TokenType list all the possible tokens returned by a lexer.
type TokenType int

const (
	// ItemError contains information about an error triggered while scanning.
	ItemError TokenType = iota
	// ItemEOF indicates end of input to be scanned
	ItemEOF

	/*
		ItemSelect
		ItemFrom
		ItemWhere
	*/

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
)

// Text constants that represent primitive types.
const (
	eof          = rune(-1)
	binding      = "?"
	leftBracket  = "{"
	rightBracket = "}"
	leftPar      = "("
	rightPar     = ")"
)

// Token contains the type and text collected around the captured token.
type Token struct {
	Type TokenType
	Text string
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
		if l.next() == eof {
			break
		}
	}
	l.emit(ItemEOF) // Useful to make EOF a token.
	return nil      // Stop the run loop.
}

func isSingleSymboToken(l *lexer, tt TokenType, prefix string) stateFn {
	if strings.HasPrefix(l.input[l.pos:], prefix) {
		l.next()
		l.emit(tt)
		return lexToken // Next state.
	}
	return nil
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
	l.tokens <- Token{t, l.input[l.start:l.pos]}
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
