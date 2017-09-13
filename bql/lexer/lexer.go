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
// The lexer is loosely written after the parser model described by Rob Pike
// in his presentation "Lexical Scanning in Go". Slides can be found at
// http://cuddle.googlecode.com/hg/talk/lex.html#landing-slide.
package lexer

import (
	"fmt"
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
	// ItemInsert represents insert keyword in BQL.
	ItemInsert
	// ItemDelete represents insert keyword in BQL.
	ItemDelete
	// ItemCreate represents the creation of a graph in BQL.
	ItemCreate
	// ItemConstruct represents the construct keyword in BQL.
	ItemConstruct
	// ItemDeconstruct represents the deconstruct keyword in BQL.
	ItemDeconstruct
	// ItemDrop represent the destruction of a graph in BQL.
	ItemDrop
	// ItemGraph represent the graph to be created of destroyed in BQL.
	ItemGraph
	// ItemData represents the data keyword in BQL.
	ItemData
	// ItemInto represents the into keyword in BQL.
	ItemInto
	// ItemFrom represents the from keyword in BQL.
	ItemFrom
	// ItemWhere represents the where keyword in BQL.
	ItemWhere
	// ItemAs represents the as keyword in BQL.
	ItemAs
	// ItemType represents keyword type in BQL.
	ItemType
	// ItemID represents id keyword in BQL.
	ItemID
	// ItemAt represents at keyword in BQL.
	ItemAt
	// ItemIn represents in keyword in BQL.
	ItemIn
	// ItemBefore represents the before keyword in BQL.
	ItemBefore
	// ItemAfter represents the after keyword in BQL.
	ItemAfter
	// ItemBetween represents the between keyword in BQL.
	ItemBetween
	// ItemCount represents the count function in BQL.
	ItemCount
	// ItemDistinct represents the distinct modifier in BQL.
	ItemDistinct
	// ItemSum represents the sum function in BQL.
	ItemSum
	// ItemGroup represents the group keyword in group by clause in BQL.
	ItemGroup
	// ItemBy represents the by keyword in group by clause in BQL.
	ItemBy
	// ItemOrder represent the order keyword in group by clause in BQL.
	ItemOrder
	// ItemHaving represents the having clause keyword clause in BQL.
	ItemHaving
	// ItemAsc represents asc keyword on order by clause in BQL.
	ItemAsc
	// ItemDesc represents desc keyword on order by clause in BQL
	ItemDesc
	// ItemLimit represents the limit clause in BQL.
	ItemLimit
	// ItemBinding represents a variable binding in BQL.
	ItemBinding
	// ItemNode represents a BadWolf node in BQL.
	ItemNode
	// ItemBlankNode represents a blank BadWolf node in BQL.
	ItemBlankNode
	// ItemLiteral represents a BadWolf literal in BQL.
	ItemLiteral
	// ItemPredicate represents a BadWolf predicates in BQL.
	ItemPredicate
	// ItemPredicateBound represents a BadWolf predicate bound in BQL.
	ItemPredicateBound
	// ItemLBracket represents the left opening bracket token in BQL.
	ItemLBracket
	// ItemRBracket represents the right opening bracket token in BQL.
	ItemRBracket
	// ItemLPar represents the left opening parenthesis token in BQL.
	ItemLPar
	// ItemRPar represents the right closing parenthesis token in BQL.
	ItemRPar
	// ItemDot represents the graph clause separator . in BQL.
	ItemDot
	// ItemSemicolon represents the final statement semicolon in BQL.
	ItemSemicolon
	// ItemComma represents the graph join operator in BQL.
	ItemComma
	// ItemLT represents < in BQL.
	ItemLT
	// ItemGT represents > in BQL.
	ItemGT
	// ItemEQ represents = in BQL.
	ItemEQ
	// ItemNot represents keyword not in BQL.
	ItemNot
	// ItemAnd represents keyword and in BQL.
	ItemAnd
	// ItemOr represents keyword or in BQL.
	ItemOr
	// ItemShow represents the show keyword.
	ItemShow
	// ItemGraphs represent the graphs keyword.
	ItemGraphs
)

func (tt TokenType) String() string {
	switch tt {
	case ItemError:
		return "ERROR"
	case ItemEOF:
		return "EOF"
	case ItemQuery:
		return "QUERY"
	case ItemInsert:
		return "INSERT"
	case ItemDelete:
		return "DELETE"
	case ItemCreate:
		return "CREATE"
	case ItemConstruct:
		return "CONSTRUCT"
	case ItemDeconstruct:
		return "DECONSTRUCT"
	case ItemDrop:
		return "DROP"
	case ItemGraph:
		return "Graph"
	case ItemData:
		return "DATA"
	case ItemInto:
		return "INTO"
	case ItemFrom:
		return "FROM"
	case ItemWhere:
		return "WHERE"
	case ItemCount:
		return "COUNT"
	case ItemSum:
		return "SUM"
	case ItemGroup:
		return "GROUP"
	case ItemBy:
		return "BY"
	case ItemHaving:
		return "HAVING"
	case ItemOrder:
		return "ORDER"
	case ItemAsc:
		return "ASC"
	case ItemDesc:
		return "DESC"
	case ItemLimit:
		return "LIMIT"
	case ItemAs:
		return "AS"
	case ItemBefore:
		return "BEFORE"
	case ItemAfter:
		return "AFTER"
	case ItemBetween:
		return "BETWEEN"
	case ItemBinding:
		return "BINDING"
	case ItemNode:
		return "NODE"
	case ItemBlankNode:
		return "BLANK_NODE"
	case ItemLiteral:
		return "LITERAL"
	case ItemPredicate:
		return "PREDICATE"
	case ItemPredicateBound:
		return "PREDICATE_BOUND"
	case ItemLBracket:
		return "LEFT_BRACKET"
	case ItemRBracket:
		return "RIGHT_BRACKET"
	case ItemLPar:
		return "LEFT_PARENT"
	case ItemRPar:
		return "RIGHT_PARENT"
	case ItemDot:
		return "DOT"
	case ItemSemicolon:
		return "SEMICOLON"
	case ItemComma:
		return "COMMA"
	case ItemLT:
		return "LT"
	case ItemGT:
		return "GT"
	case ItemEQ:
		return "EQ"
	case ItemNot:
		return "NOT"
	case ItemAnd:
		return "AND"
	case ItemOr:
		return "OR"
	case ItemID:
		return "ID"
	case ItemType:
		return "TYPE"
	case ItemAt:
		return "AT"
	case ItemIn:
		return "IN"
	case ItemDistinct:
		return "DISTINCT"
	case ItemShow:
		return "SHOW"
	case ItemGraphs:
		return "GRAPHS"
	default:
		return "UNKNOWN"
	}
}

// Text constants that represent primitive types.
const (
	eof            = rune(-1)
	binding        = rune('?')
	leftBracket    = rune('{')
	rightBracket   = rune('}')
	leftPar        = rune('(')
	rightPar       = rune(')')
	rightSquarePar = rune(']')
	dot            = rune('.')
	colon          = rune(':')
	semicolon      = rune(';')
	comma          = rune(',')
	slash          = rune('/')
	underscore     = rune('_')
	backSlash      = rune('\\')
	lt             = rune('<')
	gt             = rune('>')
	eq             = rune('=')
	quote          = rune('"')
	hat            = rune('^')
	at             = rune('@')
	newLine        = rune('\n')
	query          = "select"
	insert         = "insert"
	delete         = "delete"
	create         = "create"
	construct      = "construct"
	deconstruct    = "deconstruct"
	drop           = "drop"
	graph          = "graph"
	data           = "data"
	into           = "into"
	from           = "from"
	where          = "where"
	as             = "as"
	before         = "before"
	after          = "after"
	between        = "between"
	count          = "count"
	distinct       = "distinct"
	sum            = "sum"
	group          = "group"
	having         = "having"
	by             = "by"
	order          = "order"
	asc            = "asc"
	desc           = "desc"
	limit          = "limit"
	not            = "not"
	and            = "and"
	or             = "or"
	id             = "id"
	typeKeyword    = "type"
	atKeyword      = "at"
	inKeyword      = "in"
	showKeyword    = "show"
	graphsKeyword  = "graphs"
	anchor         = "\"@["
	literalType    = "\"^^type:"
	literalBool    = "bool"
	literalInt     = "int64"
	literalFloat   = "float64"
	literalText    = "text"
	literalBlob    = "blob"
)

// Token contains the type and text collected around the captured token.
type Token struct {
	Type         TokenType
	Text         string
	ErrorMessage string
}

// String returns a readable form of the token.
func (t *Token) String() string {
	return fmt.Sprintf("(%s, %s, %s)", t.Type, t.Text, t.ErrorMessage)
}

// stateFn represents the state of the scanner as a function that returns
// the next state.
type stateFn func(*lexer) stateFn

// lexer holds the state of the scanner.
type lexer struct {
	input    string     // the string being scanned.
	start    int        // start position of this item.
	pos      int        // current position in the input.
	width    int        // width of last rune read from input.
	line     int        // current line number for error reporting.
	lastLine int        // last line number for error reporting.
	col      int        // current column number for error reporting.
	lastCol  int        // last column number for error reporting.
	tokens   chan Token // channel of scanned items.
}

// lex creates a new lexer for the given input
func lex(input string, capacity int) (*lexer, <-chan Token) {
	l := &lexer{
		input:  input,
		tokens: make(chan Token, capacity),
	}
	go l.run() // Concurrently run state machine.
	return l, l.tokens
}

// New return a new read only channel with the tokens found in the provided
// input string.
func New(input string, capacity int) <-chan Token {
	if capacity < 0 {
		capacity = 0
	}
	_, c := lex(input, capacity)
	return c
}

// lexToken represents the initial state for token identification.
func lexToken(l *lexer) stateFn {
	for {
		{
			r := l.peek()
			switch r {
			case binding:
				l.next()
				return lexBinding
			case slash:
				return lexNode
			case underscore:
				l.next()
				return lexBlankNode
			case quote:
				return lexPredicateOrLiteral
			}
			if unicode.IsLetter(r) {
				return lexKeyword
			}
		}
		if state := isSingleSymbolToken(l, ItemLBracket, leftBracket); state != nil {
			return state
		}
		if state := isSingleSymbolToken(l, ItemRBracket, rightBracket); state != nil {
			return state
		}
		if state := isSingleSymbolToken(l, ItemLPar, leftPar); state != nil {
			return state
		}
		if state := isSingleSymbolToken(l, ItemRPar, rightPar); state != nil {
			return state
		}
		if state := isSingleSymbolToken(l, ItemSemicolon, semicolon); state != nil {
			return state
		}
		if state := isSingleSymbolToken(l, ItemDot, dot); state != nil {
			return state
		}
		if state := isSingleSymbolToken(l, ItemComma, comma); state != nil {
			return state
		}
		if state := isSingleSymbolToken(l, ItemLT, lt); state != nil {
			return state
		}
		if state := isSingleSymbolToken(l, ItemGT, gt); state != nil {
			return state
		}
		if state := isSingleSymbolToken(l, ItemEQ, eq); state != nil {
			return state
		}
		{
			r := l.next()
			if unicode.IsSpace(r) {
				l.ignore()
				continue
			}
			if l.next() == eof {
				break
			}
		}
	}
	l.emit(ItemEOF) // Useful to make EOF a token.
	return nil      // Stop the run loop.
}

// isSingleSymbolToken checks if a single char should be lexed.
func isSingleSymbolToken(l *lexer, tt TokenType, symbol rune) stateFn {
	if r := l.peek(); r == symbol {
		l.next()
		l.emit(tt)
		return lexSpace // Next state.
	}
	return nil
}

// lexBinding lexes a binding variable.
func lexBinding(l *lexer) stateFn {
	for {
		if r := l.next(); !unicode.IsLetter(r) && !unicode.IsDigit(r) && r != rune('_') || r == eof {
			l.backup()
			l.emit(ItemBinding)
			break
		}
	}
	return lexSpace
}

// lexSpace consumes spaces without emitting any token.
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

// lexKeyword lexes the BQL keywords.
func lexKeyword(l *lexer) stateFn {
	input := l.input[l.pos:]
	f := func(r rune) bool {
		return !unicode.IsLetter(r)
	}
	if idx := strings.IndexFunc(input, f); idx >= 0 {
		input = input[:idx]
	}
	if strings.EqualFold(input, query) {
		consumeKeyword(l, ItemQuery)
		return lexSpace
	}
	if strings.EqualFold(input, insert) {
		consumeKeyword(l, ItemInsert)
		return lexSpace
	}
	if strings.EqualFold(input, delete) {
		consumeKeyword(l, ItemDelete)
		return lexSpace
	}
	if strings.EqualFold(input, create) {
		consumeKeyword(l, ItemCreate)
		return lexSpace
	}
	if strings.EqualFold(input, construct) {
		consumeKeyword(l, ItemConstruct)
		return lexSpace
	}
	if strings.EqualFold(input, deconstruct) {
		consumeKeyword(l, ItemDeconstruct)
		return lexSpace
	}
	if strings.EqualFold(input, drop) {
		consumeKeyword(l, ItemDrop)
		return lexSpace
	}
	if strings.EqualFold(input, graph) {
		consumeKeyword(l, ItemGraph)
		return lexSpace
	}
	if strings.EqualFold(input, data) {
		consumeKeyword(l, ItemData)
		return lexSpace
	}
	if strings.EqualFold(input, into) {
		consumeKeyword(l, ItemInto)
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
	if strings.EqualFold(input, count) {
		consumeKeyword(l, ItemCount)
		return lexSpace
	}
	if strings.EqualFold(input, distinct) {
		consumeKeyword(l, ItemDistinct)
		return lexSpace
	}
	if strings.EqualFold(input, sum) {
		consumeKeyword(l, ItemSum)
		return lexSpace
	}
	if strings.EqualFold(input, group) {
		consumeKeyword(l, ItemGroup)
		return lexSpace
	}
	if strings.EqualFold(input, by) {
		consumeKeyword(l, ItemBy)
		return lexSpace
	}
	if strings.EqualFold(input, order) {
		consumeKeyword(l, ItemOrder)
		return lexSpace
	}
	if strings.EqualFold(input, asc) {
		consumeKeyword(l, ItemAsc)
		return lexSpace
	}
	if strings.EqualFold(input, desc) {
		consumeKeyword(l, ItemDesc)
		return lexSpace
	}
	if strings.EqualFold(input, having) {
		consumeKeyword(l, ItemHaving)
		return lexSpace
	}
	if strings.EqualFold(input, limit) {
		consumeKeyword(l, ItemLimit)
		return lexSpace
	}
	if strings.EqualFold(input, not) {
		consumeKeyword(l, ItemNot)
		return lexSpace
	}
	if strings.EqualFold(input, and) {
		consumeKeyword(l, ItemAnd)
		return lexSpace
	}
	if strings.EqualFold(input, or) {
		consumeKeyword(l, ItemOr)
		return lexSpace
	}
	if strings.EqualFold(input, id) {
		consumeKeyword(l, ItemID)
		return lexSpace
	}
	if strings.EqualFold(input, typeKeyword) {
		consumeKeyword(l, ItemType)
		return lexSpace
	}
	if strings.EqualFold(input, atKeyword) {
		consumeKeyword(l, ItemAt)
		return lexSpace
	}
	if strings.EqualFold(input, inKeyword) {
		consumeKeyword(l, ItemIn)
		return lexSpace
	}
	if strings.EqualFold(input, showKeyword) {
		consumeKeyword(l, ItemShow)
		return lexSpace
	}
	if strings.EqualFold(input, graphsKeyword) {
		consumeKeyword(l, ItemGraphs)
		return lexSpace
	}
	for {
		r := l.next()
		if unicode.IsSpace(r) || r == eof {
			l.backup()
			break
		}
	}
	l.emitError("found unknown keyword")
	return nil
}

func lexNode(l *lexer) stateFn {
	ltID := false
	for done := false; !done; {
		switch r := l.next(); r {
		case backSlash:
			if nr := l.peek(); nr == lt {
				l.next()
				continue
			}
		case eof:
			l.emitError("node is not properly terminated; missing final > delimiter")
			return nil
		case lt:
			ltID = true
		case gt:
			done = true
		}
	}
	if !ltID {
		l.emitError("node should start ID section with a < delimiter")
		return nil
	}
	l.emit(ItemNode)
	return lexSpace
}

// lexBlankNode tries to lex a blank node out of the input
func lexBlankNode(l *lexer) stateFn {
	if r := l.next(); r != colon {
		l.emitError("blank node should start with _:")
		return nil
	}
	if r := l.next(); !unicode.IsLetter(r) {
		l.emitError("blank node label should begin with a letter")
		return nil
	}
	for {
		if r := l.next(); !unicode.IsLetter(r) && !unicode.IsDigit(r) && r != rune('_') || r == eof {
			l.backup()
			l.emit(ItemBlankNode)
			break
		}
	}
	return lexSpace
}

// lexPredicateOrLiteral tries to lex a predicate or a literal out of the input.
func lexPredicateOrLiteral(l *lexer) stateFn {
	text := l.input[l.pos:]
	// Fix issue 39 (https://github.com/google/badwolf/issues/39)
	pIdx, lIdx := strings.Index(text, "\"@["), strings.Index(text, "\"^^type:")
	if pIdx < 0 && lIdx < 0 {
		l.emitError("failed to parse predicate or literal for opening \" delimiter")
		return nil
	}
	if pIdx > 0 && (lIdx < 0 || pIdx < lIdx) {
		return lexPredicate
	}
	return lexLiteral
}

// lexPredicate lexes a predicate out of the input.
func lexPredicate(l *lexer) stateFn {
	l.next()
	for done := false; !done; {
		switch r := l.next(); r {
		case backSlash:
			if nr := l.peek(); nr == quote {
				l.next()
				continue
			}
		case quote:
			l.backup()
			if !l.consume(anchor) {
				l.emitError("predicates require time anchor information; missing \"@[")
				return nil
			}
			var (
				nr     rune
				commas = 0
			)
			for {
				nr = l.next()
				if nr == comma {
					commas++
				}
				if nr == rightSquarePar || nr == eof {
					break
				}
			}
			if nr != rightSquarePar {
				l.emitError("predicate's time anchors should end with ] delimiter")
				return nil
			}
			if commas > 1 {
				l.emitError("predicate bounds should only have one , to separate bounds")
				return nil
			}
			if commas == 0 {
				l.emit(ItemPredicate)
			} else {
				l.emit(ItemPredicateBound)
			}
			done = true
		case eof:
			l.emitError("literals needs to be properly terminated; missing \" and type")
			return nil
		}
	}
	return lexSpace
}

// lexLiteral lexes a literal out of the input.
func lexLiteral(l *lexer) stateFn {
	l.next()
	for done := false; !done; {
		switch r := l.next(); r {
		case backSlash:
			if nr := l.peek(); nr == quote {
				l.next()
				continue
			}
		case quote:
			l.backup()
			if !l.consume(literalType) {
				l.emitError("literals require a type definintion; missing ^^type:")
				return nil
			}
			literalT := ""
			for {
				r := l.next()
				if !(unicode.IsLetter(r) || unicode.IsDigit(r)) || r == eof {
					break
				}
				literalT += string(r)
			}
			literalT = strings.ToLower(literalT)
			switch literalT {
			case literalBool, literalInt, literalFloat, literalText, literalBlob:
				l.backup()
				l.emit(ItemLiteral)
				done = true
			default:
				l.emitError("invalid literal type " + literalT)
				return nil
			}
		case eof:
			l.emitError("literals needs to be properly terminated; missing \" and type")
			return nil
		}
	}
	return lexSpace
}

// consumeKeyword consume and emits a valid token
func consumeKeyword(l *lexer, t TokenType) {
	for {
		if r := l.next(); !unicode.IsLetter(r) || r == eof {
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
		ErrorMessage: fmt.Sprintf("[lexer:%d:%d] %s", l.line, l.col, msg),
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
	l.col, l.line = l.lastCol, l.lastLine
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
	l.lastCol, l.lastLine = l.col, l.line
	l.col++
	if r == newLine {
		l.line++
		l.col = 0
	}
	return r
}

// peek returns but does not consume the next rune in the input.
func (l *lexer) peek() rune {
	r := l.next()
	l.backup()
	return r
}

// accept consumes the next rune if it's equal to the one provided.
func (l *lexer) accept(r rune) bool {
	if unicode.ToLower(l.next()) == unicode.ToLower(r) {
		return true
	}
	l.backup()
	return false
}

func (l *lexer) consume(text string) bool {
	for _, c := range text {
		if !l.accept(c) {
			return false
		}
	}
	return true
}
