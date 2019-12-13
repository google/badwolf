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

package lexer

import "testing"

func TestTokenTypeString(t *testing.T) {
	table := []struct {
		tt   TokenType
		want string
	}{
		{ItemError, "ERROR"},
		{ItemEOF, "EOF"},
		{ItemQuery, "QUERY"},
		{ItemInsert, "INSERT"},
		{ItemDelete, "DELETE"},
		{ItemCreate, "CREATE"},
		{ItemConstruct, "CONSTRUCT"},
		{ItemDeconstruct, "DECONSTRUCT"},
		{ItemDrop, "DROP"},
		{ItemGraph, "Graph"},
		{ItemData, "DATA"},
		{ItemInto, "INTO"},
		{ItemFrom, "FROM"},
		{ItemWhere, "WHERE"},
		{ItemCount, "COUNT"},
		{ItemSum, "SUM"},
		{ItemGroup, "GROUP"},
		{ItemBy, "BY"},
		{ItemHaving, "HAVING"},
		{ItemOrder, "ORDER"},
		{ItemAsc, "ASC"},
		{ItemDesc, "DESC"},
		{ItemLimit, "LIMIT"},
		{ItemAs, "AS"},
		{ItemBefore, "BEFORE"},
		{ItemAfter, "AFTER"},
		{ItemBetween, "BETWEEN"},
		{ItemBinding, "BINDING"},
		{ItemNode, "NODE"},
		{ItemBlankNode, "BLANK_NODE"},
		{ItemLiteral, "LITERAL"},
		{ItemPredicate, "PREDICATE"},
		{ItemPredicateBound, "PREDICATE_BOUND"},
		{ItemLBracket, "LEFT_BRACKET"},
		{ItemRBracket, "RIGHT_BRACKET"},
		{ItemLPar, "LEFT_PARENT"},
		{ItemRPar, "RIGHT_PARENT"},
		{ItemDot, "DOT"},
		{ItemSemicolon, "SEMICOLON"},
		{ItemComma, "COMMA"},
		{ItemLT, "LT"},
		{ItemGT, "GT"},
		{ItemEQ, "EQ"},
		{ItemNot, "NOT"},
		{ItemAnd, "AND"},
		{ItemOr, "OR"},
		{ItemID, "ID"},
		{ItemType, "TYPE"},
		{ItemAt, "AT"},
		{ItemIn, "IN"},
		{ItemDistinct, "DISTINCT"},
		{ItemShow, "SHOW"},
		{ItemGraphs, "GRAPHS"},
		{ItemOptional, "OPTIONAL"},
		{TokenType(-1), "UNKNOWN"},
	}

	for i, entry := range table {
		if got, want := entry.tt.String(), entry.want; got != want {
			t.Errorf("[case %d] failed; got %v, want %v", i, got, want)
		}
	}
}

func TestIndividualTokens(t *testing.T) {
	table := []struct {
		input  string
		tokens []Token
	}{
		{"",
			[]Token{
				{Type: ItemEOF}}},
		{"{}().;,<>=",
			[]Token{
				{Type: ItemLBracket, Text: "{"},
				{Type: ItemRBracket, Text: "}"},
				{Type: ItemLPar, Text: "("},
				{Type: ItemRPar, Text: ")"},
				{Type: ItemDot, Text: "."},
				{Type: ItemSemicolon, Text: ";"},
				{Type: ItemComma, Text: ","},
				{Type: ItemLT, Text: "<"},
				{Type: ItemGT, Text: ">"},
				{Type: ItemEQ, Text: "="},
				{Type: ItemEOF}}},
		{"?foo ?bar ?1234 ?foo_bar ?bar_foo",
			[]Token{
				{Type: ItemBinding, Text: "?foo"},
				{Type: ItemBinding, Text: "?bar"},
				{Type: ItemBinding, Text: "?1234"},
				{Type: ItemBinding, Text: "?foo_bar"},
				{Type: ItemBinding, Text: "?bar_foo"},
				{Type: ItemEOF}}},
		{`SeLeCt FrOm WhErE As BeFoRe AfTeR BeTwEeN CoUnT SuM GrOuP bY HaViNg LiMiT
		  OrDeR AsC DeSc NoT AnD Or Id TyPe At DiStInCt InSeRt DeLeTe DaTa InTo
		  cONsTruCT CrEaTe DrOp GrApH OpTiOnAl`,
			[]Token{
				{Type: ItemQuery, Text: "SeLeCt"},
				{Type: ItemFrom, Text: "FrOm"},
				{Type: ItemWhere, Text: "WhErE"},
				{Type: ItemAs, Text: "As"},
				{Type: ItemBefore, Text: "BeFoRe"},
				{Type: ItemAfter, Text: "AfTeR"},
				{Type: ItemBetween, Text: "BeTwEeN"},
				{Type: ItemCount, Text: "CoUnT"},
				{Type: ItemSum, Text: "SuM"},
				{Type: ItemGroup, Text: "GrOuP"},
				{Type: ItemBy, Text: "bY"},
				{Type: ItemHaving, Text: "HaViNg"},
				{Type: ItemLimit, Text: "LiMiT"},
				{Type: ItemOrder, Text: "OrDeR"},
				{Type: ItemAsc, Text: "AsC"},
				{Type: ItemDesc, Text: "DeSc"},
				{Type: ItemNot, Text: "NoT"},
				{Type: ItemAnd, Text: "AnD"},
				{Type: ItemOr, Text: "Or"},
				{Type: ItemID, Text: "Id"},
				{Type: ItemType, Text: "TyPe"},
				{Type: ItemAt, Text: "At"},
				{Type: ItemDistinct, Text: "DiStInCt"},
				{Type: ItemInsert, Text: "InSeRt"},
				{Type: ItemDelete, Text: "DeLeTe"},
				{Type: ItemData, Text: "DaTa"},
				{Type: ItemInto, Text: "InTo"},
				{Type: ItemConstruct, Text: "cONsTruCT"},
				{Type: ItemCreate, Text: "CrEaTe"},
				{Type: ItemDrop, Text: "DrOp"},
				{Type: ItemGraph, Text: "GrApH"},
				{Type: ItemOptional, Text: "OpTiOnAl"},
				{Type: ItemEOF}}},
		{"/_<foo>/_<bar>",
			[]Token{
				{Type: ItemNode, Text: "/_<foo>"},
				{Type: ItemNode, Text: "/_<bar>"},
				{Type: ItemEOF}}},
		{"/_<foo>/_\\<bar>",
			[]Token{
				{Type: ItemNode, Text: "/_<foo>"},
				{Type: ItemError, Text: "/_\\<bar>",
					ErrorMessage: "[lexer:0:15] node should start ID section with a < delimiter"},
				{Type: ItemEOF}}},
		{"/_foo>",
			[]Token{
				{Type: ItemError, Text: "/_foo>",
					ErrorMessage: "[lexer:0:6] node should start ID section with a < delimiter"},
				{Type: ItemEOF}}},
		{"/_<foo",
			[]Token{
				{Type: ItemError, Text: "/_<foo",
					ErrorMessage: "[lexer:0:6] node is not properly terminated; missing final > delimiter"},
				{Type: ItemEOF}}},
		{"_:v1 _:foo_bar",
			[]Token{
				{Type: ItemBlankNode, Text: "_:v1"},
				{Type: ItemBlankNode, Text: "_:foo_bar"},
				{Type: ItemEOF}}},
		{"_v1",
			[]Token{
				{Type: ItemError, Text: "_v",
					ErrorMessage: "[lexer:0:2] blank node should start with _:"},
				{Type: ItemEOF}}},

		{"_:1v",
			[]Token{
				{Type: ItemError, Text: "_:1",
					ErrorMessage: "[lexer:0:3] blank node label should begin with a letter"},
				{Type: ItemEOF}}},
		{"_:_",
			[]Token{
				{Type: ItemError, Text: "_:_",
					ErrorMessage: "[lexer:0:3] blank node label should begin with a letter"},
				{Type: ItemEOF}}},
		{`"true"^^type:bool "1"^^type:int64"2"^^type:float64"t"^^type:text`,
			[]Token{
				{Type: ItemLiteral, Text: `"true"^^type:bool`},
				{Type: ItemLiteral, Text: `"1"^^type:int64`},
				{Type: ItemLiteral, Text: `"2"^^type:float64`},
				{Type: ItemLiteral, Text: `"t"^^type:text`},
				{Type: ItemEOF}}},
		{`"[1 2 3 4]"^^type:blob`,
			[]Token{
				{Type: ItemLiteral, Text: `"[1 2 3 4]"^^type:blob`},
				{Type: ItemEOF}}},
		{"\"1\"^type:int64",
			[]Token{
				{Type: ItemError,
					ErrorMessage: "[lexer:0:0] failed to parse predicate or literal for opening \" delimiter"},
				{Type: ItemEOF}}},
		{"\"1\"^^type:int32",
			[]Token{
				{Type: ItemError,
					Text:         `"1"^^type:int32`,
					ErrorMessage: "[lexer:0:15] invalid literal type int32"},
				{Type: ItemEOF}}},
		{`"p1"@[] "p2"@["some data"]"p3"@["some data"]"p4"@["a","b"]"p4"@["a",]"p4"@[,"b"]"p4"@[,]`,
			[]Token{
				{Type: ItemPredicate, Text: `"p1"@[]`},
				{Type: ItemPredicate, Text: `"p2"@["some data"]`},
				{Type: ItemPredicate, Text: `"p3"@["some data"]`},
				{Type: ItemPredicateBound, Text: `"p4"@["a","b"]`},
				{Type: ItemPredicateBound, Text: `"p4"@["a",]`},
				{Type: ItemPredicateBound, Text: `"p4"@[,"b"]`},
				{Type: ItemPredicateBound, Text: `"p4"@[,]`},
				{Type: ItemEOF}}},
		{`"p\"1"@[]`,
			[]Token{
				{Type: ItemPredicate, Text: `"p\"1"@[]`},
				{Type: ItemEOF}}},
		{`"p1"@]`,
			[]Token{
				{Type: ItemError,
					Text:         "",
					ErrorMessage: "[lexer:0:0] failed to parse predicate or literal for opening \" delimiter"},
				{Type: ItemEOF}}},
		{`"p1"@[,,]`,
			[]Token{
				{Type: ItemError,
					Text:         `"p1"@[,,]`,
					ErrorMessage: "[lexer:0:9] predicate bounds should only have one , to separate bounds"},
				{Type: ItemEOF}}},
		{`/room<000> "named"@[] "Hallway"^^type:text. /room<000> "connects_to"@[] /room<001>`,
			[]Token{
				{Type: ItemNode, Text: `/room<000>`},
				{Type: ItemPredicate, Text: `"named"@[]`},
				{Type: ItemLiteral, Text: `"Hallway"^^type:text`},
				{Type: ItemDot, Text: `.`},
				{Type: ItemNode, Text: `/room<000>`},
				{Type: ItemPredicate, Text: `"connects_to"@[]`},
				{Type: ItemNode, Text: `/room<001>`},
				{Type: ItemEOF}}},
		{`"Hallway\"1\""^^type:text`,
			[]Token{
				{Type: ItemLiteral, Text: `"Hallway\"1\""^^type:text`},
				{Type: ItemEOF}}},
	}

	for _, test := range table {
		_, c := lex(test.input, 0)
		idx := 0
		for got := range c {
			if idx >= len(test.tokens) {
				t.Fatalf("lex(%q) has not finished producing tokens when it should have.", test.input)
			}
			if want := test.tokens[idx]; got != want {
				t.Errorf("lex(%q) failed to provide %+v, got %+v instead", test.input, want, got)
			}
			idx++
		}
	}
}

func TestValidTokenQuery(t *testing.T) {
	table := []struct {
		input  string
		tokens []TokenType
	}{
		{"select ?s?p?o from ?foo where {?s?p?o};", []TokenType{
			ItemQuery, ItemBinding, ItemBinding, ItemBinding, ItemFrom, ItemBinding,
			ItemWhere, ItemLBracket, ItemBinding, ItemBinding, ItemBinding,
			ItemRBracket, ItemSemicolon, ItemEOF}},
		{`select ?s
		    from ?foo
		    where {
				  ?s "bar"@["123"] /_<foo> .
					?s "foo"@[] "1"^^type:int64
				};`, []TokenType{
			ItemQuery, ItemBinding, ItemFrom, ItemBinding, ItemWhere, ItemLBracket,
			ItemBinding, ItemPredicate, ItemNode, ItemDot, ItemBinding, ItemPredicate,
			ItemLiteral, ItemRBracket, ItemSemicolon, ItemEOF}},
		{`select count(?foo) as ?foo
		    from ?foo
		    where {
				  ?s "bar"@["123"] /_<foo> .
					?s "foo"@[] "1"^^type:int64
				}
				group by ?foo, ?foo
				order by ?foo asc desc
				having ?foo < ?foo and not ?foo or ?foo = ?foo
				limit "1"^^type:int64;`, []TokenType{
			ItemQuery, ItemCount, ItemLPar, ItemBinding, ItemRPar, ItemAs,
			ItemBinding, ItemFrom, ItemBinding, ItemWhere, ItemLBracket, ItemBinding,
			ItemPredicate, ItemNode, ItemDot, ItemBinding, ItemPredicate, ItemLiteral,
			ItemRBracket, ItemGroup, ItemBy, ItemBinding, ItemComma, ItemBinding,
			ItemOrder, ItemBy, ItemBinding, ItemAsc, ItemDesc, ItemHaving,
			ItemBinding, ItemLT, ItemBinding, ItemAnd, ItemNot, ItemBinding, ItemOr,
			ItemBinding, ItemEQ, ItemBinding, ItemLimit, ItemLiteral, ItemSemicolon,
			ItemEOF}},
		{`construct {?s "foo"@[] ?o} into ?a from ?b where {?s "foo"@[] ?o};`, []TokenType{
			ItemConstruct, ItemLBracket, ItemBinding, ItemPredicate, ItemBinding,
			ItemRBracket, ItemInto, ItemBinding, ItemFrom, ItemBinding, ItemWhere,
			ItemLBracket, ItemBinding, ItemPredicate, ItemBinding, ItemRBracket,
			ItemSemicolon, ItemEOF}},
		{`construct {_:v1 "predicate"@[] ?p.
		             _:v1 "object"@[] ?o} into ?a from ?b where {?s "foo"@[] ?o};`, []TokenType{
			ItemConstruct, ItemLBracket, ItemBlankNode, ItemPredicate, ItemBinding, ItemDot,
			ItemBlankNode, ItemPredicate, ItemBinding, ItemRBracket, ItemInto, ItemBinding,
			ItemFrom, ItemBinding, ItemWhere, ItemLBracket, ItemBinding, ItemPredicate,
			ItemBinding, ItemRBracket, ItemSemicolon, ItemEOF}},
	}
	for _, test := range table {
		_, c := lex(test.input, 0)
		idx := 0
		for got := range c {
			if idx >= len(test.tokens) {
				t.Fatalf("lex(%q) has not finished producing tokens when it should have.", test.input)
			}
			if want := test.tokens[idx]; got.Type != want {
				t.Errorf("lex(%q) failed to provide token %s; got %s instead", test.input, got, want)
			}
			idx++
		}
	}

}
