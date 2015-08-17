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

func TestIndividualTokens(t *testing.T) {
	table := []struct {
		input  string
		tokens []Token
	}{
		{"", []Token{
			{Type: ItemEOF}}},
		{"{}().;,<>=", []Token{
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
		{"?foo ?bar", []Token{
			{Type: ItemBinding, Text: "?foo"},
			{Type: ItemBinding, Text: "?bar"},
			{Type: ItemEOF}}},
		{`SeLeCt FrOm WhErE As BeFoRe AfTeR BeTwEeN CoUnT SuM GrOuP bY HaViNg LiMiT
		  OrDeR AsC DeSc NoT AnD Or Id TyPe At`,
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
				{Type: ItemEOF}}},
		{"/_<foo>/_<bar>", []Token{
			{Type: ItemNode, Text: "/_<foo>"},
			{Type: ItemNode, Text: "/_<bar>"},
			{Type: ItemEOF}}},
		{"/_<foo>/_\\<bar>", []Token{
			{Type: ItemNode, Text: "/_<foo>"},
			{Type: ItemError, Text: "/_\\<bar>",
				ErrorMessage: "[lexer:0:15] node should start ID section with a < delimiter"},
			{Type: ItemEOF}}},
		{"/_foo>", []Token{
			{Type: ItemError, Text: "/_foo>",
				ErrorMessage: "[lexer:0:6] node should start ID section with a < delimiter"},
			{Type: ItemEOF}}},
		{"/_<foo", []Token{
			{Type: ItemError, Text: "/_<foo",
				ErrorMessage: "[lexer:0:6] node is not properly terminated; missing final > delimiter"},
			{Type: ItemEOF}}},
		{"\"true\"^^type:bool \"1\"^^type:int64\"2\"^^type:float64\"t\"^^type:text",
			[]Token{
				{Type: ItemLiteral, Text: "\"true\"^^type:bool"},
				{Type: ItemLiteral, Text: "\"1\"^^type:int64"},
				{Type: ItemLiteral, Text: "\"2\"^^type:float64"},
				{Type: ItemLiteral, Text: "\"t\"^^type:text"},
				{Type: ItemEOF}}},
		{"\"[1 2 3 4]\"^^type:blob", []Token{
			{Type: ItemLiteral, Text: "\"[1 2 3 4]\"^^type:blob"},
			{Type: ItemEOF}}},
		{"\"1\"^type:int64", []Token{
			{Type: ItemError,
				ErrorMessage: "[lexer:0:0] failed to parse predicate or literal for opening \" delimiter"},
			{Type: ItemEOF}}},
		{"\"1\"^^type:int32", []Token{
			{Type: ItemError,
				Text:         "\"1\"^^type:int32",
				ErrorMessage: "[lexer:0:15] invalid literal type int32"},
			{Type: ItemEOF}}},
		{"\"p1\"@[] \"p2\"@[\"some data\"]\"p3\"@[\"some data\"]", []Token{
			{Type: ItemPredicate, Text: "\"p1\"@[]"},
			{Type: ItemPredicate, Text: "\"p2\"@[\"some data\"]"},
			{Type: ItemPredicate, Text: "\"p3\"@[\"some data\"]"},
			{Type: ItemEOF}}},
		{"\"p1\"@]", []Token{
			{Type: ItemError,
				Text:         "",
				ErrorMessage: "[lexer:0:0] failed to parse predicate or literal for opening \" delimiter"},
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
