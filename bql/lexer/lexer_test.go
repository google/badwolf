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

func TestEmpty(t *testing.T) {
	table := []struct {
		input  string
		tokens []Token
	}{
		{"", []Token{
			{Type: ItemEOF}}},
		{"{}().;", []Token{
			{Type: ItemLBracket, Text: "{"},
			{Type: ItemRBracket, Text: "}"},
			{Type: ItemLPar, Text: "("},
			{Type: ItemRPar, Text: ")"},
			{Type: ItemDot, Text: "."},
			{Type: ItemSemicolon, Text: ";"},
			{Type: ItemEOF}}},
		{"?foo ?bar", []Token{
			{Type: ItemBinding, Text: "?foo"},
			{Type: ItemBinding, Text: "?bar"},
			{Type: ItemEOF}}},
		{"SeLeCt FrOm WhErE As BeFoRe AfTeR BeTwEeN", []Token{
			{Type: ItemQuery, Text: "SeLeCt"},
			{Type: ItemFrom, Text: "FrOm"},
			{Type: ItemWhere, Text: "WhErE"},
			{Type: ItemAs, Text: "As"},
			{Type: ItemBefore, Text: "BeFoRe"},
			{Type: ItemAfter, Text: "AfTeR"},
			{Type: ItemBetween, Text: "BeTwEeN"},
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
		_, c := lex(test.input)
		idx := 0
		for got := range c {
			if want := test.tokens[idx]; got != want {
				t.Errorf("lex(%q) failed to provide %+v, got %+v instead", test.input, want, got)
			}
			idx++
			if idx > len(test.tokens) {
				t.Fatalf("lex(%q) has not finished producing tokens when it should have.", test.input)
			}
		}
	}
}
