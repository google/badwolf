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

import (
	"fmt"
	"testing"
)

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
				ErrorMessage: "node should start ID section with a < delimiter"},
			{Type: ItemEOF}}},
		{"/_foo>", []Token{
			{Type: ItemError, Text: "/_foo>",
				ErrorMessage: "node should start ID section with a < delimiter"},
			{Type: ItemEOF}}},
		{"/_<foo", []Token{
			{Type: ItemError, Text: "/_<foo",
				ErrorMessage: "node is not properly terminated; missing final > delimiter"},
			{Type: ItemEOF}}},
	}

	for _, test := range table {
		_, c := lex(test.input)
		idx := 0
		for got := range c {
			fmt.Printf("%v\n", got)
			if want := test.tokens[idx]; got != want {
				t.Errorf("lex(%q) failed to provide %v, got %v instead", test.input, want, got)
			}
			idx++
			if idx > len(test.tokens) {
				t.Fatalf("lex(%q) has not finished producing tokens when it should have.", test.input)
			}
		}
	}
}
