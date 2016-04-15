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
	"testing"

	"github.com/google/badwolf/bql/lexer"
)

func TestEmptyImputLLk(t *testing.T) {
	const k = 10
	l := NewLLk("", k)
	if l.Current().Type != lexer.ItemEOF {
		t.Errorf("LLk.Current: should always return ItemEOF tokens if empty string is provided")
	}
	if _, err := l.Peek(0); err == nil {
		t.Errorf("LLk.Peek: should always fail when lookahed == 0")
	}
	if _, err := l.Peek(k + 1); err == nil {
		t.Errorf("LLk.Peek: should always fail when lookahed beyond k; %d > %d", k+1, k)
	}
	for i := 1; i <= k; i++ {
		if _, err := l.Peek(i); err != nil {
			t.Errorf("LLk.Peek: should always succed to lookahed for %d < %d", i, k)
		}
	}
	if !l.CanAccept(lexer.ItemEOF) {
		t.Errorf("LLk.CanAccept: should accept ItemEOF token at the end of the imput")
	}
	if !l.Consume(lexer.ItemEOF) {
		t.Errorf("LLk.Consume: should consume ItemEOF token at the end of the imput")
	}
}

func TestNonEmptyInputLLk(t *testing.T) {
	l := NewLLk("select ;", 1)
	if l.Current().Type != lexer.ItemQuery {
		t.Errorf("LLk.Current: should have return ItemQuery as the current token")
	}
	if tkn, err := l.Peek(1); err != nil || tkn.Type != lexer.ItemSemicolon {
		t.Errorf("LLk.Peek(1): should return ItemSemicolon as the current token instead of %s", tkn.Type)
	}
	if !l.CanAccept(lexer.ItemQuery) {
		t.Errorf("LLk.CanAccept: should accept ItemQuery token")
	}
	if !l.Consume(lexer.ItemQuery) {
		t.Errorf("LLk.Consume: should consume ItemQuery token")
	}
	if l.Current().Type != lexer.ItemSemicolon {
		t.Errorf("LLk.Current: should have return ItemSemicolon as the current token")
	}
	if tkn, err := l.Peek(1); err != nil || tkn.Type != lexer.ItemEOF {
		t.Errorf("LLk.Peek(1): should return ItemEOF at the end of input instead of %s", tkn.Type)
	}
}

// Issue 39 (https://github.com/google/badwolf/issues/39)
func TestTripleInputLLK(t *testing.T) {
	triple := `/room<000> "named"@[] "Hallway"^^type:text`
	l := NewLLk(triple, 1)
	if tkn := l.Current(); tkn.Type != lexer.ItemNode || tkn.Text != "/room<000>" {
		t.Errorf("LLk.Current: should always return ItemNode token with text /room<000> but got %v", tkn)
	}
	l.Consume(l.Current().Type)
	if tkn := l.Current(); tkn.Type != lexer.ItemPredicate || tkn.Text != `"named"@[]` {
		t.Errorf(`LLk.Current: should always return ItemPredicate token with text "named"@[] but got %v`, tkn)
	}
	l.Consume(l.Current().Type)
	if tkn := l.Current(); tkn.Type != lexer.ItemLiteral || tkn.Text != `"Hallway"^^type:text` {
		t.Errorf(`LLk.Current: should always return ItemNode token with text Hallway"^^type:text but got %v`, tkn)
	}
}

// Issue 39 (https://github.com/google/badwolf/issues/39)
func TestStatementInputLLK(t *testing.T) {
	statement := `
		create graph ?world;

		insert data into ?world {
		  /room<000> "named"@[] "Hallway"^^type:text.
		  /room<000> "connects_to"@[] /room<001>
		};`
	wantTokens := []lexer.Token{
		{
			Type: lexer.ItemCreate,
			Text: "create",
		},
		{
			Type: lexer.ItemGraph,
			Text: "graph",
		},
		{
			Type: lexer.ItemBinding,
			Text: "?world",
		},
		{
			Type: lexer.ItemSemicolon,
			Text: ";",
		},
		{
			Type: lexer.ItemInsert,
			Text: "insert",
		},
		{
			Type: lexer.ItemData,
			Text: "data",
		},
		{
			Type: lexer.ItemInto,
			Text: "into",
		},
		{
			Type: lexer.ItemBinding,
			Text: "?world",
		},
		{
			Type: lexer.ItemLBracket,
			Text: "{",
		},
		{
			Type: lexer.ItemNode,
			Text: "/room<000>",
		},
		{
			Type: lexer.ItemPredicate,
			Text: `"named"@[]`,
		},
		{
			Type: lexer.ItemLiteral,
			Text: `"Hallway"^^type:text`,
		},
		{
			Type: lexer.ItemDot,
			Text: ".",
		},
		{
			Type: lexer.ItemNode,
			Text: "/room<000>",
		},
		{
			Type: lexer.ItemPredicate,
			Text: `"connects_to"@[]`,
		},
		{
			Type: lexer.ItemNode,
			Text: "/room<001>",
		},
		{
			Type: lexer.ItemRBracket,
			Text: "}",
		},
		{
			Type: lexer.ItemSemicolon,
			Text: ";",
		},
	}
	l := NewLLk(statement, 1)
	for _, want := range wantTokens {
		if tkn := l.Current(); tkn.Type != want.Type || tkn.Text != want.Text {
			t.Errorf("LLk.Current: Found the wrong tokent; want %v, got %v", tkn, want)
		}
		l.Consume(l.Current().Type)
	}
}
