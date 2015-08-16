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
