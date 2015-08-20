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

package semantic

import (
	"testing"

	"github.com/google/badwolf/bql/grammar"
	"github.com/google/badwolf/bql/lexer"
)

func TestToNode(t *testing.T) {
	// Consume a valid node token.
	tkn := &lexer.Token{
		Type: lexer.ItemNode,
		Text: "/_<foo>",
	}
	ce := grammar.NewConsumedToken(tkn)
	if n, err := ToNode(ce); err != nil || n.String() != "/_<foo>" {
		t.Errorf("semantic.ToNode failed to properly convert %+v; err=%v, node=%v", ce, err, n)
	}
	// Reject invalid tokens.
	tkn.Text = "/foo"
	ice := grammar.NewConsumedToken(tkn)
	if n, err := ToNode(ice); err == nil {
		t.Errorf("semantic.ToNode should have never produced node %v from invalid text %q", n, tkn.Text)
	}
	// Reject invalid token types.
	tkn.Type = lexer.ItemEOF
	nce := grammar.NewConsumedToken(tkn)
	if n, err := ToNode(nce); err == nil {
		t.Errorf("semantic.ToNode should have never produced node %v from invalid type %q", n, tkn.Type)
	}
}

func TestToPredicate(t *testing.T) {
	// Consume a valid node token.
	tkn := &lexer.Token{
		Type: lexer.ItemPredicate,
		Text: `"foo"@[]`,
	}
	ce := grammar.NewConsumedToken(tkn)
	if p, err := ToPredicate(ce); err != nil || p.String() != `"foo"@[]` {
		t.Errorf("semantic.ToPredicate failed to properly convert %+v; err=%v, predicate=%v", ce, err, p)
	}
	// Reject invalid tokens.
	tkn.Text = `"incomplete"@`
	ice := grammar.NewConsumedToken(tkn)
	if p, err := ToPredicate(ice); err == nil {
		t.Errorf("semantic.ToPredicate should have never produced predicate %v from invalid text %q", p, tkn.Text)
	}
	// Reject invalid token types.
	tkn.Type = lexer.ItemEOF
	nce := grammar.NewConsumedToken(tkn)
	if p, err := ToPredicate(nce); err == nil {
		t.Errorf("semantic.ToPredicate should have never produced predicate %v from invalid type %q", p, tkn.Type)
	}
}

func TestToLiteral(t *testing.T) {
	// Consume a valid node token.
	tkn := &lexer.Token{
		Type: lexer.ItemLiteral,
		Text: `"true"^^type:bool`,
	}
	ce := grammar.NewConsumedToken(tkn)
	if l, err := ToLiteral(ce); err != nil || l.String() != `"true"^^type:bool` {
		t.Errorf("semantic.ToLiteral failed to properly convert %+v; err=%v, literal=%v", ce, err, l)
	}
	// Reject invalid tokens.
	tkn.Text = `"incomplete"^^`
	ice := grammar.NewConsumedToken(tkn)
	if l, err := ToLiteral(ice); err == nil {
		t.Errorf("semantic.ToLiteral should have never produced literal %v from invalid text %q", l, tkn.Text)
	}
	// Reject invalid token types.
	tkn.Type = lexer.ItemEOF
	nce := grammar.NewConsumedToken(tkn)
	if l, err := ToLiteral(nce); err == nil {
		t.Errorf("semantic.ToLiteral should have never produced literal %v from invalid type %q", l, tkn.Type)
	}
}
