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

// Package grammar imlements the grammar parser for the BadWolf query language.
// The parser is impemented as a reusable recursive decent parser for a left
// LL(k) left factorized grammar. BQL is an LL(1) grammar however the parser
// is designed to be reusable and help separate the grammar from the parsing
// mechanics to improve maintainablity and flexibility of grammar changes
// by keeping those the code separation clearly delineated.
package grammar

import "github.com/google/badwolf/bql/lexer"

// BQL LL1 grammar.
var BQL = Grammar{
	"START": []Clause{
		{
			Elements: []Element{
				NewToken(lexer.ItemQuery),
				NewSymbol("VARS"),
				NewToken(lexer.ItemFrom),
				NewSymbol("GRAPHS"),
				NewToken(lexer.ItemSemicolon),
			},
		},
	},
	"VARS": []Clause{
		{
			Elements: []Element{
				NewToken(lexer.ItemBinding),
				NewSymbol("MORE_VARS"),
			},
		},
	},
	"MORE_VARS": []Clause{
		{
			Elements: []Element{
				NewToken(lexer.ItemComma),
				NewToken(lexer.ItemBinding),
				NewSymbol("MORE_VARS"),
			},
		},
		{},
	},
	"GRAPHS": []Clause{
		{
			Elements: []Element{
				NewToken(lexer.ItemBinding),
				NewSymbol("MORE_GRAPHS"),
			},
		},
	},
	"MORE_GRAPHS": []Clause{
		{
			Elements: []Element{
				NewToken(lexer.ItemComma),
				NewToken(lexer.ItemBinding),
				NewSymbol("MORE_GRAPHS"),
			},
		},
		{},
	},
}
