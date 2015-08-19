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
				NewSymbol("WHERE"),
				NewSymbol("GROUP_BY"),
				NewSymbol("ORDER_BY"),
				NewSymbol("HAVING"),
				NewSymbol("GLOBAL_TIME_BOUND"),
				NewSymbol("LIMIT"),
				NewToken(lexer.ItemSemicolon),
			},
		},
		{
			Elements: []Element{
				NewToken(lexer.ItemInsert),
				NewToken(lexer.ItemData),
				NewToken(lexer.ItemInto),
				NewSymbol("GRAPHS"),
				NewToken(lexer.ItemLBracket),
				NewToken(lexer.ItemNode),
				NewToken(lexer.ItemPredicate),
				NewSymbol("INSERT_OBJECT"),
				NewSymbol("INSERT_DATA"),
				NewToken(lexer.ItemRBracket),
				NewToken(lexer.ItemSemicolon),
			},
		},
		{
			Elements: []Element{
				NewToken(lexer.ItemDelete),
				NewToken(lexer.ItemData),
				NewToken(lexer.ItemFrom),
				NewSymbol("GRAPHS"),
				NewToken(lexer.ItemLBracket),
				NewToken(lexer.ItemNode),
				NewToken(lexer.ItemPredicate),
				NewSymbol("DELETE_OBJECT"),
				NewSymbol("DELETE_DATA"),
				NewToken(lexer.ItemRBracket),
				NewToken(lexer.ItemSemicolon),
			},
		},
	},
	"VARS": []Clause{
		{
			Elements: []Element{
				NewToken(lexer.ItemBinding),
				NewSymbol("VARS_AS"),
				NewSymbol("MORE_VARS"),
			},
		},
		{
			Elements: []Element{
				NewToken(lexer.ItemCount),
				NewToken(lexer.ItemLPar),
				NewSymbol("COUNT_DISTINCT"),
				NewToken(lexer.ItemBinding),
				NewToken(lexer.ItemRPar),
				NewToken(lexer.ItemAs),
				NewToken(lexer.ItemBinding),
				NewSymbol("MORE_VARS"),
			},
		},
		{
			Elements: []Element{
				NewToken(lexer.ItemSum),
				NewToken(lexer.ItemLPar),
				NewToken(lexer.ItemBinding),
				NewToken(lexer.ItemRPar),
				NewToken(lexer.ItemAs),
				NewToken(lexer.ItemBinding),
				NewSymbol("MORE_VARS"),
			},
		},
	},
	"COUNT_DISTINCT": []Clause{
		{
			Elements: []Element{
				NewToken(lexer.ItemDistinct),
			},
		},
		{},
	},
	"VARS_AS": []Clause{
		{
			Elements: []Element{
				NewToken(lexer.ItemAs),
				NewToken(lexer.ItemBinding),
			},
		},
		{},
	},
	"MORE_VARS": []Clause{
		{
			Elements: []Element{
				NewToken(lexer.ItemComma),
				NewSymbol("VARS"),
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
	"WHERE": []Clause{
		{
			Elements: []Element{
				NewToken(lexer.ItemWhere),
				NewToken(lexer.ItemLBracket),
				NewSymbol("CLAUSES"),
				NewToken(lexer.ItemRBracket),
			},
		},
	},
	"CLAUSES": []Clause{
		{
			Elements: []Element{
				NewToken(lexer.ItemNode),
				NewSymbol("SUBJECT_EXTRACT"),
				NewSymbol("PREDICATES"),
				NewSymbol("OBJECTS"),
				NewSymbol("NORE_CLAUSES"),
			},
		},
		{
			Elements: []Element{
				NewToken(lexer.ItemBinding),
				NewSymbol("SUBJECT_EXTRACT"),
				NewSymbol("PREDICATE"),
				NewSymbol("OBJECT"),
				NewSymbol("NORE_CLAUSES"),
			},
		},
	},
	"SUBJECT_EXTRACT": []Clause{
		{
			Elements: []Element{
				NewToken(lexer.ItemAs),
				NewToken(lexer.ItemBinding),
				NewSymbol("SUBJECT_TYPE"),
				NewSymbol("SUBJECT_ID"),
			},
		},
		{
			Elements: []Element{
				NewToken(lexer.ItemType),
				NewToken(lexer.ItemBinding),
				NewSymbol("SUBJECT_ID"),
			},
		},
		{
			Elements: []Element{
				NewToken(lexer.ItemID),
				NewToken(lexer.ItemBinding),
			},
		},
		{},
	},
	"SUBJECT_TYPE": []Clause{
		{
			Elements: []Element{
				NewToken(lexer.ItemType),
				NewToken(lexer.ItemBinding),
			},
		},
		{},
	},
	"SUBJECT_ID": []Clause{
		{
			Elements: []Element{
				NewToken(lexer.ItemID),
				NewToken(lexer.ItemBinding),
			},
		},
		{},
	},
	"PREDICATE": []Clause{
		{
			Elements: []Element{
				NewToken(lexer.ItemPredicate),
				NewSymbol("PREDICATE_AS"),
				NewSymbol("PREDICATE_ID"),
				NewSymbol("PREDICATE_AT"),
			},
		},
		{
			Elements: []Element{
				NewToken(lexer.ItemBinding),
				NewSymbol("PREDICATE_AS"),
				NewSymbol("PREDICATE_ID"),
				NewSymbol("PREDICATE_AT"),
			},
		},
	},
	"PREDICATE_AS": []Clause{
		{
			Elements: []Element{
				NewToken(lexer.ItemAs),
				NewToken(lexer.ItemBinding),
			},
		},
		{},
	},
	"PREDICATE_ID": []Clause{
		{
			Elements: []Element{
				NewToken(lexer.ItemID),
				NewToken(lexer.ItemBinding),
			},
		},
		{},
	},
	"PREDICATE_AT": []Clause{
		{
			Elements: []Element{
				NewToken(lexer.ItemAt),
				NewToken(lexer.ItemBinding),
			},
		},
		{},
	},
	"OBJECT": []Clause{
		{
			Elements: []Element{
				NewToken(lexer.ItemNode),
				NewSymbol("SUBJECT_EXTRACT"),
			},
		},
		{
			Elements: []Element{
				NewToken(lexer.ItemPredicate),
				NewSymbol("PREDICATE_AS"),
				NewSymbol("PREDICATE_ID"),
				NewSymbol("PREDICATE_AT"),
			},
		},
		{
			Elements: []Element{
				NewToken(lexer.ItemLiteral),
				NewSymbol("LITERAL_AS"),
			},
		},
		{
			Elements: []Element{
				NewToken(lexer.ItemBinding),
				NewSymbol("LITERAL_BINDING_AS"),
				NewSymbol("LITERAL_BINDING_TYPE"),
				NewSymbol("LITERAL_BINDING_ID"),
				NewSymbol("LITERAL_BINDING_AT"),
			},
		},
	},
	"LITERAL_AS": []Clause{
		{
			Elements: []Element{
				NewToken(lexer.ItemAs),
				NewToken(lexer.ItemBinding),
			},
		},
		{},
	},
	"LITERAL_BINDING_AS": []Clause{
		{
			Elements: []Element{
				NewToken(lexer.ItemAs),
				NewToken(lexer.ItemBinding),
			},
		},
		{},
	},
	"LITERAL_BINDING_TYPE": []Clause{
		{
			Elements: []Element{
				NewToken(lexer.ItemType),
				NewToken(lexer.ItemBinding),
			},
		},
		{},
	},
	"LITERAL_BINDING_ID": []Clause{
		{
			Elements: []Element{
				NewToken(lexer.ItemID),
				NewToken(lexer.ItemBinding),
			},
		},
		{},
	},
	"LITERAL_BINDING_AT": []Clause{
		{
			Elements: []Element{
				NewToken(lexer.ItemAt),
				NewToken(lexer.ItemBinding),
			},
		},
		{},
	},
	"NORE_CLAUSES": []Clause{
		{
			Elements: []Element{
				NewToken(lexer.ItemDot),
				NewSymbol("CLAUSES"),
			},
		},
		{},
	},
	"GROUP_BY": []Clause{
		{
			Elements: []Element{
				NewToken(lexer.ItemGroup),
				NewToken(lexer.ItemBy),
				NewToken(lexer.ItemBinding),
				NewSymbol("GROUP_BY_BINDINGS"),
			},
		},
		{},
	},
	"GROUP_BY_BINDINGS": []Clause{
		{
			Elements: []Element{
				NewToken(lexer.ItemComma),
				NewToken(lexer.ItemBinding),
				NewSymbol("GROUP_BY_BINDINGS"),
			},
		},
		{},
	},
	"ORDER_BY": []Clause{
		{
			Elements: []Element{
				NewToken(lexer.ItemOrder),
				NewToken(lexer.ItemBy),
				NewToken(lexer.ItemBinding),
				NewSymbol("ORDER_BY_DIRECTION"),
				NewSymbol("ORDER_BY_BINDINGS"),
			},
		},
		{},
	},
	"ORDER_BY_DIRECTION": []Clause{
		{
			Elements: []Element{
				NewToken(lexer.ItemAsc),
			},
		},
		{
			Elements: []Element{
				NewToken(lexer.ItemDesc),
			},
		},
		{},
	},
	"ORDER_BY_BINDINGS": []Clause{
		{
			Elements: []Element{
				NewToken(lexer.ItemComma),
				NewToken(lexer.ItemBinding),
				NewSymbol("ORDER_BY_DIRECTION"),
				NewSymbol("ORDER_BY_BINDINGS"),
			},
		},
		{},
	},
	"HAVING": []Clause{
		{
			Elements: []Element{
				NewToken(lexer.ItemHaving),
				NewSymbol("HAVING_CLAUSE"),
			},
		},
		{},
	},
	"HAVING_CLAUSE": []Clause{
		{
			Elements: []Element{
				NewToken(lexer.ItemBinding),
				NewSymbol("HAVING_CLAUSE_BINARY_COMPOSITE"),
			},
		},
		{
			Elements: []Element{
				NewToken(lexer.ItemNot),
				NewSymbol("HAVING_CLAUSE"),
			},
		},
		{
			Elements: []Element{
				NewToken(lexer.ItemLPar),
				NewSymbol("HAVING_CLAUSE"),
				NewToken(lexer.ItemRPar),
				NewSymbol("HAVING_CLAUSE_BINARY_COMPOSITE"),
			},
		},
	},
	"HAVING_CLAUSE_BINARY_COMPOSITE": []Clause{
		{
			Elements: []Element{
				NewToken(lexer.ItemAnd),
				NewSymbol("HAVING_CLAUSE"),
			},
		},
		{
			Elements: []Element{
				NewToken(lexer.ItemOr),
				NewSymbol("HAVING_CLAUSE"),
			},
		},
		{
			Elements: []Element{
				NewToken(lexer.ItemEQ),
				NewSymbol("HAVING_CLAUSE"),
			},
		},
		{
			Elements: []Element{
				NewToken(lexer.ItemLT),
				NewSymbol("HAVING_CLAUSE"),
			},
		},
		{
			Elements: []Element{
				NewToken(lexer.ItemGT),
				NewSymbol("HAVING_CLAUSE"),
			},
		},
		{},
	},
	"GLOBAL_TIME_BOUND": []Clause{
		{
			Elements: []Element{
				NewToken(lexer.ItemBefore),
				NewToken(lexer.ItemPredicate),
				NewSymbol("GLOBAL_TIME_BOUND_COMPOSITE"),
			},
		},
		{
			Elements: []Element{
				NewToken(lexer.ItemAfter),
				NewToken(lexer.ItemPredicate),
				NewSymbol("GLOBAL_TIME_BOUND_COMPOSITE"),
			},
		},
		{
			Elements: []Element{
				NewToken(lexer.ItemBetween),
				NewToken(lexer.ItemPredicate),
				NewToken(lexer.ItemComma),
				NewToken(lexer.ItemPredicate),
				NewSymbol("GLOBAL_TIME_BOUND_COMPOSITE"),
			},
		},
		{
			Elements: []Element{
				NewToken(lexer.ItemLPar),
				NewSymbol("GLOBAL_TIME_BOUND"),
				NewToken(lexer.ItemRPar),
			},
		},
		{},
	},
	"GLOBAL_TIME_BOUND_COMPOSITE": []Clause{
		{
			Elements: []Element{
				NewToken(lexer.ItemAnd),
				NewSymbol("GLOBAL_TIME_BOUND"),
			},
		},
		{
			Elements: []Element{
				NewToken(lexer.ItemOr),
				NewSymbol("GLOBAL_TIME_BOUND"),
			},
		},
		{},
	},
	"LIMIT": []Clause{
		{
			Elements: []Element{
				NewToken(lexer.ItemLimit),
				NewToken(lexer.ItemLiteral),
			},
		},
		{},
	},
	"INSERT_OBJECT": []Clause{
		{
			Elements: []Element{
				NewToken(lexer.ItemNode),
			},
		},
		{
			Elements: []Element{
				NewToken(lexer.ItemPredicate),
			},
		},
		{
			Elements: []Element{
				NewToken(lexer.ItemLiteral),
			},
		},
	},
	"INSERT_DATA": []Clause{
		{
			Elements: []Element{
				NewToken(lexer.ItemDot),
				NewToken(lexer.ItemNode),
				NewToken(lexer.ItemPredicate),
				NewSymbol("INSERT_OBJECT"),
				NewSymbol("INSERT_DATA"),
			},
		},
		{},
	},
	"DELETE_OBJECT": []Clause{
		{
			Elements: []Element{
				NewToken(lexer.ItemNode),
			},
		},
		{
			Elements: []Element{
				NewToken(lexer.ItemPredicate),
			},
		},
		{
			Elements: []Element{
				NewToken(lexer.ItemLiteral),
			},
		},
	},
	"DELETE_DATA": []Clause{
		{
			Elements: []Element{
				NewToken(lexer.ItemDot),
				NewToken(lexer.ItemNode),
				NewToken(lexer.ItemPredicate),
				NewSymbol("DELETE_OBJECT"),
				NewSymbol("DELETE_DATA"),
			},
		},
		{},
	},
}
