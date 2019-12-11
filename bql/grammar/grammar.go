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

import (
	"github.com/google/badwolf/bql/lexer"
	"github.com/google/badwolf/bql/semantic"
)

// BQL LL1 grammar.
func BQL() *Grammar {
	return &Grammar{
		"START": []*Clause{
			{
				Elements: []Element{
					NewTokenType(lexer.ItemQuery),
					NewSymbol("VARS"),
					NewTokenType(lexer.ItemFrom),
					NewSymbol("INPUT_GRAPHS"),
					NewSymbol("WHERE"),
					NewSymbol("GROUP_BY"),
					NewSymbol("ORDER_BY"),
					NewSymbol("HAVING"),
					NewSymbol("GLOBAL_TIME_BOUND"),
					NewSymbol("LIMIT"),
					NewTokenType(lexer.ItemSemicolon),
				},
			},
			{
				Elements: []Element{
					NewTokenType(lexer.ItemInsert),
					NewTokenType(lexer.ItemData),
					NewTokenType(lexer.ItemInto),
					NewSymbol("OUTPUT_GRAPHS"),
					NewTokenType(lexer.ItemLBracket),
					NewTokenType(lexer.ItemNode),
					NewTokenType(lexer.ItemPredicate),
					NewSymbol("INSERT_OBJECT"),
					NewSymbol("INSERT_DATA"),
					NewTokenType(lexer.ItemRBracket),
					NewTokenType(lexer.ItemSemicolon),
				},
			},
			{
				Elements: []Element{
					NewTokenType(lexer.ItemDelete),
					NewTokenType(lexer.ItemData),
					NewTokenType(lexer.ItemFrom),
					NewSymbol("INPUT_GRAPHS"),
					NewTokenType(lexer.ItemLBracket),
					NewTokenType(lexer.ItemNode),
					NewTokenType(lexer.ItemPredicate),
					NewSymbol("DELETE_OBJECT"),
					NewSymbol("DELETE_DATA"),
					NewTokenType(lexer.ItemRBracket),
					NewTokenType(lexer.ItemSemicolon),
				},
			},
			{
				Elements: []Element{
					NewTokenType(lexer.ItemCreate),
					NewSymbol("CREATE_GRAPHS"),
					NewTokenType(lexer.ItemSemicolon),
				},
			},
			{
				Elements: []Element{
					NewTokenType(lexer.ItemDrop),
					NewSymbol("DROP_GRAPHS"),
					NewTokenType(lexer.ItemSemicolon),
				},
			},
			{
				Elements: []Element{
					NewTokenType(lexer.ItemConstruct),
					NewSymbol("CONSTRUCT_FACTS"),
					NewTokenType(lexer.ItemInto),
					NewSymbol("OUTPUT_GRAPHS"),
					NewTokenType(lexer.ItemFrom),
					NewSymbol("INPUT_GRAPHS"),
					NewSymbol("WHERE"),
					NewSymbol("HAVING"),
					NewTokenType(lexer.ItemSemicolon),
				},
			},
			{
				Elements: []Element{
					NewTokenType(lexer.ItemDeconstruct),
					NewSymbol("DECONSTRUCT_FACTS"),
					NewTokenType(lexer.ItemIn),
					NewSymbol("OUTPUT_GRAPHS"),
					NewTokenType(lexer.ItemFrom),
					NewSymbol("INPUT_GRAPHS"),
					NewSymbol("WHERE"),
					NewSymbol("HAVING"),
					NewTokenType(lexer.ItemSemicolon),
				},
			},
			{
				Elements: []Element{
					NewTokenType(lexer.ItemShow),
					NewSymbol("GRAPH_SHOW"),
					NewTokenType(lexer.ItemSemicolon),
				},
			},
		},
		"CREATE_GRAPHS": []*Clause{
			{
				Elements: []Element{
					NewTokenType(lexer.ItemGraph),
					NewSymbol("GRAPHS"),
				},
			},
		},
		"DROP_GRAPHS": []*Clause{
			{
				Elements: []Element{
					NewTokenType(lexer.ItemGraph),
					NewSymbol("GRAPHS"),
				},
			},
		},
		"VARS": []*Clause{
			{
				Elements: []Element{
					NewTokenType(lexer.ItemBinding),
					NewSymbol("VARS_AS"),
					NewSymbol("MORE_VARS"),
				},
			},
			{
				Elements: []Element{
					NewTokenType(lexer.ItemCount),
					NewTokenType(lexer.ItemLPar),
					NewSymbol("COUNT_DISTINCT"),
					NewTokenType(lexer.ItemBinding),
					NewTokenType(lexer.ItemRPar),
					NewTokenType(lexer.ItemAs),
					NewTokenType(lexer.ItemBinding),
					NewSymbol("MORE_VARS"),
				},
			},
			{
				Elements: []Element{
					NewTokenType(lexer.ItemSum),
					NewTokenType(lexer.ItemLPar),
					NewTokenType(lexer.ItemBinding),
					NewTokenType(lexer.ItemRPar),
					NewTokenType(lexer.ItemAs),
					NewTokenType(lexer.ItemBinding),
					NewSymbol("MORE_VARS"),
				},
			},
		},
		"COUNT_DISTINCT": []*Clause{
			{
				Elements: []Element{
					NewTokenType(lexer.ItemDistinct),
				},
			},
			{},
		},
		"VARS_AS": []*Clause{
			{
				Elements: []Element{
					NewTokenType(lexer.ItemAs),
					NewTokenType(lexer.ItemBinding),
				},
			},
			{},
		},
		"MORE_VARS": []*Clause{
			{
				Elements: []Element{
					NewTokenType(lexer.ItemComma),
					NewSymbol("VARS"),
				},
			},
			{},
		},
		"GRAPHS": []*Clause{
			{
				Elements: []Element{
					NewTokenType(lexer.ItemBinding),
					NewSymbol("MORE_GRAPHS"),
				},
			},
		},
		"MORE_GRAPHS": []*Clause{
			{
				Elements: []Element{
					NewTokenType(lexer.ItemComma),
					NewTokenType(lexer.ItemBinding),
					NewSymbol("MORE_GRAPHS"),
				},
			},
			{},
		},
		"INPUT_GRAPHS": []*Clause{
			{
				Elements: []Element{
					NewTokenType(lexer.ItemBinding),
					NewSymbol("MORE_INPUT_GRAPHS"),
				},
			},
		},
		"MORE_INPUT_GRAPHS": []*Clause{
			{
				Elements: []Element{
					NewTokenType(lexer.ItemComma),
					NewTokenType(lexer.ItemBinding),
					NewSymbol("MORE_INPUT_GRAPHS"),
				},
			},
			{},
		},
		"OUTPUT_GRAPHS": []*Clause{
			{
				Elements: []Element{
					NewTokenType(lexer.ItemBinding),
					NewSymbol("MORE_OUTPUT_GRAPHS"),
				},
			},
		},
		"MORE_OUTPUT_GRAPHS": []*Clause{
			{
				Elements: []Element{
					NewTokenType(lexer.ItemComma),
					NewTokenType(lexer.ItemBinding),
					NewSymbol("MORE_OUTPUT_GRAPHS"),
				},
			},
			{},
		},
		"WHERE": []*Clause{
			{
				Elements: []Element{
					NewTokenType(lexer.ItemWhere),
					NewTokenType(lexer.ItemLBracket),
					NewSymbol("FIRST_CLAUSE"),
					NewTokenType(lexer.ItemRBracket),
				},
			},
		},
		"FIRST_CLAUSE": []*Clause{
			{
				Elements: []Element{
					NewTokenType(lexer.ItemNode),
					NewSymbol("SUBJECT_EXTRACT"),
					NewSymbol("PREDICATE"),
					NewSymbol("OBJECT"),
					NewSymbol("MORE_CLAUSES"),
				},
			},
			{
				Elements: []Element{
					NewTokenType(lexer.ItemBinding),
					NewSymbol("SUBJECT_EXTRACT"),
					NewSymbol("PREDICATE"),
					NewSymbol("OBJECT"),
					NewSymbol("MORE_CLAUSES"),
				},
			},
		},
		"MORE_CLAUSES": []*Clause{
			{
				Elements: []Element{
					NewTokenType(lexer.ItemDot),
					NewSymbol("CLAUSES"),
				},
			},
			{},
		},
		"CLAUSES": []*Clause{
			{
				Elements: []Element{
					NewTokenType(lexer.ItemOptional),
					NewTokenType(lexer.ItemLBracket),
					NewSymbol("OPTIONAL_CLAUSE"),
					NewTokenType(lexer.ItemRBracket),
					NewSymbol("MORE_CLAUSES"),
				},
			},
			{
				Elements: []Element{
					NewTokenType(lexer.ItemNode),
					NewSymbol("SUBJECT_EXTRACT"),
					NewSymbol("PREDICATE"),
					NewSymbol("OBJECT"),
					NewSymbol("MORE_CLAUSES"),
				},
			},
			{
				Elements: []Element{
					NewTokenType(lexer.ItemBinding),
					NewSymbol("SUBJECT_EXTRACT"),
					NewSymbol("PREDICATE"),
					NewSymbol("OBJECT"),
					NewSymbol("MORE_CLAUSES"),
				},
			},
		},
		"OPTIONAL_CLAUSE": []*Clause{
			{
				Elements: []Element{
					NewTokenType(lexer.ItemNode),
					NewSymbol("SUBJECT_EXTRACT"),
					NewSymbol("PREDICATE"),
					NewSymbol("OBJECT"),
				},
			},
			{
				Elements: []Element{
					NewTokenType(lexer.ItemBinding),
					NewSymbol("SUBJECT_EXTRACT"),
					NewSymbol("PREDICATE"),
					NewSymbol("OBJECT"),
				},
			},
		},
		"SUBJECT_EXTRACT": []*Clause{
			{
				Elements: []Element{
					NewTokenType(lexer.ItemAs),
					NewTokenType(lexer.ItemBinding),
					NewSymbol("SUBJECT_TYPE"),
					NewSymbol("SUBJECT_ID"),
				},
			},
			{
				Elements: []Element{
					NewTokenType(lexer.ItemType),
					NewTokenType(lexer.ItemBinding),
					NewSymbol("SUBJECT_ID"),
				},
			},
			{
				Elements: []Element{
					NewTokenType(lexer.ItemID),
					NewTokenType(lexer.ItemBinding),
				},
			},
			{},
		},
		"SUBJECT_TYPE": []*Clause{
			{
				Elements: []Element{
					NewTokenType(lexer.ItemType),
					NewTokenType(lexer.ItemBinding),
				},
			},
			{},
		},
		"SUBJECT_ID": []*Clause{
			{
				Elements: []Element{
					NewTokenType(lexer.ItemID),
					NewTokenType(lexer.ItemBinding),
				},
			},
			{},
		},
		"PREDICATE": []*Clause{
			{
				Elements: []Element{
					NewTokenType(lexer.ItemPredicate),
					NewSymbol("PREDICATE_AS"),
					NewSymbol("PREDICATE_ID"),
					NewSymbol("PREDICATE_AT"),
				},
			},
			{
				Elements: []Element{
					NewTokenType(lexer.ItemPredicateBound),
					NewSymbol("PREDICATE_AS"),
					NewSymbol("PREDICATE_ID"),
					NewSymbol("PREDICATE_BOUND_AT"),
				},
			},
			{
				Elements: []Element{
					NewTokenType(lexer.ItemBinding),
					NewSymbol("PREDICATE_AS"),
					NewSymbol("PREDICATE_ID"),
					NewSymbol("PREDICATE_AT"),
				},
			},
		},
		"PREDICATE_AS": []*Clause{
			{
				Elements: []Element{
					NewTokenType(lexer.ItemAs),
					NewTokenType(lexer.ItemBinding),
				},
			},
			{},
		},
		"PREDICATE_ID": []*Clause{
			{
				Elements: []Element{
					NewTokenType(lexer.ItemID),
					NewTokenType(lexer.ItemBinding),
				},
			},
			{},
		},
		"PREDICATE_AT": []*Clause{
			{
				Elements: []Element{
					NewTokenType(lexer.ItemAt),
					NewTokenType(lexer.ItemBinding),
				},
			},
			{},
		},
		"PREDICATE_BOUND_AT": []*Clause{
			{
				Elements: []Element{
					NewTokenType(lexer.ItemAt),
					NewSymbol("PREDICATE_BOUND_AT_BINDINGS"),
				},
			},
			{},
		},
		"PREDICATE_BOUND_AT_BINDINGS": []*Clause{
			{
				Elements: []Element{
					NewTokenType(lexer.ItemBinding),
					NewSymbol("PREDICATE_BOUND_AT_BINDINGS_END"),
				},
			},
			{},
		},
		"PREDICATE_BOUND_AT_BINDINGS_END": []*Clause{
			{
				Elements: []Element{
					NewTokenType(lexer.ItemComma),
					NewTokenType(lexer.ItemBinding),
				},
			},
			{},
		},
		"OBJECT": []*Clause{
			{
				Elements: []Element{
					NewTokenType(lexer.ItemLiteral),
					NewSymbol("OBJECT_LITERAL_AS"),
				},
			},
			{
				Elements: []Element{
					NewTokenType(lexer.ItemNode),
					NewSymbol("OBJECT_SUBJECT_EXTRACT"),
				},
			},
			{
				Elements: []Element{
					NewTokenType(lexer.ItemPredicate),
					NewSymbol("OBJECT_PREDICATE_AS"),
					NewSymbol("OBJECT_PREDICATE_ID"),
					NewSymbol("OBJECT_PREDICATE_AT"),
				},
			},
			{
				Elements: []Element{
					NewTokenType(lexer.ItemPredicateBound),
					NewSymbol("OBJECT_PREDICATE_AS"),
					NewSymbol("OBJECT_PREDICATE_ID"),
					NewSymbol("OBJECT_PREDICATE_BOUND_AT"),
				},
			},
			{
				Elements: []Element{
					NewTokenType(lexer.ItemBinding),
					NewSymbol("OBJECT_LITERAL_BINDING_AS"),
					NewSymbol("OBJECT_LITERAL_BINDING_TYPE"),
					NewSymbol("OBJECT_LITERAL_BINDING_ID"),
					NewSymbol("OBJECT_LITERAL_BINDING_AT"),
				},
			},
		},
		"OBJECT_SUBJECT_EXTRACT": []*Clause{
			{
				Elements: []Element{
					NewTokenType(lexer.ItemAs),
					NewTokenType(lexer.ItemBinding),
					NewSymbol("OBJECT_SUBJECT_TYPE"),
					NewSymbol("OBJECT_SUBJECT_ID"),
				},
			},
			{
				Elements: []Element{
					NewTokenType(lexer.ItemType),
					NewTokenType(lexer.ItemBinding),
					NewSymbol("OBJECT_SUBJECT_ID"),
				},
			},
			{
				Elements: []Element{
					NewTokenType(lexer.ItemID),
					NewTokenType(lexer.ItemBinding),
				},
			},
			{},
		},
		"OBJECT_SUBJECT_TYPE": []*Clause{
			{
				Elements: []Element{
					NewTokenType(lexer.ItemType),
					NewTokenType(lexer.ItemBinding),
				},
			},
			{},
		},
		"OBJECT_SUBJECT_ID": []*Clause{
			{
				Elements: []Element{
					NewTokenType(lexer.ItemID),
					NewTokenType(lexer.ItemBinding),
				},
			},
			{},
		},
		"OBJECT_PREDICATE_AS": []*Clause{
			{
				Elements: []Element{
					NewTokenType(lexer.ItemAs),
					NewTokenType(lexer.ItemBinding),
				},
			},
			{},
		},
		"OBJECT_PREDICATE_ID": []*Clause{
			{
				Elements: []Element{
					NewTokenType(lexer.ItemID),
					NewTokenType(lexer.ItemBinding),
				},
			},
			{},
		},
		"OBJECT_PREDICATE_AT": []*Clause{
			{
				Elements: []Element{
					NewTokenType(lexer.ItemAt),
					NewTokenType(lexer.ItemBinding),
				},
			},
			{},
		},
		"OBJECT_PREDICATE_BOUND_AT": []*Clause{
			{
				Elements: []Element{
					NewTokenType(lexer.ItemAt),
					NewSymbol("OBJECT_PREDICATE_BOUND_AT_BINDINGS"),
				},
			},
			{},
		},
		"OBJECT_PREDICATE_BOUND_AT_BINDINGS": []*Clause{
			{
				Elements: []Element{
					NewTokenType(lexer.ItemBinding),
					NewSymbol("OBJECT_PREDICATE_BOUND_AT_BINDINGS_END"),
				},
			},
			{},
		},
		"OBJECT_PREDICATE_BOUND_AT_BINDINGS_END": []*Clause{
			{
				Elements: []Element{
					NewTokenType(lexer.ItemComma),
					NewTokenType(lexer.ItemBinding),
				},
			},
			{},
		},
		"OBJECT_LITERAL_AS": []*Clause{
			{
				Elements: []Element{
					NewTokenType(lexer.ItemAs),
					NewTokenType(lexer.ItemBinding),
				},
			},
			{},
		},
		"OBJECT_LITERAL_BINDING_AS": []*Clause{
			{
				Elements: []Element{
					NewTokenType(lexer.ItemAs),
					NewTokenType(lexer.ItemBinding),
				},
			},
			{},
		},
		"OBJECT_LITERAL_BINDING_TYPE": []*Clause{
			{
				Elements: []Element{
					NewTokenType(lexer.ItemType),
					NewTokenType(lexer.ItemBinding),
				},
			},
			{},
		},
		"OBJECT_LITERAL_BINDING_ID": []*Clause{
			{
				Elements: []Element{
					NewTokenType(lexer.ItemID),
					NewTokenType(lexer.ItemBinding),
				},
			},
			{},
		},
		"OBJECT_LITERAL_BINDING_AT": []*Clause{
			{
				Elements: []Element{
					NewTokenType(lexer.ItemAt),
					NewTokenType(lexer.ItemBinding),
				},
			},
			{},
		},
		"GROUP_BY": []*Clause{
			{
				Elements: []Element{
					NewTokenType(lexer.ItemGroup),
					NewTokenType(lexer.ItemBy),
					NewTokenType(lexer.ItemBinding),
					NewSymbol("GROUP_BY_BINDINGS"),
				},
			},
			{},
		},
		"GROUP_BY_BINDINGS": []*Clause{
			{
				Elements: []Element{
					NewTokenType(lexer.ItemComma),
					NewTokenType(lexer.ItemBinding),
					NewSymbol("GROUP_BY_BINDINGS"),
				},
			},
			{},
		},
		"ORDER_BY": []*Clause{
			{
				Elements: []Element{
					NewTokenType(lexer.ItemOrder),
					NewTokenType(lexer.ItemBy),
					NewTokenType(lexer.ItemBinding),
					NewSymbol("ORDER_BY_DIRECTION"),
					NewSymbol("ORDER_BY_BINDINGS"),
				},
			},
			{},
		},
		"ORDER_BY_DIRECTION": []*Clause{
			{
				Elements: []Element{
					NewTokenType(lexer.ItemAsc),
				},
			},
			{
				Elements: []Element{
					NewTokenType(lexer.ItemDesc),
				},
			},
			{},
		},
		"ORDER_BY_BINDINGS": []*Clause{
			{
				Elements: []Element{
					NewTokenType(lexer.ItemComma),
					NewTokenType(lexer.ItemBinding),
					NewSymbol("ORDER_BY_DIRECTION"),
					NewSymbol("ORDER_BY_BINDINGS"),
				},
			},
			{},
		},
		"HAVING": []*Clause{
			{
				Elements: []Element{
					NewTokenType(lexer.ItemHaving),
					NewSymbol("HAVING_CLAUSE"),
				},
			},
			{},
		},
		"HAVING_CLAUSE": []*Clause{
			{
				Elements: []Element{
					NewTokenType(lexer.ItemBinding),
					NewSymbol("HAVING_CLAUSE_BINARY_COMPOSITE"),
				},
			},
			{
				Elements: []Element{
					NewTokenType(lexer.ItemNot),
					NewSymbol("HAVING_CLAUSE"),
				},
			},
			{
				Elements: []Element{
					NewTokenType(lexer.ItemLPar),
					NewSymbol("HAVING_CLAUSE"),
					NewTokenType(lexer.ItemRPar),
					NewSymbol("HAVING_CLAUSE_BINARY_COMPOSITE"),
				},
			},
		},
		"HAVING_CLAUSE_BINARY_COMPOSITE": []*Clause{
			{
				Elements: []Element{
					NewTokenType(lexer.ItemAnd),
					NewSymbol("HAVING_CLAUSE"),
				},
			},
			{
				Elements: []Element{
					NewTokenType(lexer.ItemOr),
					NewSymbol("HAVING_CLAUSE"),
				},
			},
			{
				Elements: []Element{
					NewTokenType(lexer.ItemEQ),
					NewSymbol("HAVING_CLAUSE"),
				},
			},
			{
				Elements: []Element{
					NewTokenType(lexer.ItemLT),
					NewSymbol("HAVING_CLAUSE"),
				},
			},
			{
				Elements: []Element{
					NewTokenType(lexer.ItemGT),
					NewSymbol("HAVING_CLAUSE"),
				},
			},
			{},
		},
		"GLOBAL_TIME_BOUND": []*Clause{
			{
				Elements: []Element{
					NewTokenType(lexer.ItemBefore),
					NewTokenType(lexer.ItemPredicate),
				},
			},
			{
				Elements: []Element{
					NewTokenType(lexer.ItemAfter),
					NewTokenType(lexer.ItemPredicate),
				},
			},
			{
				Elements: []Element{
					NewTokenType(lexer.ItemBetween),
					NewTokenType(lexer.ItemPredicateBound),
				},
			},
			{},
		},
		"LIMIT": []*Clause{
			{
				Elements: []Element{
					NewTokenType(lexer.ItemLimit),
					NewTokenType(lexer.ItemLiteral),
				},
			},
			{},
		},
		"INSERT_OBJECT": []*Clause{
			{
				Elements: []Element{
					NewTokenType(lexer.ItemNode),
				},
			},
			{
				Elements: []Element{
					NewTokenType(lexer.ItemPredicate),
				},
			},
			{
				Elements: []Element{
					NewTokenType(lexer.ItemLiteral),
				},
			},
		},
		"INSERT_DATA": []*Clause{
			{
				Elements: []Element{
					NewTokenType(lexer.ItemDot),
					NewTokenType(lexer.ItemNode),
					NewTokenType(lexer.ItemPredicate),
					NewSymbol("INSERT_OBJECT"),
					NewSymbol("INSERT_DATA"),
				},
			},
			{},
		},
		"DELETE_OBJECT": []*Clause{
			{
				Elements: []Element{
					NewTokenType(lexer.ItemNode),
				},
			},
			{
				Elements: []Element{
					NewTokenType(lexer.ItemPredicate),
				},
			},
			{
				Elements: []Element{
					NewTokenType(lexer.ItemLiteral),
				},
			},
		},
		"DELETE_DATA": []*Clause{
			{
				Elements: []Element{
					NewTokenType(lexer.ItemDot),
					NewTokenType(lexer.ItemNode),
					NewTokenType(lexer.ItemPredicate),
					NewSymbol("DELETE_OBJECT"),
					NewSymbol("DELETE_DATA"),
				},
			},
			{},
		},
		"CONSTRUCT_FACTS": []*Clause{
			{
				Elements: []Element{
					NewTokenType(lexer.ItemLBracket),
					NewSymbol("CONSTRUCT_TRIPLES"),
					NewTokenType(lexer.ItemRBracket),
				},
			},
		},
		"CONSTRUCT_TRIPLES": []*Clause{
			{
				Elements: []Element{
					NewTokenType(lexer.ItemNode),
					NewSymbol("CONSTRUCT_PREDICATE"),
					NewSymbol("CONSTRUCT_OBJECT"),
					NewSymbol("MORE_CONSTRUCT_PREDICATE_OBJECT_PAIRS"),
					NewSymbol("MORE_CONSTRUCT_TRIPLES"),
				},
			},
			{
				Elements: []Element{
					NewTokenType(lexer.ItemBlankNode),
					NewSymbol("CONSTRUCT_PREDICATE"),
					NewSymbol("CONSTRUCT_OBJECT"),
					NewSymbol("MORE_CONSTRUCT_PREDICATE_OBJECT_PAIRS"),
					NewSymbol("MORE_CONSTRUCT_TRIPLES"),
				},
			},
			{
				Elements: []Element{
					NewTokenType(lexer.ItemBinding),
					NewSymbol("CONSTRUCT_PREDICATE"),
					NewSymbol("CONSTRUCT_OBJECT"),
					NewSymbol("MORE_CONSTRUCT_PREDICATE_OBJECT_PAIRS"),
					NewSymbol("MORE_CONSTRUCT_TRIPLES"),
				},
			},
		},
		"CONSTRUCT_PREDICATE": []*Clause{
			{
				Elements: []Element{
					NewTokenType(lexer.ItemPredicate),
				},
			},
			{
				Elements: []Element{
					NewTokenType(lexer.ItemBinding),
				},
			},
		},
		"CONSTRUCT_OBJECT": []*Clause{
			{
				Elements: []Element{
					NewTokenType(lexer.ItemNode),
				},
			},
			{
				Elements: []Element{
					NewTokenType(lexer.ItemBlankNode),
				},
			},
			{
				Elements: []Element{
					NewTokenType(lexer.ItemPredicate),
				},
			},
			{
				Elements: []Element{
					NewTokenType(lexer.ItemLiteral),
				},
			},
			{
				Elements: []Element{
					NewTokenType(lexer.ItemBinding),
				},
			},
		},
		"MORE_CONSTRUCT_PREDICATE_OBJECT_PAIRS": []*Clause{
			{
				Elements: []Element{
					NewTokenType(lexer.ItemSemicolon),
					NewSymbol("CONSTRUCT_PREDICATE"),
					NewSymbol("CONSTRUCT_OBJECT"),
					NewSymbol("MORE_CONSTRUCT_PREDICATE_OBJECT_PAIRS"),
				},
			},
			{},
		},
		"MORE_CONSTRUCT_TRIPLES": []*Clause{
			{
				Elements: []Element{
					NewTokenType(lexer.ItemDot),
					NewSymbol("CONSTRUCT_TRIPLES"),
				},
			},
			{},
		},
		"DECONSTRUCT_FACTS": []*Clause{
			{
				Elements: []Element{
					NewTokenType(lexer.ItemLBracket),
					NewSymbol("DECONSTRUCT_TRIPLES"),
					NewTokenType(lexer.ItemRBracket),
				},
			},
		},
		"DECONSTRUCT_TRIPLES": []*Clause{
			{
				Elements: []Element{
					NewTokenType(lexer.ItemNode),
					NewSymbol("CONSTRUCT_PREDICATE"),
					NewSymbol("CONSTRUCT_OBJECT"),
					NewSymbol("MORE_DECONSTRUCT_TRIPLES"),
				},
			},
			{
				Elements: []Element{
					NewTokenType(lexer.ItemBinding),
					NewSymbol("CONSTRUCT_PREDICATE"),
					NewSymbol("CONSTRUCT_OBJECT"),
					NewSymbol("MORE_DECONSTRUCT_TRIPLES"),
				},
			},
		},
		"MORE_DECONSTRUCT_TRIPLES": []*Clause{
			{
				Elements: []Element{
					NewTokenType(lexer.ItemDot),
					NewSymbol("DECONSTRUCT_TRIPLES"),
				},
			},
			{},
		},
		"GRAPH_SHOW": []*Clause{
			{
				Elements: []Element{
					NewTokenType(lexer.ItemGraphs),
				},
			},
		},
	}
}

func setClauseHook(g *Grammar, symbols []semantic.Symbol, start, end semantic.ClauseHook) {
	for _, sym := range symbols {
		for _, cls := range (*g)[sym] {
			cls.ProcessStart = start
			cls.ProcessEnd = end
		}
	}
}

type condition func(*Clause) bool

func setElementHook(g *Grammar, symbols []semantic.Symbol, hook semantic.ElementHook, cnd condition) {
	for _, sym := range symbols {
		for _, cls := range (*g)[sym] {
			if cnd == nil || cnd(cls) {
				cls.ProcessedElement = hook
			}
		}
	}
}

// SemanticBQL contains the BQL grammar with hooks injected.
func SemanticBQL() *Grammar {
	semanticBQL := BQL()
	dataAcc := semantic.DataAccumulatorHook()

	// Create and Drop semantic hooks for type.
	setClauseHook(semanticBQL, []semantic.Symbol{"CREATE_GRAPHS"}, nil, semantic.TypeBindingClauseHook(semantic.Create))
	setClauseHook(semanticBQL, []semantic.Symbol{"DROP_GRAPHS"}, nil, semantic.TypeBindingClauseHook(semantic.Drop))

	// Add graph binding collection to GRAPHS and MORE_GRAPHS clauses.
	graphSymbols := []semantic.Symbol{"GRAPHS", "MORE_GRAPHS"}
	setElementHook(semanticBQL, graphSymbols, semantic.GraphAccumulatorHook(), nil)

	// Add graph binding collection to INPUT_GRAPHS and MORE_INPUT_GRAPHS clauses.
	inputGraphSymbols := []semantic.Symbol{"INPUT_GRAPHS", "MORE_INPUT_GRAPHS"}
	setElementHook(semanticBQL, inputGraphSymbols, semantic.InputGraphAccumulatorHook(), nil)

	// Add graph binding collection to OUTPUT_GRAPHS and MORE_OUTPUT_GRAPHS clauses.
	outputGraphSymbols := []semantic.Symbol{"OUTPUT_GRAPHS", "MORE_OUTPUT_GRAPHS"}
	setElementHook(semanticBQL, outputGraphSymbols, semantic.OutputGraphAccumulatorHook(), nil)

	// Insert and Delete semantic hooks addition.
	insertSymbols := []semantic.Symbol{
		"INSERT_OBJECT", "INSERT_DATA", "DELETE_OBJECT", "DELETE_DATA",
	}
	setElementHook(semanticBQL, insertSymbols, dataAcc, nil)
	setClauseHook(semanticBQL, []semantic.Symbol{"INSERT_OBJECT"}, nil, semantic.TypeBindingClauseHook(semantic.Insert))
	setClauseHook(semanticBQL, []semantic.Symbol{"DELETE_OBJECT"}, nil, semantic.TypeBindingClauseHook(semantic.Delete))

	// Query semantic hooks.
	setClauseHook(semanticBQL, []semantic.Symbol{"WHERE"}, semantic.WhereInitWorkingClauseHook(), semantic.VarBindingsGraphChecker())

	clauseSymbols := []semantic.Symbol{
		"FIRST_CLAUSE", "CLAUSES", "MORE_CLAUSES",
	}
	setClauseHook(semanticBQL, clauseSymbols, semantic.WhereNextWorkingClauseHook(), semantic.WhereNextWorkingClauseHook())

	subSymbols := []semantic.Symbol{
		"FIRST_CLAUSE", "CLAUSES", "OPTIONAL_CLAUSE", "SUBJECT_EXTRACT", "SUBJECT_TYPE", "SUBJECT_ID",
	}
	setElementHook(semanticBQL, subSymbols, semantic.WhereSubjectClauseHook(), nil)

	predSymbols := []semantic.Symbol{
		"PREDICATE", "PREDICATE_AS", "PREDICATE_ID", "PREDICATE_AT",
		"PREDICATE_BOUND_AT", "PREDICATE_BOUND_AT_BINDINGS", "PREDICATE_BOUND_AT_BINDINGS_END",
	}
	setElementHook(semanticBQL, predSymbols, semantic.WherePredicateClauseHook(), nil)

	objSymbols := []semantic.Symbol{
		"OBJECT", "OBJECT_SUBJECT_EXTRACT", "OBJECT_SUBJECT_TYPE", "OBJECT_SUBJECT_ID",
		"OBJECT_PREDICATE_AS", "OBJECT_PREDICATE_ID", "OBJECT_PREDICATE_AT",
		"OBJECT_PREDICATE_BOUND_AT", "OBJECT_PREDICATE_BOUND_AT_BINDINGS",
		"OBJECT_PREDICATE_BOUND_AT_BINDINGS_END", "OBJECT_LITERAL_AS",
		"OBJECT_LITERAL_BINDING_AS", "OBJECT_LITERAL_BINDING_TYPE",
		"OBJECT_LITERAL_BINDING_ID", "OBJECT_LITERAL_BINDING_AT",
	}
	setElementHook(semanticBQL, objSymbols, semantic.WhereObjectClauseHook(), nil)

	// Collect binding variables variables.
	varSymbols := []semantic.Symbol{
		"VARS", "VARS_AS", "MORE_VARS", "COUNT_DISTINCT",
	}
	setElementHook(semanticBQL, varSymbols, semantic.VarAccumulatorHook(), nil)

	// Collect and validate group by bindings.
	grpSymbols := []semantic.Symbol{"GROUP_BY", "GROUP_BY_BINDINGS"}
	setElementHook(semanticBQL, grpSymbols, semantic.GroupByBindings(), nil)
	setClauseHook(semanticBQL, []semantic.Symbol{"GROUP_BY"}, nil, semantic.GroupByBindingsChecker())

	// Collect and validate order by bindings.
	ordSymbols := []semantic.Symbol{"ORDER_BY", "ORDER_BY_DIRECTION", "ORDER_BY_BINDINGS"}
	setElementHook(semanticBQL, ordSymbols, semantic.OrderByBindings(), nil)
	setClauseHook(semanticBQL, []semantic.Symbol{"ORDER_BY"}, nil, semantic.OrderByBindingsChecker())

	// Collect the tokens that form the having clause and build the function
	// that will evaluate the result rows.
	havingSymbols := []semantic.Symbol{"HAVING", "HAVING_CLAUSE", "HAVING_CLAUSE_BINARY_COMPOSITE"}
	setElementHook(semanticBQL, havingSymbols, semantic.HavingExpression(), nil)
	setClauseHook(semanticBQL, []semantic.Symbol{"HAVING"}, nil, semantic.HavingExpressionBuilder())

	// Global time bound semantic hooks addition.
	globalSymbols := []semantic.Symbol{"GLOBAL_TIME_BOUND"}
	setElementHook(semanticBQL, globalSymbols, semantic.CollectGlobalBounds(), nil)

	// LIMIT clause semantic hook addition.
	limitSymbols := []semantic.Symbol{"LIMIT"}
	setElementHook(semanticBQL, limitSymbols, semantic.LimitCollection(), nil)

	// Global data accumulator hook.
	setElementHook(semanticBQL, []semantic.Symbol{"START"}, dataAcc,
		func(cls *Clause) bool {
			if t := cls.Elements[0].Token(); t != lexer.ItemInsert && t != lexer.ItemDelete {
				return false
			}
			return true
		})
	setClauseHook(semanticBQL, []semantic.Symbol{"START"}, nil, semantic.GroupByBindingsChecker())

	// CONSTRUCT and DECONSTRUCT clauses semantic hooks.
	setClauseHook(semanticBQL, []semantic.Symbol{"CONSTRUCT_FACTS"}, semantic.InitWorkingConstructClauseHook(), semantic.TypeBindingClauseHook(semantic.Construct))
	setClauseHook(semanticBQL, []semantic.Symbol{"DECONSTRUCT_FACTS"}, semantic.InitWorkingConstructClauseHook(), semantic.TypeBindingClauseHook(semantic.Deconstruct))
	constructAndDeconstructTriplesSymbols := []semantic.Symbol{"CONSTRUCT_TRIPLES", "MORE_CONSTRUCT_TRIPLES", "DECONSTRUCT_TRIPLES", "MORE_DECONSTRUCT_TRIPLES"}
	setClauseHook(semanticBQL, constructAndDeconstructTriplesSymbols, semantic.NextWorkingConstructClauseHook(), semantic.NextWorkingConstructClauseHook())
	setClauseHook(semanticBQL, []semantic.Symbol{"CONSTRUCT_PREDICATE"}, semantic.NextWorkingConstructPredicateObjectPairClauseHook(), nil)
	setClauseHook(semanticBQL, []semantic.Symbol{"CONSTRUCT_OBJECT"}, nil, semantic.NextWorkingConstructPredicateObjectPairClauseHook())

	setElementHook(semanticBQL, []semantic.Symbol{"CONSTRUCT_TRIPLES", "DECONSTRUCT_TRIPLES"}, semantic.ConstructSubjectHook(), nil)
	setElementHook(semanticBQL, []semantic.Symbol{"CONSTRUCT_PREDICATE"}, semantic.ConstructPredicateHook(), nil)
	setElementHook(semanticBQL, []semantic.Symbol{"CONSTRUCT_OBJECT"}, semantic.ConstructObjectHook(), nil)

	// SHOW GRAPHS clause semantic hooks.
	setClauseHook(semanticBQL, []semantic.Symbol{"GRAPH_SHOW"}, nil, semantic.ShowClauseHook())

	return semanticBQL
}
