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

var (
	// BQL LL1 grammar.
	BQL Grammar
	// SemanticBQL contains the BQL grammar with hooks injected.
	SemanticBQL Grammar
)

func init() {
	initBQL()
	initSemanticBQL()
}

func initBQL() {
	BQL = Grammar{
		"START": []*Clause{
			{
				Elements: []Element{
					NewTokenType(lexer.ItemQuery),
					NewSymbol("VARS"),
					NewTokenType(lexer.ItemFrom),
					NewSymbol("GRAPHS"),
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
					NewSymbol("GRAPHS"),
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
					NewSymbol("GRAPHS"),
					NewTokenType(lexer.ItemLBracket),
					NewTokenType(lexer.ItemNode),
					NewTokenType(lexer.ItemPredicate),
					NewSymbol("DELETE_OBJECT"),
					NewSymbol("DELETE_DATA"),
					NewTokenType(lexer.ItemRBracket),
					NewTokenType(lexer.ItemSemicolon),
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
		"WHERE": []*Clause{
			{
				Elements: []Element{
					NewTokenType(lexer.ItemWhere),
					NewTokenType(lexer.ItemLBracket),
					NewSymbol("CLAUSES"),
					NewTokenType(lexer.ItemRBracket),
				},
			},
		},
		"CLAUSES": []*Clause{
			{
				Elements: []Element{
					NewTokenType(lexer.ItemNode),
					NewSymbol("SUBJECT_EXTRACT"),
					NewSymbol("PREDICATES"),
					NewSymbol("OBJECTS"),
					NewSymbol("NORE_CLAUSES"),
				},
			},
			{
				Elements: []Element{
					NewTokenType(lexer.ItemBinding),
					NewSymbol("SUBJECT_EXTRACT"),
					NewSymbol("PREDICATE"),
					NewSymbol("OBJECT"),
					NewSymbol("NORE_CLAUSES"),
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
					NewTokenType(lexer.ItemNode),
					NewSymbol("SUBJECT_EXTRACT"),
				},
			},
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
					NewTokenType(lexer.ItemLiteral),
					NewSymbol("LITERAL_AS"),
				},
			},
			{
				Elements: []Element{
					NewTokenType(lexer.ItemBinding),
					NewSymbol("LITERAL_BINDING_AS"),
					NewSymbol("LITERAL_BINDING_TYPE"),
					NewSymbol("LITERAL_BINDING_ID"),
					NewSymbol("LITERAL_BINDING_AT"),
				},
			},
		},
		"LITERAL_AS": []*Clause{
			{
				Elements: []Element{
					NewTokenType(lexer.ItemAs),
					NewTokenType(lexer.ItemBinding),
				},
			},
			{},
		},
		"LITERAL_BINDING_AS": []*Clause{
			{
				Elements: []Element{
					NewTokenType(lexer.ItemAs),
					NewTokenType(lexer.ItemBinding),
				},
			},
			{},
		},
		"LITERAL_BINDING_TYPE": []*Clause{
			{
				Elements: []Element{
					NewTokenType(lexer.ItemType),
					NewTokenType(lexer.ItemBinding),
				},
			},
			{},
		},
		"LITERAL_BINDING_ID": []*Clause{
			{
				Elements: []Element{
					NewTokenType(lexer.ItemID),
					NewTokenType(lexer.ItemBinding),
				},
			},
			{},
		},
		"LITERAL_BINDING_AT": []*Clause{
			{
				Elements: []Element{
					NewTokenType(lexer.ItemAt),
					NewTokenType(lexer.ItemBinding),
				},
			},
			{},
		},
		"NORE_CLAUSES": []*Clause{
			{
				Elements: []Element{
					NewTokenType(lexer.ItemDot),
					NewSymbol("CLAUSES"),
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
					NewSymbol("GLOBAL_TIME_BOUND_COMPOSITE"),
				},
			},
			{
				Elements: []Element{
					NewTokenType(lexer.ItemAfter),
					NewTokenType(lexer.ItemPredicate),
					NewSymbol("GLOBAL_TIME_BOUND_COMPOSITE"),
				},
			},
			{
				Elements: []Element{
					NewTokenType(lexer.ItemBetween),
					NewTokenType(lexer.ItemPredicate),
					NewTokenType(lexer.ItemComma),
					NewTokenType(lexer.ItemPredicate),
					NewSymbol("GLOBAL_TIME_BOUND_COMPOSITE"),
				},
			},
			{
				Elements: []Element{
					NewTokenType(lexer.ItemLPar),
					NewSymbol("GLOBAL_TIME_BOUND"),
					NewTokenType(lexer.ItemRPar),
				},
			},
			{},
		},
		"GLOBAL_TIME_BOUND_COMPOSITE": []*Clause{
			{
				Elements: []Element{
					NewTokenType(lexer.ItemAnd),
					NewSymbol("GLOBAL_TIME_BOUND"),
				},
			},
			{
				Elements: []Element{
					NewTokenType(lexer.ItemOr),
					NewSymbol("GLOBAL_TIME_BOUND"),
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
	}
}

func cloneGrammar(dst, src *Grammar) {
	for k, cls := range *src {
		newCls := []*Clause{}
		for _, c := range cls {
			newC := new(Clause)
			*newC = *c
			newCls = append(newCls, newC)
		}
		(*dst)[k] = newCls
	}
}

func initSemanticBQL() {
	SemanticBQL = Grammar{}
	cloneGrammar(&SemanticBQL, &BQL)

	// Insert and Delete semantic hook addition.
	symbols := []semantic.Symbol{"INSERT_OBJECT", "INSERT_DATA", "DELETE_OBJECT", "DELETE_DATA"}
	for _, sym := range symbols {
		for _, cls := range SemanticBQL[sym] {
			cls.ProcessedElement = semantic.DataAccumulatorHook()
		}
	}
	for _, cls := range SemanticBQL["INSERT_OBJECT"] {
		cls.ProcessEnd = semantic.TypeBindingClauseHook(semantic.Insert)
	}
	for _, cls := range SemanticBQL["DELETE_OBJECT"] {
		cls.ProcessEnd = semantic.TypeBindingClauseHook(semantic.Delete)
	}
	for _, cls := range SemanticBQL["START"] {
		if t := cls.Elements[0].Token(); t != lexer.ItemInsert && t != lexer.ItemDelete {
			continue
		}
		cls.ProcessedElement = semantic.DataAccumulatorHook()
	}

	// Add graph binding collection to GRAPHS and MORE_GRAPHS clauses.
	graphSymbols := []semantic.Symbol{"GRAPHS", "MORE_GRAPHS"}
	for _, sym := range graphSymbols {
		for _, cls := range SemanticBQL[sym] {
			cls.ProcessedElement = semantic.GraphAccumulatorHook()
		}
	}
}
