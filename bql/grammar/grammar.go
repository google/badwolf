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

func startClauses() []*Clause {
	return []*Clause{
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
	}
}

func createGraphClauses() []*Clause {
	return []*Clause{
		{
			Elements: []Element{
				NewTokenType(lexer.ItemGraph),
				NewSymbol("GRAPHS"),
			},
		},
	}
}

func dropGraphClauses() []*Clause {
	return []*Clause{
		{
			Elements: []Element{
				NewTokenType(lexer.ItemGraph),
				NewSymbol("GRAPHS"),
			},
		},
	}
}

func varsClauses() []*Clause {
	return []*Clause{
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
	}
}

func countDistinctClauses() []*Clause {
	return []*Clause{
		{
			Elements: []Element{
				NewTokenType(lexer.ItemDistinct),
			},
		},
		{},
	}
}

func varsAsClauses() []*Clause {
	return []*Clause{
		{
			Elements: []Element{
				NewTokenType(lexer.ItemAs),
				NewTokenType(lexer.ItemBinding),
			},
		},
		{},
	}
}

func moreVarsClauses() []*Clause {
	return []*Clause{
		{
			Elements: []Element{
				NewTokenType(lexer.ItemComma),
				NewSymbol("VARS"),
			},
		},
		{},
	}
}

func graphsClauses() []*Clause {
	return []*Clause{
		{
			Elements: []Element{
				NewTokenType(lexer.ItemBinding),
				NewSymbol("MORE_GRAPHS"),
			},
		},
	}
}

func moreGraphsClauses() []*Clause {
	return []*Clause{
		{
			Elements: []Element{
				NewTokenType(lexer.ItemComma),
				NewTokenType(lexer.ItemBinding),
				NewSymbol("MORE_GRAPHS"),
			},
		},
		{},
	}
}

func inputGraphClauses() []*Clause {
	return []*Clause{
		{
			Elements: []Element{
				NewTokenType(lexer.ItemBinding),
				NewSymbol("MORE_INPUT_GRAPHS"),
			},
		},
	}
}

func moreInputGraphClauses() []*Clause {
	return []*Clause{
		{
			Elements: []Element{
				NewTokenType(lexer.ItemComma),
				NewTokenType(lexer.ItemBinding),
				NewSymbol("MORE_INPUT_GRAPHS"),
			},
		},
		{},
	}
}

func outputGraphClauses() []*Clause {
	return []*Clause{
		{
			Elements: []Element{
				NewTokenType(lexer.ItemBinding),
				NewSymbol("MORE_OUTPUT_GRAPHS"),
			},
		},
	}
}

func moreOutputGraphClauses() []*Clause {
	return []*Clause{
		{
			Elements: []Element{
				NewTokenType(lexer.ItemComma),
				NewTokenType(lexer.ItemBinding),
				NewSymbol("MORE_OUTPUT_GRAPHS"),
			},
		},
		{},
	}
}

func whereClauses() []*Clause {
	return []*Clause{
		{
			Elements: []Element{
				NewTokenType(lexer.ItemWhere),
				NewTokenType(lexer.ItemLBracket),
				NewSymbol("FIRST_CLAUSE"),
				NewTokenType(lexer.ItemRBracket),
			},
		},
	}
}

func firstClauses() []*Clause {
	return []*Clause{
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
	}
}
func moreClauses() []*Clause {
	return []*Clause{
		{
			Elements: []Element{
				NewTokenType(lexer.ItemDot),
				NewSymbol("CLAUSES"),
				NewSymbol("FILTER_CLAUSES"),
			},
		},
		{},
	}
}
func clauses() []*Clause {
	return []*Clause{
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
		{},
	}
}

func moreFilterClauses() []*Clause {
	return []*Clause{
		{
			Elements: []Element{
				NewTokenType(lexer.ItemDot),
				NewSymbol("FILTER_CLAUSES"),
			},
		},
		{},
	}
}
func filterClauses() []*Clause {
	return []*Clause{
		{
			Elements: []Element{
				NewTokenType(lexer.ItemFilter),
				NewTokenType(lexer.ItemFilterFunction),
				NewTokenType(lexer.ItemLPar),
				NewTokenType(lexer.ItemBinding),
				NewSymbol("MORE_FILTER_ARGUMENTS"),
				NewTokenType(lexer.ItemRPar),
				NewSymbol("MORE_FILTER_CLAUSES"),
			},
		},
		{},
	}
}
func moreFilterArguments() []*Clause {
	return []*Clause{
		{
			Elements: []Element{
				NewTokenType(lexer.ItemComma),
				NewTokenType(lexer.ItemLiteral),
			},
		},
		{},
	}
}

func optionalClauses() []*Clause {
	return []*Clause{
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
	}
}

func subjectExtractClauses() []*Clause {
	return []*Clause{
		{
			Elements: []Element{
				NewTokenType(lexer.ItemAs),
				NewTokenType(lexer.ItemBinding),
				NewSymbol("SUBJECT_ID_TYPE_PERMUTATION"),
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
				NewSymbol("SUBJECT_TYPE"),
			},
		},
		{},
	}
}

func subjectIDTypePermutationClauses() []*Clause {
	return []*Clause{
		{
			Elements: []Element{
				NewTokenType(lexer.ItemID),
				NewTokenType(lexer.ItemBinding),
				NewSymbol("SUBJECT_TYPE"),
			},
		},
		{
			Elements: []Element{
				NewTokenType(lexer.ItemType),
				NewTokenType(lexer.ItemBinding),
				NewSymbol("SUBJECT_ID"),
			},
		},
		{},
	}
}

func subjectTypeClauses() []*Clause {
	return []*Clause{
		{
			Elements: []Element{
				NewTokenType(lexer.ItemType),
				NewTokenType(lexer.ItemBinding),
			},
		},
		{},
	}
}

func subjectIDClauses() []*Clause {
	return []*Clause{
		{
			Elements: []Element{
				NewTokenType(lexer.ItemID),
				NewTokenType(lexer.ItemBinding),
			},
		},
		{},
	}
}

func predicateClauses() []*Clause {
	return []*Clause{
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
	}
}

func predicateAsClauses() []*Clause {
	return []*Clause{
		{
			Elements: []Element{
				NewTokenType(lexer.ItemAs),
				NewTokenType(lexer.ItemBinding),
			},
		},
		{},
	}
}

func predicateIDClauses() []*Clause {
	return []*Clause{
		{
			Elements: []Element{
				NewTokenType(lexer.ItemID),
				NewTokenType(lexer.ItemBinding),
			},
		},
		{},
	}
}

func predicateAtClauses() []*Clause {
	return []*Clause{
		{
			Elements: []Element{
				NewTokenType(lexer.ItemAt),
				NewTokenType(lexer.ItemBinding),
			},
		},
		{},
	}
}

func predicateBoundAtClauses() []*Clause {
	return []*Clause{
		{
			Elements: []Element{
				NewTokenType(lexer.ItemAt),
				NewSymbol("PREDICATE_BOUND_AT_BINDINGS"),
			},
		},
		{},
	}
}

func predicateBoundAtBindingsClauses() []*Clause {
	return []*Clause{
		{
			Elements: []Element{
				NewTokenType(lexer.ItemBinding),
				NewSymbol("PREDICATE_BOUND_AT_BINDINGS_END"),
			},
		},
		{},
	}
}

func predicateBoundAtBindingsEndClauses() []*Clause {
	return []*Clause{{
		Elements: []Element{
			NewTokenType(lexer.ItemComma),
			NewTokenType(lexer.ItemBinding),
		},
	},
		{},
	}
}

func objectClauses() []*Clause {
	return []*Clause{
		{
			Elements: []Element{
				NewTokenType(lexer.ItemLiteral),
				NewSymbol("OBJECT_LITERAL_AS"),
			},
		},
		{
			Elements: []Element{
				NewTokenType(lexer.ItemNode),
				NewSymbol("OBJECT_NODE_EXTRACT"),
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
				NewSymbol("OBJECT_BINDING_EXTRACT"),
			},
		},
	}
}

func objectNodeExtractClauses() []*Clause {
	return []*Clause{
		{
			Elements: []Element{
				NewTokenType(lexer.ItemAs),
				NewTokenType(lexer.ItemBinding),
				NewSymbol("OBJECT_NODE_ID_TYPE_PERMUTATION"),
			},
		},
		{
			Elements: []Element{
				NewTokenType(lexer.ItemType),
				NewTokenType(lexer.ItemBinding),
				NewSymbol("OBJECT_NODE_ID"),
			},
		},
		{
			Elements: []Element{
				NewTokenType(lexer.ItemID),
				NewTokenType(lexer.ItemBinding),
				NewSymbol("OBJECT_NODE_TYPE"),
			},
		},
		{},
	}
}

func objectNodeIDTypePermutationClauses() []*Clause {
	return []*Clause{
		{
			Elements: []Element{
				NewTokenType(lexer.ItemID),
				NewTokenType(lexer.ItemBinding),
				NewSymbol("OBJECT_NODE_TYPE"),
			},
		},
		{
			Elements: []Element{
				NewTokenType(lexer.ItemType),
				NewTokenType(lexer.ItemBinding),
				NewSymbol("OBJECT_NODE_ID"),
			},
		},
		{},
	}
}

func objectNodeTypeClauses() []*Clause {
	return []*Clause{
		{
			Elements: []Element{
				NewTokenType(lexer.ItemType),
				NewTokenType(lexer.ItemBinding),
			},
		},
		{},
	}
}

func objectNodeIDClauses() []*Clause {
	return []*Clause{
		{
			Elements: []Element{
				NewTokenType(lexer.ItemID),
				NewTokenType(lexer.ItemBinding),
			},
		},
		{},
	}
}

func objectPredicateAsClauses() []*Clause {
	return []*Clause{
		{
			Elements: []Element{
				NewTokenType(lexer.ItemAs),
				NewTokenType(lexer.ItemBinding),
			},
		},
		{},
	}
}

func objectPredicateIDClauses() []*Clause {
	return []*Clause{
		{
			Elements: []Element{
				NewTokenType(lexer.ItemID),
				NewTokenType(lexer.ItemBinding),
			},
		},
		{},
	}
}

func objectPredicateAtClauses() []*Clause {
	return []*Clause{
		{
			Elements: []Element{
				NewTokenType(lexer.ItemAt),
				NewTokenType(lexer.ItemBinding),
			},
		},
		{},
	}
}

func objectPredicateBoundAtClauses() []*Clause {
	return []*Clause{
		{
			Elements: []Element{
				NewTokenType(lexer.ItemAt),
				NewSymbol("OBJECT_PREDICATE_BOUND_AT_BINDINGS"),
			},
		},
		{},
	}
}

func objectPredicateBoundAtBindingsClauses() []*Clause {
	return []*Clause{{
		Elements: []Element{
			NewTokenType(lexer.ItemBinding),
			NewSymbol("OBJECT_PREDICATE_BOUND_AT_BINDINGS_END"),
		},
	},
		{},
	}
}

func objectPredicateBoundAtBindingsEndClauses() []*Clause {
	return []*Clause{{
		Elements: []Element{
			NewTokenType(lexer.ItemComma),
			NewTokenType(lexer.ItemBinding),
		},
	},
		{},
	}
}

func objectLiteralAsClauses() []*Clause {
	return []*Clause{{
		Elements: []Element{
			NewTokenType(lexer.ItemAs),
			NewTokenType(lexer.ItemBinding),
		},
	},
		{},
	}
}

func objectBindingExtractClauses() []*Clause {
	return []*Clause{
		{
			Elements: []Element{
				NewTokenType(lexer.ItemAs),
				NewTokenType(lexer.ItemBinding),
				NewSymbol("OBJECT_BINDING_ID_TYPE_PERMUTATION"),
				NewSymbol("OBJECT_BINDING_AT"),
			},
		},
		{
			Elements: []Element{
				NewTokenType(lexer.ItemType),
				NewTokenType(lexer.ItemBinding),
				NewSymbol("OBJECT_BINDING_ID"),
				NewSymbol("OBJECT_BINDING_AT"),
			},
		},
		{
			Elements: []Element{
				NewTokenType(lexer.ItemID),
				NewTokenType(lexer.ItemBinding),
				NewSymbol("OBJECT_BINDING_TYPE"),
				NewSymbol("OBJECT_BINDING_AT"),
			},
		},
		{
			Elements: []Element{
				NewTokenType(lexer.ItemAt),
				NewTokenType(lexer.ItemBinding),
			},
		},
		{},
	}
}

func objectBindingIDTypePermutationClauses() []*Clause {
	return []*Clause{
		{
			Elements: []Element{
				NewTokenType(lexer.ItemID),
				NewTokenType(lexer.ItemBinding),
				NewSymbol("OBJECT_BINDING_TYPE"),
			},
		},
		{
			Elements: []Element{
				NewTokenType(lexer.ItemType),
				NewTokenType(lexer.ItemBinding),
				NewSymbol("OBJECT_BINDING_ID"),
			},
		},
		{},
	}
}

func objectBindingTypeClauses() []*Clause {
	return []*Clause{
		{
			Elements: []Element{
				NewTokenType(lexer.ItemType),
				NewTokenType(lexer.ItemBinding),
			},
		},
		{},
	}
}

func objectBindingIDClauses() []*Clause {
	return []*Clause{
		{
			Elements: []Element{
				NewTokenType(lexer.ItemID),
				NewTokenType(lexer.ItemBinding),
			},
		},
		{},
	}
}

func objectBindingAtClauses() []*Clause {
	return []*Clause{
		{
			Elements: []Element{
				NewTokenType(lexer.ItemAt),
				NewTokenType(lexer.ItemBinding),
			},
		},
		{},
	}
}

func groupByClauses() []*Clause {
	return []*Clause{
		{
			Elements: []Element{
				NewTokenType(lexer.ItemGroup),
				NewTokenType(lexer.ItemBy),
				NewTokenType(lexer.ItemBinding),
				NewSymbol("GROUP_BY_BINDINGS"),
			},
		},
		{},
	}
}

func groupByBindingsClauses() []*Clause {
	return []*Clause{{
		Elements: []Element{
			NewTokenType(lexer.ItemComma),
			NewTokenType(lexer.ItemBinding),
			NewSymbol("GROUP_BY_BINDINGS"),
		},
	},
		{},
	}
}
func orderByClauses() []*Clause {
	return []*Clause{{
		Elements: []Element{
			NewTokenType(lexer.ItemOrder),
			NewTokenType(lexer.ItemBy),
			NewTokenType(lexer.ItemBinding),
			NewSymbol("ORDER_BY_DIRECTION"),
			NewSymbol("ORDER_BY_BINDINGS"),
		},
	},
		{},
	}
}
func orderByDirectionClauses() []*Clause {
	return []*Clause{{
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
	}
}
func orderByBindingsClauses() []*Clause {
	return []*Clause{{
		Elements: []Element{
			NewTokenType(lexer.ItemComma),
			NewTokenType(lexer.ItemBinding),
			NewSymbol("ORDER_BY_DIRECTION"),
			NewSymbol("ORDER_BY_BINDINGS"),
		},
	},
		{},
	}
}
func topHavingClauses() []*Clause {
	return []*Clause{{
		Elements: []Element{
			NewTokenType(lexer.ItemHaving),
			NewSymbol("HAVING_CLAUSE"),
		},
	},
		{},
	}
}
func havingClauses() []*Clause {
	return []*Clause{
		{
			Elements: []Element{
				NewTokenType(lexer.ItemBinding),
				NewSymbol("HAVING_CLAUSE_BINARY_COMPOSITE"),
			},
		},
		{
			Elements: []Element{
				NewTokenType(lexer.ItemNode),
				NewSymbol("HAVING_CLAUSE_BINARY_COMPOSITE"),
			},
		},
		{
			Elements: []Element{
				NewTokenType(lexer.ItemLiteral),
				NewSymbol("HAVING_CLAUSE_BINARY_COMPOSITE"),
			},
		},
		{
			Elements: []Element{
				NewTokenType(lexer.ItemTime),
				NewSymbol("HAVING_CLAUSE_BINARY_COMPOSITE"),
			},
		},
		{
			Elements: []Element{
				NewTokenType(lexer.ItemPredicate),
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
	}
}
func havingClausesBinaryCompositeClauses() []*Clause {
	return []*Clause{{
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
	}
}
func globalTimeBoundClauses() []*Clause {
	return []*Clause{
		{
			Elements: []Element{
				NewTokenType(lexer.ItemBefore),
				NewTokenType(lexer.ItemTime),
			},
		},
		{
			Elements: []Element{
				NewTokenType(lexer.ItemAfter),
				NewTokenType(lexer.ItemTime),
			},
		},
		{
			Elements: []Element{
				NewTokenType(lexer.ItemBetween),
				NewTokenType(lexer.ItemPredicateBound),
			},
		},
		{},
	}
}
func limitClauses() []*Clause {
	return []*Clause{{
		Elements: []Element{
			NewTokenType(lexer.ItemLimit),
			NewTokenType(lexer.ItemLiteral),
		},
	},
		{},
	}
}
func insertObjectClauses() []*Clause {
	return []*Clause{{
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
	}
}
func insertDataClauses() []*Clause {
	return []*Clause{{
		Elements: []Element{
			NewTokenType(lexer.ItemDot),
			NewTokenType(lexer.ItemNode),
			NewTokenType(lexer.ItemPredicate),
			NewSymbol("INSERT_OBJECT"),
			NewSymbol("INSERT_DATA"),
		},
	},
		{},
	}
}
func deleteObjectClauses() []*Clause {
	return []*Clause{{
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
	}
}
func deleteDataClauses() []*Clause {
	return []*Clause{{
		Elements: []Element{
			NewTokenType(lexer.ItemDot),
			NewTokenType(lexer.ItemNode),
			NewTokenType(lexer.ItemPredicate),
			NewSymbol("DELETE_OBJECT"),
			NewSymbol("DELETE_DATA"),
		},
	},
		{},
	}
}
func constructFactsClauses() []*Clause {
	return []*Clause{{
		Elements: []Element{
			NewTokenType(lexer.ItemLBracket),
			NewSymbol("CONSTRUCT_TRIPLES"),
			NewTokenType(lexer.ItemRBracket),
		},
	},
	}
}
func constructTriplesClauses() []*Clause {
	return []*Clause{{
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
	}
}
func constructPredicateClauses() []*Clause {
	return []*Clause{{
		Elements: []Element{
			NewTokenType(lexer.ItemPredicate),
		},
	},
		{
			Elements: []Element{
				NewTokenType(lexer.ItemBinding),
			},
		},
	}
}
func constructObjectClauses() []*Clause {
	return []*Clause{
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
	}
}
func moreConstructPredicateObjectPairsClauses() []*Clause {
	return []*Clause{{
		Elements: []Element{
			NewTokenType(lexer.ItemSemicolon),
			NewSymbol("CONSTRUCT_PREDICATE"),
			NewSymbol("CONSTRUCT_OBJECT"),
			NewSymbol("MORE_CONSTRUCT_PREDICATE_OBJECT_PAIRS"),
		},
	},
		{},
	}
}
func moreConstructTriplesClauses() []*Clause {
	return []*Clause{{
		Elements: []Element{
			NewTokenType(lexer.ItemDot),
			NewSymbol("CONSTRUCT_TRIPLES"),
		},
	},
		{},
	}
}
func deconstructFactsClauses() []*Clause {
	return []*Clause{{
		Elements: []Element{
			NewTokenType(lexer.ItemLBracket),
			NewSymbol("DECONSTRUCT_TRIPLES"),
			NewTokenType(lexer.ItemRBracket),
		},
	},
	}
}
func deconstructTriplesClauses() []*Clause {
	return []*Clause{{
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
	}
}
func moreDeconstructTriplesClauses() []*Clause {
	return []*Clause{
		{
			Elements: []Element{
				NewTokenType(lexer.ItemDot),
				NewSymbol("DECONSTRUCT_TRIPLES"),
			},
		},
		{},
	}
}
func graphShowClauses() []*Clause {
	return []*Clause{
		{
			Elements: []Element{
				NewTokenType(lexer.ItemGraphs),
			},
		},
	}
}

// BQL LL1 grammar.
func BQL() *Grammar {
	return &Grammar{
		"START":                                  startClauses(),
		"CREATE_GRAPHS":                          createGraphClauses(),
		"DROP_GRAPHS":                            dropGraphClauses(),
		"VARS":                                   varsClauses(),
		"COUNT_DISTINCT":                         countDistinctClauses(),
		"VARS_AS":                                varsAsClauses(),
		"MORE_VARS":                              moreVarsClauses(),
		"GRAPHS":                                 graphsClauses(),
		"MORE_GRAPHS":                            moreGraphsClauses(),
		"INPUT_GRAPHS":                           inputGraphClauses(),
		"MORE_INPUT_GRAPHS":                      moreInputGraphClauses(),
		"OUTPUT_GRAPHS":                          outputGraphClauses(),
		"MORE_OUTPUT_GRAPHS":                     moreOutputGraphClauses(),
		"WHERE":                                  whereClauses(),
		"FIRST_CLAUSE":                           firstClauses(),
		"MORE_CLAUSES":                           moreClauses(),
		"CLAUSES":                                clauses(),
		"OPTIONAL_CLAUSE":                        optionalClauses(),
		"FILTER_CLAUSES":                         filterClauses(),
		"MORE_FILTER_CLAUSES":                    moreFilterClauses(),
		"MORE_FILTER_ARGUMENTS":                  moreFilterArguments(),
		"SUBJECT_EXTRACT":                        subjectExtractClauses(),
		"SUBJECT_TYPE":                           subjectTypeClauses(),
		"SUBJECT_ID":                             subjectIDClauses(),
		"SUBJECT_ID_TYPE_PERMUTATION":            subjectIDTypePermutationClauses(),
		"PREDICATE":                              predicateClauses(),
		"PREDICATE_AS":                           predicateAsClauses(),
		"PREDICATE_ID":                           predicateIDClauses(),
		"PREDICATE_AT":                           predicateAtClauses(),
		"PREDICATE_BOUND_AT":                     predicateBoundAtClauses(),
		"PREDICATE_BOUND_AT_BINDINGS":            predicateBoundAtBindingsClauses(),
		"PREDICATE_BOUND_AT_BINDINGS_END":        predicateBoundAtBindingsEndClauses(),
		"OBJECT":                                 objectClauses(),
		"OBJECT_NODE_EXTRACT":                    objectNodeExtractClauses(),
		"OBJECT_NODE_TYPE":                       objectNodeTypeClauses(),
		"OBJECT_NODE_ID":                         objectNodeIDClauses(),
		"OBJECT_NODE_ID_TYPE_PERMUTATION":        objectNodeIDTypePermutationClauses(),
		"OBJECT_PREDICATE_AS":                    objectPredicateAsClauses(),
		"OBJECT_PREDICATE_ID":                    objectPredicateIDClauses(),
		"OBJECT_PREDICATE_AT":                    objectPredicateAtClauses(),
		"OBJECT_PREDICATE_BOUND_AT":              objectPredicateBoundAtClauses(),
		"OBJECT_PREDICATE_BOUND_AT_BINDINGS":     objectPredicateBoundAtBindingsClauses(),
		"OBJECT_PREDICATE_BOUND_AT_BINDINGS_END": objectPredicateBoundAtBindingsEndClauses(),
		"OBJECT_LITERAL_AS":                      objectLiteralAsClauses(),
		"OBJECT_BINDING_EXTRACT":                 objectBindingExtractClauses(),
		"OBJECT_BINDING_TYPE":                    objectBindingTypeClauses(),
		"OBJECT_BINDING_ID":                      objectBindingIDClauses(),
		"OBJECT_BINDING_ID_TYPE_PERMUTATION":     objectBindingIDTypePermutationClauses(),
		"OBJECT_BINDING_AT":                      objectBindingAtClauses(),
		"GROUP_BY":                               groupByClauses(),
		"GROUP_BY_BINDINGS":                      groupByBindingsClauses(),
		"ORDER_BY":                               orderByClauses(),
		"ORDER_BY_DIRECTION":                     orderByDirectionClauses(),
		"ORDER_BY_BINDINGS":                      orderByBindingsClauses(),
		"HAVING":                                 topHavingClauses(),
		"HAVING_CLAUSE":                          havingClauses(),
		"HAVING_CLAUSE_BINARY_COMPOSITE":         havingClausesBinaryCompositeClauses(),
		"GLOBAL_TIME_BOUND":                      globalTimeBoundClauses(),
		"LIMIT":                                  limitClauses(),
		"INSERT_OBJECT":                          insertObjectClauses(),
		"INSERT_DATA":                            insertDataClauses(),
		"DELETE_OBJECT":                          deleteObjectClauses(),
		"DELETE_DATA":                            deleteDataClauses(),
		"CONSTRUCT_FACTS":                        constructFactsClauses(),
		"CONSTRUCT_TRIPLES":                      constructTriplesClauses(),
		"CONSTRUCT_PREDICATE":                    constructPredicateClauses(),
		"CONSTRUCT_OBJECT":                       constructObjectClauses(),
		"MORE_CONSTRUCT_PREDICATE_OBJECT_PAIRS":  moreConstructPredicateObjectPairsClauses(),
		"MORE_CONSTRUCT_TRIPLES":                 moreConstructTriplesClauses(),
		"DECONSTRUCT_FACTS":                      deconstructFactsClauses(),
		"DECONSTRUCT_TRIPLES":                    deconstructTriplesClauses(),
		"MORE_DECONSTRUCT_TRIPLES":               moreDeconstructTriplesClauses(),
		"GRAPH_SHOW":                             graphShowClauses(),
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
		"FIRST_CLAUSE", "CLAUSES", "OPTIONAL_CLAUSE", "SUBJECT_EXTRACT", "SUBJECT_TYPE", "SUBJECT_ID", "SUBJECT_ID_TYPE_PERMUTATION",
	}
	setElementHook(semanticBQL, subSymbols, semantic.WhereSubjectClauseHook(), nil)

	predSymbols := []semantic.Symbol{
		"PREDICATE", "PREDICATE_AS", "PREDICATE_ID", "PREDICATE_AT",
		"PREDICATE_BOUND_AT", "PREDICATE_BOUND_AT_BINDINGS", "PREDICATE_BOUND_AT_BINDINGS_END",
	}
	setElementHook(semanticBQL, predSymbols, semantic.WherePredicateClauseHook(), nil)

	objSymbols := []semantic.Symbol{
		"OBJECT", "OBJECT_NODE_EXTRACT", "OBJECT_NODE_TYPE", "OBJECT_NODE_ID",
		"OBJECT_NODE_ID_TYPE_PERMUTATION", "OBJECT_PREDICATE_AS", "OBJECT_PREDICATE_ID", "OBJECT_PREDICATE_AT",
		"OBJECT_PREDICATE_BOUND_AT", "OBJECT_PREDICATE_BOUND_AT_BINDINGS",
		"OBJECT_PREDICATE_BOUND_AT_BINDINGS_END", "OBJECT_LITERAL_AS",
		"OBJECT_BINDING_EXTRACT", "OBJECT_BINDING_TYPE",
		"OBJECT_BINDING_ID", "OBJECT_BINDING_ID_TYPE_PERMUTATION", "OBJECT_BINDING_AT",
	}
	setElementHook(semanticBQL, objSymbols, semantic.WhereObjectClauseHook(), nil)

	// Filter clause hook.
	filterSymbols := []semantic.Symbol{
		"FILTER_CLAUSES", "MORE_FILTER_ARGUMENTS",
	}
	setElementHook(semanticBQL, filterSymbols, semantic.WhereFilterClauseHook(), nil)

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
