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
	"errors"
	"fmt"
	"strings"

	"github.com/google/badwolf/bql/lexer"
	"github.com/google/badwolf/triple"
	"github.com/google/badwolf/triple/literal"
	"github.com/google/badwolf/triple/node"
	"github.com/google/badwolf/triple/predicate"
)

// ClauseHook is a function hook for the parser that gets called on clause wide
// events.
type ClauseHook func(*Statement, Symbol) (ClauseHook, error)

// ElementHook is a function hook for the parser that gets called after an
// Element is confused.
type ElementHook func(*Statement, ConsumedElement) (ElementHook, error)

// TypeBindingClauseHook returns a ClauseHook that sets the binding type.
func TypeBindingClauseHook(t StatementType) ClauseHook {
	var f ClauseHook
	f = func(stm *Statement, _ Symbol) (ClauseHook, error) {
		stm.BindType(t)
		return f, nil
	}
	return f
}

// dataAccumulator creates a element hook that tracks fully formed triples and
// adds them to the Statement when fully formed.
func dataAccumulator(b literal.Builder) ElementHook {
	var (
		hook ElementHook
		s    *node.Node
		p    *predicate.Predicate
		o    *triple.Object
	)

	hook = func(st *Statement, ce ConsumedElement) (ElementHook, error) {
		if ce.IsSymbol() {
			return hook, nil
		}
		tkn := ce.Token()
		if tkn.Type != lexer.ItemNode && tkn.Type != lexer.ItemPredicate && tkn.Type != lexer.ItemLiteral {
			return hook, nil
		}
		if s == nil {
			if tkn.Type != lexer.ItemNode {
				return nil, fmt.Errorf("hook.DataAccumulator requires a node to create a subject, got %v instead", tkn)
			}
			tmp, err := node.Parse(tkn.Text)
			if err != nil {
				return nil, err
			}
			s = tmp
			return hook, nil
		}
		if p == nil {
			if tkn.Type != lexer.ItemPredicate {
				return nil, fmt.Errorf("hook.DataAccumulator requires a predicate to create a predicate, got %v instead", tkn)
			}
			tmp, err := predicate.Parse(tkn.Text)
			if err != nil {
				return nil, err
			}
			p = tmp
			return hook, nil
		}
		if o == nil {
			tmp, err := triple.ParseObject(tkn.Text, b)
			if err != nil {
				return nil, err
			}
			o = tmp
			trpl, err := triple.NewTriple(s, p, o)
			if err != nil {
				return nil, err
			}
			st.AddData(trpl)
			s, p, o = nil, nil, nil
			return hook, nil
		}
		return nil, fmt.Errorf("hook.DataAccumulator has failed to flush the triple %s, %s, %s", s, p, o)
	}
	return hook
}

var (
	// dach provides a unique data hook generator.
	dach ElementHook

	// gach provide a unique hook to collect all targetted Graphs
	// for a given Statement.
	gach ElementHook

	// wnch contains the next clause hook for where clauses.
	wnch ClauseHook

	// wich contains the initial reset of the working clause hook for where clauses.
	wich ClauseHook

	// wsch contains the where clause subject hook.
	wsch ElementHook

	// wsch contains the where clause subject hook.
	wpch ElementHook

	// wsch contains the where clause subject hook.
	woch ElementHook
)

func init() {
	dach = dataAccumulator(literal.DefaultBuilder())
	gach = graphAccumulator()
	wnch = whereNextWorkingClause()
	wich = whereInitWorkingClause()
	wsch = whereSubjectClause()
	wpch = wherePredicateClause()
	woch = whereObjectClause()
}

// DataAccumulatorHook returns the singleton for data accumulation.
func DataAccumulatorHook() ElementHook {
	return dach
}

// GraphAccumulatorHook return the singleton for graph accumulation.
func GraphAccumulatorHook() ElementHook {
	return gach
}

// WhereInitWorkingClauseHook return the singleton for graph accumulation.
func WhereInitWorkingClauseHook() ClauseHook {
	return wnch
}

// WhereNextWorkingClauseHook return the singleton for graph accumulation.
func WhereNextWorkingClauseHook() ClauseHook {
	return wnch
}

func graphAccumulator() ElementHook {
	var hook ElementHook
	hook = func(st *Statement, ce ConsumedElement) (ElementHook, error) {
		if ce.IsSymbol() {
			return hook, nil
		}
		tkn := ce.Token()
		switch tkn.Type {
		case lexer.ItemComma:
			return hook, nil
		case lexer.ItemBinding:
			st.AddGraph(strings.TrimSpace(tkn.Text))
			return hook, nil
		default:
			return nil, fmt.Errorf("hook.GrapAccumulator requires a binding to refer to a graph, got %v instead", tkn)
		}
	}
	return hook
}

func whereNextWorkingClause() ClauseHook {
	var f ClauseHook
	f = func(stm *Statement, _ Symbol) (ClauseHook, error) {
		stm.AddWorkingGrpahClause()
		return f, nil
	}
	return f
}

func whereInitWorkingClause() ClauseHook {
	var f ClauseHook
	f = func(stm *Statement, _ Symbol) (ClauseHook, error) {
		stm.ResetWorkingGraphClause()
		return f, nil
	}
	return f
}

func whereSubjectClause() ElementHook {
	var (
		f            ElementHook
		lastNopToken *lexer.Token
	)
	f = func(st *Statement, ce ConsumedElement) (ElementHook, error) {
		if ce.IsSymbol() {
			return f, nil
		}
		tkn := ce.Token()
		c := st.WorkingClause()
		if tkn.Type == lexer.ItemNode {
			if c.s != nil {
				return nil, fmt.Errorf("invalid node in where clause that already has a subject; current %v, got %v", c.s, tkn.Type)
			}
			n, err := ToNode(ce)
			if err != nil {
				return nil, err
			}
			c.s = n
			return f, nil
		}
		if tkn.Type == lexer.ItemBinding {
			if lastNopToken.Type == lexer.ItemAs {
				if c.sAlias != "" {
					return nil, fmt.Errorf("AS alias binding for subject has already being assined on %v", st)
				}
				c.sAlias = tkn.Text
				return f, nil
			}
			if lastNopToken.Type == lexer.ItemType {
				if c.sTypeAlias != "" {
					return nil, fmt.Errorf("TYPE alias binding for subject has already being assined on %v", st)
				}
				c.sTypeAlias = tkn.Text
				return f, nil
			}
			if c.sIDAlias == "" && lastNopToken.Type == lexer.ItemID {
				if c.sIDAlias != "" {
					return nil, fmt.Errorf("ID alias binding for subject has already being assined on %v", st)
				}
				c.sIDAlias = tkn.Text
				return f, nil
			}
		}
		lastNopToken = tkn
		return f, nil
	}
	return f
}

func wherePredicateClause() ElementHook {
	var f ElementHook
	f = func(st *Statement, ce ConsumedElement) (ElementHook, error) {
		// TODO(xllora): Implement.
		return nil, errors.New("not implemented")
	}
	return f
}

func whereObjectClause() ElementHook {
	var f ElementHook
	f = func(st *Statement, ce ConsumedElement) (ElementHook, error) {
		// TODO(xllora): Implement.
		return nil, errors.New("not implemented")
	}
	return f
}
