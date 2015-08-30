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
	"regexp"
	"strings"
	"time"

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
	// predicateRegexp contains the regular expression for not fullly defined predicates.
	predicateRegexp *regexp.Regexp

	// boundRegexp contains the regular expression for not fullly defined predicate bounds.
	boundRegexp *regexp.Regexp

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

	predicateRegexp = regexp.MustCompile(`^"(.+)"@\["?([^\]"]*)"?\]$`)
	boundRegexp = regexp.MustCompile(`^"(.+)"@\["?([^\]"]*)"?,"?([^\]"]*)"?\]$`)
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

// WhereSubjectClauseHook returnce the singleton for working clause hooks that
// populates the subject.
func WhereSubjectClauseHook() ElementHook {
	return wsch
}

// WherePredicateClauseHook returnce the singleton for working clause hooks that
// populates the predicate.
func WherePredicateClauseHook() ElementHook {
	return wpch
}

// WhereObjectClauseHook returnce the singleton for working clause hooks that
// populates the object.
func WhereObjectClauseHook() ElementHook {
	return woch
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
			if lastNopToken == nil {
				if c.sBinding != "" {
					return nil, fmt.Errorf("subject binding %q is already set to %q", tkn.Text, c.sBinding)
				}
				c.sBinding = tkn.Text
				return f, nil
			}
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

func processPredicate(c *GraphClause, ce ConsumedElement) error {
	raw := ce.Token().Text
	p, err := predicate.Parse(raw)
	if err == nil {
		// A fully specified predicate was provided.
		c.p = p
		return nil
	}
	// The predicate may have a binding on the anchor.
	cmps := predicateRegexp.FindAllStringSubmatch(raw, 2)
	if len(cmps) != 1 || (len(cmps) == 1 && len(cmps[0]) != 3) {
		return fmt.Errorf("failed to extract partialy defined predicate %s, got %v instead", raw, cmps)
	}
	id, ta := cmps[0][1], cmps[0][2]
	c.pID = id
	if ta != "" {
		c.pAnchorBinding = ta
	}
	return nil
}

func processPredicateBinding(c *GraphClause, ce ConsumedElement, lastNopToken *lexer.Token) error {
	raw := ce.Token().Text
	cmps := boundRegexp.FindAllStringSubmatch(raw, 2)
	if len(cmps) != 1 || (len(cmps) == 1 && len(cmps[0]) != 4) {
		return fmt.Errorf("failed to extract partialy defined predicate bound %s, got %v instead", raw, cmps)
	}
	id, tl, tu := cmps[0][1], cmps[0][2], cmps[0][3]
	c.pID = id
	// Lower bound procssing.
	if tl[0] == '?' {
		c.pLowerBoundAlias = tl
	} else {
		ptl, err := time.Parse(time.RFC3339Nano, tl)
		if err != nil {
			return fmt.Errorf("predicate.Parse failed to parse time anchor %s in %s with error %v", tl, raw, err)
		}
		c.pLowerBound = &ptl
	}
	// Lower bound procssing.
	if tu[0] == '?' {
		c.pLowerBoundAlias = tu
	} else {
		ptu, err := time.Parse(time.RFC3339Nano, tu)
		if err != nil {
			return fmt.Errorf("predicate.Parse failed to parse time anchor %s in %s with error %v", tu, raw, err)
		}
		c.pUpperBound = &ptu
	}
	return nil
}

func wherePredicateClause() ElementHook {
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
		if tkn.Type == lexer.ItemBinding {
			if lastNopToken == nil {
				c.pBinding = tkn.Text
			} else {
				switch lastNopToken.Type {
				case lexer.ItemAs:
					c.pAlias = tkn.Text
				case lexer.ItemID:
					c.pIDAlias = tkn.Text
				case lexer.ItemAt:
					c.pAnchorAlias = tkn.Text
				default:
					return nil, fmt.Errorf("binding %q found after invalid token %s", tkn.Text, lastNopToken.Type)
				}
			}
		}
		if tkn.Type == lexer.ItemPredicate {
			if c.p != nil {
				return nil, fmt.Errorf("invalid predicate %s on graph clause since already set to %s", tkn.Text, c.p)
			}
			if err := processPredicate(c, ce); err != nil {
				return nil, err
			}
		}
		if tkn.Type == lexer.ItemBinding {
			if err := processPredicateBinding(c, ce, lastNopToken); err != nil {
				return nil, err
			}
		}
		lastNopToken = tkn
		return f, nil
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
