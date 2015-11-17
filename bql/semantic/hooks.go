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
			trpl, err := triple.New(s, p, o)
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

	// wpch contains the where clause subject hook.
	wpch ElementHook

	// woch contains the where clause subject hook.
	woch ElementHook

	//vach contains the variable accumulator hook.
	vach ElementHook
)

func init() {
	dach = dataAccumulator(literal.DefaultBuilder())
	gach = graphAccumulator()
	wnch = whereNextWorkingClause()
	wich = whereInitWorkingClause()
	wsch = whereSubjectClause()
	wpch = wherePredicateClause()
	woch = whereObjectClause()
	vach = varAccumulator()

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

// varAccumulatorHook returnce the singleton for working clause hooks that
// populates the object.
func varAccumulatorHook() ElementHook {
	return vach
}

// graphAccumulator returns an element hook that keeps track of the graphs
// listed in a statement.
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

// whereNextWorkingClause returns a clause hook to close the current graphs
// clause and starts a new working one.
func whereNextWorkingClause() ClauseHook {
	var f ClauseHook
	f = func(stm *Statement, _ Symbol) (ClauseHook, error) {
		stm.AddWorkingGrpahClause()
		return f, nil
	}
	return f
}

// whereInitWorkingClause initialize a new working graph clause.
func whereInitWorkingClause() ClauseHook {
	var f ClauseHook
	f = func(stm *Statement, _ Symbol) (ClauseHook, error) {
		stm.ResetWorkingGraphClause()
		return f, nil
	}
	return f
}

// whereSubjectClause returns an element hook that updates the subject
// modifiers on the working graph clause.
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
		switch tkn.Type {
		case lexer.ItemNode:
			if c.S != nil {
				return nil, fmt.Errorf("invalid node in where clause that already has a subject; current %v, got %v", c.S, tkn.Type)
			}
			n, err := ToNode(ce)
			if err != nil {
				return nil, err
			}
			c.S = n
			lastNopToken = nil
			return f, nil
		case lexer.ItemBinding:
			if lastNopToken == nil {
				if c.SBinding != "" {
					return nil, fmt.Errorf("subject binding %q is already set to %q", tkn.Text, c.SBinding)
				}
				c.SBinding = tkn.Text
				lastNopToken = nil
				return f, nil
			}
			if lastNopToken.Type == lexer.ItemAs {
				if c.SAlias != "" {
					return nil, fmt.Errorf("AS alias binding for subject has already being assined on %v", st)
				}
				c.SAlias = tkn.Text
				lastNopToken = nil
				return f, nil
			}
			if lastNopToken.Type == lexer.ItemType {
				if c.STypeAlias != "" {
					return nil, fmt.Errorf("TYPE alias binding for subject has already being assined on %v", st)
				}
				c.STypeAlias = tkn.Text
				lastNopToken = nil
				return f, nil
			}
			if c.SIDAlias == "" && lastNopToken.Type == lexer.ItemID {
				if c.SIDAlias != "" {
					return nil, fmt.Errorf("ID alias binding for subject has already being assined on %v", st)
				}
				c.SIDAlias = tkn.Text
				lastNopToken = nil
				return f, nil
			}
		}
		lastNopToken = tkn
		return f, nil
	}
	return f
}

// processPredicate updates the working graph clause if threre is an available
// predcicate.
func processPredicate(c *GraphClause, ce ConsumedElement, lastNopToken *lexer.Token) (*predicate.Predicate, string, string, bool, error) {
	var (
		nP             *predicate.Predicate
		pID            string
		pAnchorBinding string
		temporal       bool
	)
	raw := ce.Token().Text
	p, err := predicate.Parse(raw)
	if err == nil {
		// A fully specified predicate was provided.
		nP = p
		return nP, pID, pAnchorBinding, nP.Type() == predicate.Temporal, nil
	}
	// The predicate may have a binding on the anchor.
	cmps := predicateRegexp.FindAllStringSubmatch(raw, 2)
	if len(cmps) != 1 || (len(cmps) == 1 && len(cmps[0]) != 3) {
		return nil, "", "", false, fmt.Errorf("failed to extract partialy defined predicate %q, got %v instead", raw, cmps)
	}
	id, ta := cmps[0][1], cmps[0][2]
	pID = id
	if ta != "" {
		pAnchorBinding = ta
		temporal = true
	}
	return nil, pID, pAnchorBinding, temporal, nil
}

// processPredicateBound updates the working graph clause if threre is an
// available predcicate bound.
func processPredicateBound(c *GraphClause, ce ConsumedElement, lastNopToken *lexer.Token) (string, string, string, *time.Time, *time.Time, bool, error) {
	var (
		pID              string
		pLowerBoundAlias string
		pUpperBoundAlias string
		pLowerBound      *time.Time
		pUpperBound      *time.Time
	)
	raw := ce.Token().Text
	cmps := boundRegexp.FindAllStringSubmatch(raw, 2)
	if len(cmps) != 1 || (len(cmps) == 1 && len(cmps[0]) != 4) {
		return "", "", "", nil, nil, false, fmt.Errorf("failed to extract partialy defined predicate bound %q, got %v instead", raw, cmps)
	}
	id, tl, tu := cmps[0][1], cmps[0][2], cmps[0][3]
	pID = id
	// Lower bound procssing.
	if strings.Index(tl, "?") != -1 {
		pLowerBoundAlias = tl
	} else {
		stl := strings.TrimSpace(tl)
		if stl != "" {
			ptl, err := time.Parse(time.RFC3339Nano, stl)
			if err != nil {
				return "", "", "", nil, nil, false, fmt.Errorf("predicate.Parse failed to parse time anchor %s in %s with error %v", tl, raw, err)
			}
			pLowerBound = &ptl
		}
	}
	// Lower bound procssing.
	if strings.Index(tu, "?") != -1 {
		pUpperBoundAlias = tu
	} else {
		stu := strings.TrimSpace(tu)
		if stu != "" {
			ptu, err := time.Parse(time.RFC3339Nano, stu)
			if err != nil {
				return "", "", "", nil, nil, false, fmt.Errorf("predicate.Parse failed to parse time anchor %s in %s with error %v", tu, raw, err)
			}
			pUpperBound = &ptu
		}
	}
	if pLowerBound != nil && pUpperBound != nil {
		if pLowerBound.After(*pUpperBound) {
			lb, up := pLowerBound.Format(time.RFC3339Nano), pUpperBound.Format(time.RFC3339Nano)
			return "", "", "", nil, nil, false, fmt.Errorf("invalid time bound; lower bound %s after upper bound %s", lb, up)
		}
	}
	return pID, pLowerBoundAlias, pUpperBoundAlias, pLowerBound, pUpperBound, true, nil
}

// wherePredicateClause returns an element hook that updates the predicate
// modifiers on the working graph clause.
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
		switch tkn.Type {
		case lexer.ItemPredicate:
			lastNopToken = nil
			if c.P != nil {
				return nil, fmt.Errorf("invalid predicate %s on graph clause since already set to %s", tkn.Text, c.P)
			}
			p, pID, pAnchorBinding, pTemporal, err := processPredicate(c, ce, lastNopToken)
			if err != nil {
				return nil, err
			}
			c.P, c.PID, c.PAnchorBinding, c.PTemporal = p, pID, pAnchorBinding, pTemporal
			return f, nil
		case lexer.ItemPredicateBound:
			lastNopToken = nil
			if c.PLowerBound != nil || c.PUpperBound != nil || c.PLowerBoundAlias != "" || c.PUpperBoundAlias != "" {
				return nil, fmt.Errorf("invalid predicate bound %s on graph clause since already set to %s", tkn.Text, c.P)
			}
			pID, pLowerBoundAlias, pUpperBoundAlias, pLowerBound, pUpperBound, pTemp, err := processPredicateBound(c, ce, lastNopToken)
			if err != nil {
				return nil, err
			}
			c.PID, c.PLowerBoundAlias, c.PUpperBoundAlias, c.PLowerBound, c.PUpperBound, c.PTemporal = pID, pLowerBoundAlias, pUpperBoundAlias, pLowerBound, pUpperBound, pTemp
			return f, nil
		case lexer.ItemBinding:
			if lastNopToken == nil {
				if c.PBinding != "" {
					return nil, fmt.Errorf("invalid binding %q loose after no valid modifier", tkn.Text)
				}
				c.PBinding = tkn.Text
				return f, nil
			}
			switch lastNopToken.Type {
			case lexer.ItemAs:
				if c.PAlias != "" {
					return nil, fmt.Errorf("AS alias binding for predicate has already being assined on %v", st)
				}
				c.PAlias = tkn.Text
			case lexer.ItemID:
				if c.PIDAlias != "" {
					return nil, fmt.Errorf("ID alias binding for predicate has already being assined on %v", st)
				}
				c.PIDAlias = tkn.Text
			case lexer.ItemAt:
				if c.PAnchorAlias != "" {
					return nil, fmt.Errorf("AT alias binding for predicate has already being assined on %v", st)
				}
				c.PAnchorAlias = tkn.Text
			default:
				return nil, fmt.Errorf("binding %q found after invalid token %s", tkn.Text, lastNopToken)
			}
			lastNopToken = nil
			return f, nil
		}
		lastNopToken = tkn
		return f, nil
	}
	return f
}

// whereObjectClause returns an element hook that updates the object
// modifiers on the working graph clause.
func whereObjectClause() ElementHook {
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
		switch tkn.Type {
		case lexer.ItemNode, lexer.ItemLiteral:
			lastNopToken = nil
			if c.O != nil {
				return nil, fmt.Errorf("invalid object %s for object on graph clause since already set to %s", tkn.Text, c.O)
			}
			obj, err := triple.ParseObject(tkn.Text, literal.DefaultBuilder())
			if err != nil {
				return nil, err
			}
			c.O = obj
			return f, nil
		case lexer.ItemPredicate:
			lastNopToken = nil
			if c.O != nil {
				return nil, fmt.Errorf("invalid predicate %s for object on graph clause since already set to %s", tkn.Text, c.O)
			}
			var (
				pred *predicate.Predicate
				err  error
			)
			pred, c.OID, c.OAnchorBinding, c.OTemporal, err = processPredicate(c, ce, lastNopToken)
			if err != nil {
				return nil, err
			}
			if pred != nil {
				c.O = triple.NewPredicateObject(pred)
			}
			return f, nil
		case lexer.ItemPredicateBound:
			lastNopToken = nil
			if c.OLowerBound != nil || c.OUpperBound != nil || c.OLowerBoundAlias != "" || c.OUpperBoundAlias != "" {
				return nil, fmt.Errorf("invalid predicate bound %s on graph clause since already set to %s", tkn.Text, c.O)
			}
			oID, oLowerBoundAlias, oUpperBoundAlias, oLowerBound, oUpperBound, oTemp, err := processPredicateBound(c, ce, lastNopToken)
			if err != nil {
				return nil, err
			}
			c.OID, c.OLowerBoundAlias, c.OUpperBoundAlias, c.OLowerBound, c.OUpperBound, c.OTemporal = oID, oLowerBoundAlias, oUpperBoundAlias, oLowerBound, oUpperBound, oTemp
			return f, nil
		case lexer.ItemBinding:
			if lastNopToken == nil {
				if c.OBinding != "" {
					return nil, fmt.Errorf("object binding %q is already set to %q", tkn.Text, c.SBinding)
				}
				c.OBinding = tkn.Text
				return f, nil
			}
			defer func() {
				lastNopToken = nil
			}()
			switch lastNopToken.Type {
			case lexer.ItemAs:
				if c.OAlias != "" {
					return nil, fmt.Errorf("AS alias binding for predicate has already being assined on %v", st)
				}
				c.OAlias = tkn.Text
			case lexer.ItemType:
				if c.OTypeAlias != "" {
					return nil, fmt.Errorf("TYPE alias binding for predicate has already being assined on %v", st)
				}
				c.OTypeAlias = tkn.Text
			case lexer.ItemID:
				if c.OIDAlias != "" {
					return nil, fmt.Errorf("ID alias binding for predicate has already being assined on %v", st)
				}
				c.OIDAlias = tkn.Text
			case lexer.ItemAt:
				if c.OAnchorAlias != "" {
					return nil, fmt.Errorf("AT alias binding for predicate has already being assined on %v", st)
				}
				c.OAnchorAlias = tkn.Text
			default:
				return nil, fmt.Errorf("binding %q found after invalid token %s", tkn.Text, lastNopToken)
			}
			return f, nil
		}
		lastNopToken = tkn
		return f, nil
	}
	return f
}

// whereObjectClause returns an element hook that updates the object
// modifiers on the working graph clause.
func varAccumulator() ElementHook {
	return func(st *Statement, ce ConsumedElement) (ElementHook, error) {
		return nil, nil
	}
}
