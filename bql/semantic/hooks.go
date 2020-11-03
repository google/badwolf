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
	"github.com/google/badwolf/bql/planner/filter"
	"github.com/google/badwolf/bql/table"
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

var (
	// predicateRegexp contains the regular expression for not fully defined predicates.
	predicateRegexp = regexp.MustCompile(`^"(.+)"@\["?([^\]"]*)"?\]$`)

	// boundRegexp contains the regular expression for not fully defined predicate bounds.
	boundRegexp = regexp.MustCompile(`^"(.+)"@\["?([^\]"]*)"?,"?([^\]"]*)"?\]$`)
)

// DataAccumulatorHook returns the singleton for data accumulation.
func DataAccumulatorHook() ElementHook {
	return dataAccumulator(literal.DefaultBuilder())
}

// GraphAccumulatorHook returns the singleton for graph accumulation.
func GraphAccumulatorHook() ElementHook {
	return graphAccumulator()
}

// InputGraphAccumulatorHook returns the singleton for input graph accumulation.
func InputGraphAccumulatorHook() ElementHook {
	return inputGraphAccumulator()
}

// OutputGraphAccumulatorHook returns the singleton for output graph accumulation.
func OutputGraphAccumulatorHook() ElementHook {
	return outputGraphAccumulator()
}

// WhereInitWorkingClauseHook returns the singleton for graph accumulation.
func WhereInitWorkingClauseHook() ClauseHook {
	return whereInitWorkingClause()
}

// WhereNextWorkingClauseHook returns the singleton for graph accumulation.
func WhereNextWorkingClauseHook() ClauseHook {
	return whereNextWorkingClause()
}

// WhereSubjectClauseHook returns the singleton for working clause hooks that
// populates the subject.
func WhereSubjectClauseHook() ElementHook {
	return whereSubjectClause()
}

// WherePredicateClauseHook returns the singleton for working clause hooks that
// populates the predicate.
func WherePredicateClauseHook() ElementHook {
	return wherePredicateClause()
}

// WhereObjectClauseHook returns the singleton for working clause hooks that
// populates the object.
func WhereObjectClauseHook() ElementHook {
	return whereObjectClause()
}

// WhereFilterClauseHook returns the singleton for the working filter clause hook that
// populates the filters list.
func WhereFilterClauseHook() ElementHook {
	return whereFilterClause()
}

// VarAccumulatorHook returns the singleton for accumulating variable
// projections.
func VarAccumulatorHook() ElementHook {
	return varAccumulator()
}

// VarBindingsGraphChecker returns the singleton for checking a query statement
// for valid bindings in the select variables.
func VarBindingsGraphChecker() ClauseHook {
	return bindingsGraphChecker()
}

// GroupByBindings returns the singleton for collecting all the group by
// bindings.
func GroupByBindings() ElementHook {
	return groupByBindings()
}

// GroupByBindingsChecker returns the singleton to check that the group by
// bindings are valid.
func GroupByBindingsChecker() ClauseHook {
	return groupByBindingsChecker()
}

// OrderByBindings returns the singleton for collecting all the group by
// bindings.
func OrderByBindings() ElementHook {
	return orderByBindings()
}

// OrderByBindingsChecker returns the singleton to check that the group by
// bindings are valid.
func OrderByBindingsChecker() ClauseHook {
	return orderByBindingsChecker()
}

// HavingExpression returns the singleton to collect the tokens that form the
// having clause.
func HavingExpression() ElementHook {
	return havingExpression()
}

// HavingExpressionBuilder returns the singleton to collect the tokens that form
// the having clause.
func HavingExpressionBuilder() ClauseHook {
	return havingExpressionBuilder()
}

// LimitCollection returns the limit collection hook.
func LimitCollection() ElementHook {
	return limitCollection()
}

// CollectGlobalBounds returns the global temporary bounds hook.
func CollectGlobalBounds() ElementHook {
	return collectGlobalBounds()
}

// InitWorkingConstructClauseHook returns the singleton for clause accumulation within the construct statement.
func InitWorkingConstructClauseHook() ClauseHook {
	return InitWorkingConstructClause()
}

// NextWorkingConstructClauseHook returns the singleton for clause accumulation within the construct statement.
func NextWorkingConstructClauseHook() ClauseHook {
	return NextWorkingConstructClause()
}

// ConstructSubjectHook returns the singleton for populating the subject in the
// working construct clause.
func ConstructSubjectHook() ElementHook {
	return constructSubject()
}

// ConstructPredicateHook returns the singleton for populating the predicate in the
// current predicate-object pair in the working construct clause.
func ConstructPredicateHook() ElementHook {
	return constructPredicate()
}

// ConstructObjectHook returns the singleton for populating the object in the
// current predicate-object pair in the working construct clause.
func ConstructObjectHook() ElementHook {
	return constructObject()
}

// NextWorkingConstructPredicateObjectPairClauseHook returns the singleton for adding the current predicate-object pair
// to the set of predicate-objects pairs within the working construct statement and initializing a new
// working predicate-object pair.
func NextWorkingConstructPredicateObjectPairClauseHook() ClauseHook {
	return NextWorkingConstructPredicateObjectPair()
}

// TypeBindingClauseHook returns a ClauseHook that sets the binding type.
func TypeBindingClauseHook(t StatementType) ClauseHook {
	var hook ClauseHook
	hook = func(s *Statement, _ Symbol) (ClauseHook, error) {
		s.BindType(t)
		return hook, nil
	}
	return hook
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
			return nil, fmt.Errorf("hook.GraphAccumulator requires a binding to refer to a graph, got %v instead", tkn)
		}
	}
	return hook
}

// inputGraphAccumulator returns an element hook that keeps track of the graphs
// listed in a statement.
func inputGraphAccumulator() ElementHook {
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
			st.AddInputGraph(strings.TrimSpace(tkn.Text))
			return hook, nil
		default:
			return nil, fmt.Errorf("hook.InputGraphAccumulator requires a binding to refer to a graph, got %v instead", tkn)
		}
	}
	return hook
}

// outputGraphAccumulator returns an element hook that keeps track of the graphs
// listed in a statement.
func outputGraphAccumulator() ElementHook {
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
			st.AddOutputGraph(strings.TrimSpace(tkn.Text))
			return hook, nil
		default:
			return nil, fmt.Errorf("hook.OutputGraphAccumulator requires a binding to refer to a graph, got %v instead", tkn)
		}
	}
	return hook
}

// whereNextWorkingClause returns a clause hook to close the current graphs
// clause and starts a new working one.
func whereNextWorkingClause() ClauseHook {
	var hook ClauseHook
	hook = func(s *Statement, _ Symbol) (ClauseHook, error) {
		s.AddWorkingGraphClause()
		return hook, nil
	}
	return hook
}

// whereInitWorkingClause initialize a new working graph clause.
func whereInitWorkingClause() ClauseHook {
	var hook ClauseHook
	hook = func(s *Statement, _ Symbol) (ClauseHook, error) {
		s.ResetWorkingGraphClause()
		s.ResetWorkingFilterClause()
		return hook, nil
	}
	return hook
}

// whereSubjectClause returns an element hook that updates the subject
// modifiers on the working graph clause.
func whereSubjectClause() ElementHook {
	var (
		hook         ElementHook
		lastNopToken *lexer.Token
	)
	hook = func(st *Statement, ce ConsumedElement) (ElementHook, error) {
		if ce.IsSymbol() {
			return hook, nil
		}
		tkn := ce.Token()
		c := st.WorkingClause()
		switch tkn.Type {
		case lexer.ItemLBracket:
			lastNopToken = nil
			return hook, nil
		case lexer.ItemRBracket:
			lastNopToken = nil
			return hook, nil
		case lexer.ItemOptional:
			c.Optional = true
			lastNopToken = nil
			return hook, nil
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
			return hook, nil
		case lexer.ItemBinding:
			if lastNopToken == nil {
				if c.SBinding != "" {
					return nil, fmt.Errorf("subject binding %q is already set to %q", tkn.Text, c.SBinding)
				}
				c.SBinding = tkn.Text
				lastNopToken = nil
				return hook, nil
			}
			if lastNopToken.Type == lexer.ItemAs {
				if c.SAlias != "" {
					return nil, fmt.Errorf("AS alias binding for subject has already being assigned on %v", st)
				}
				c.SAlias = tkn.Text
				lastNopToken = nil
				return hook, nil
			}
			if lastNopToken.Type == lexer.ItemType {
				if c.STypeAlias != "" {
					return nil, fmt.Errorf("TYPE alias binding for subject has already being assigned on %v", st)
				}
				c.STypeAlias = tkn.Text
				lastNopToken = nil
				return hook, nil
			}
			if c.SIDAlias == "" && lastNopToken.Type == lexer.ItemID {
				if c.SIDAlias != "" {
					return nil, fmt.Errorf("ID alias binding for subject has already being assigned on %v", st)
				}
				c.SIDAlias = tkn.Text
				lastNopToken = nil
				return hook, nil
			}
		}
		lastNopToken = tkn
		return hook, nil
	}
	return hook
}

// processPredicate parses a consumed element and returns a predicate and its attributes if possible.
func processPredicate(ce ConsumedElement) (*predicate.Predicate, string, string, bool, error) {
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
		return nil, "", "", false, fmt.Errorf("failed to extract partially defined predicate %q, got %v instead", raw, cmps)
	}
	id, ta := cmps[0][1], cmps[0][2]
	pID = id
	if ta != "" {
		pAnchorBinding = ta
		temporal = true
	}
	return nil, pID, pAnchorBinding, temporal, nil
}

// processPredicate parses a consumed element and returns a bound predicate and its attributes if possible.
func processPredicateBound(ce ConsumedElement) (string, string, string, *time.Time, *time.Time, bool, error) {
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
		return "", "", "", nil, nil, false, fmt.Errorf("failed to extract partially defined predicate bound %q, got %v instead", raw, cmps)
	}
	id, tl, tu := cmps[0][1], cmps[0][2], cmps[0][3]
	pID = id
	// Lower bound processing.
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
	// Lower bound processing.
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
		hook         ElementHook
		lastNopToken *lexer.Token
	)
	hook = func(st *Statement, ce ConsumedElement) (ElementHook, error) {
		if ce.IsSymbol() {
			return hook, nil
		}
		tkn := ce.Token()
		c := st.WorkingClause()
		switch tkn.Type {
		case lexer.ItemPredicate:
			lastNopToken = nil
			if c.P != nil {
				return nil, fmt.Errorf("invalid predicate %s on graph clause since already set to %s", tkn.Text, c.P)
			}
			p, pID, pAnchorBinding, pTemporal, err := processPredicate(ce)
			if err != nil {
				return nil, err
			}
			c.P, c.PID, c.PAnchorBinding, c.PTemporal = p, pID, pAnchorBinding, pTemporal
			return hook, nil
		case lexer.ItemPredicateBound:
			lastNopToken = nil
			if c.PLowerBound != nil || c.PUpperBound != nil || c.PLowerBoundAlias != "" || c.PUpperBoundAlias != "" {
				return nil, fmt.Errorf("invalid predicate bound %s on graph clause since already set to %s", tkn.Text, c.P)
			}
			pID, pLowerBoundAlias, pUpperBoundAlias, pLowerBound, pUpperBound, pTemp, err := processPredicateBound(ce)
			if err != nil {
				return nil, err
			}
			c.PID, c.PLowerBoundAlias, c.PUpperBoundAlias, c.PLowerBound, c.PUpperBound, c.PTemporal = pID, pLowerBoundAlias, pUpperBoundAlias, pLowerBound, pUpperBound, pTemp
			return hook, nil
		case lexer.ItemBinding:
			if lastNopToken == nil {
				if c.PBinding != "" {
					return nil, fmt.Errorf("invalid binding %q loose after no valid modifier", tkn.Text)
				}
				c.PBinding = tkn.Text
				return hook, nil
			}
			switch lastNopToken.Type {
			case lexer.ItemAs:
				if c.PAlias != "" {
					return nil, fmt.Errorf("AS alias binding for predicate has already being assigned on %v", st)
				}
				c.PAlias = tkn.Text
			case lexer.ItemID:
				if c.PIDAlias != "" {
					return nil, fmt.Errorf("ID alias binding for predicate has already being assigned on %v", st)
				}
				c.PIDAlias = tkn.Text
			case lexer.ItemAt:
				if c.PAnchorAlias != "" {
					return nil, fmt.Errorf("AT alias binding for predicate has already being assigned on %v", st)
				}
				c.PAnchorAlias = tkn.Text
			default:
				return nil, fmt.Errorf("binding %q found after invalid token %s", tkn.Text, lastNopToken)
			}
			lastNopToken = nil
			return hook, nil
		}
		lastNopToken = tkn
		return hook, nil
	}
	return hook
}

// whereObjectClause returns an element hook that updates the object
// modifiers on the working graph clause.
func whereObjectClause() ElementHook {
	var (
		hook         ElementHook
		lastNopToken *lexer.Token
	)
	hook = func(st *Statement, ce ConsumedElement) (ElementHook, error) {
		if ce.IsSymbol() {
			return hook, nil
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
			return hook, nil
		case lexer.ItemPredicate:
			lastNopToken = nil
			if c.O != nil {
				return nil, fmt.Errorf("invalid predicate %s for object on graph clause since already set to %s", tkn.Text, c.O)
			}
			var (
				pred *predicate.Predicate
				err  error
			)
			pred, c.OID, c.OAnchorBinding, c.OTemporal, err = processPredicate(ce)
			if err != nil {
				return nil, err
			}
			if pred != nil {
				c.O = triple.NewPredicateObject(pred)
			}
			return hook, nil
		case lexer.ItemPredicateBound:
			lastNopToken = nil
			if c.OLowerBound != nil || c.OUpperBound != nil || c.OLowerBoundAlias != "" || c.OUpperBoundAlias != "" {
				return nil, fmt.Errorf("invalid predicate bound %s on graph clause since already set to %s", tkn.Text, c.O)
			}
			oID, oLowerBoundAlias, oUpperBoundAlias, oLowerBound, oUpperBound, oTemp, err := processPredicateBound(ce)
			if err != nil {
				return nil, err
			}
			c.OID, c.OLowerBoundAlias, c.OUpperBoundAlias, c.OLowerBound, c.OUpperBound, c.OTemporal = oID, oLowerBoundAlias, oUpperBoundAlias, oLowerBound, oUpperBound, oTemp
			return hook, nil
		case lexer.ItemBinding:
			if lastNopToken == nil {
				if c.OBinding != "" {
					return nil, fmt.Errorf("object binding %q is already set to %q", tkn.Text, c.SBinding)
				}
				c.OBinding = tkn.Text
				return hook, nil
			}
			defer func() {
				lastNopToken = nil
			}()
			switch lastNopToken.Type {
			case lexer.ItemAs:
				if c.OAlias != "" {
					return nil, fmt.Errorf("AS alias binding for predicate has already being assigned on %v", st)
				}
				c.OAlias = tkn.Text
			case lexer.ItemType:
				if c.OTypeAlias != "" {
					return nil, fmt.Errorf("TYPE alias binding for predicate has already being assigned on %v", st)
				}
				c.OTypeAlias = tkn.Text
			case lexer.ItemID:
				if c.OIDAlias != "" {
					return nil, fmt.Errorf("ID alias binding for predicate has already being assigned on %v", st)
				}
				c.OIDAlias = tkn.Text
			case lexer.ItemAt:
				if c.OAnchorAlias != "" {
					return nil, fmt.Errorf("AT alias binding for predicate has already being assigned on %v", st)
				}
				c.OAnchorAlias = tkn.Text
			default:
				return nil, fmt.Errorf("binding %q found after invalid token %s", tkn.Text, lastNopToken)
			}
			return hook, nil
		}
		lastNopToken = tkn
		return hook, nil
	}
	return hook
}

// addOperationToWorkingFilter takes the filter operation in its string format and tries to add the
// correspondent filter.Operation to workingFilter.
func addOperationToWorkingFilter(op string, workingFilter *FilterClause) error {
	if workingFilter == nil {
		return fmt.Errorf("could not add filter function %q to nil filter clause (which is still nil probably because a call to st.ResetWorkingFilterClause was not made before start processing the first filter clause)", op)
	}
	if !workingFilter.Operation.IsEmpty() {
		return fmt.Errorf("invalid filter function %q on filter clause since already set to %q", op, workingFilter.Operation)
	}
	lowercaseOp := strings.ToLower(op)
	if _, ok := filter.SupportedOperations[lowercaseOp]; !ok {
		return fmt.Errorf("filter function %q on filter clause is not supported", op)
	}

	workingFilter.Operation = filter.SupportedOperations[lowercaseOp]
	return nil
}

// addBindingToWorkingFilter takes the given binding and tries to add it to workingFilter.
func addBindingToWorkingFilter(bndg string, workingFilter *FilterClause) error {
	if workingFilter == nil {
		return fmt.Errorf("could not add binding %q to nil filter clause (which is still nil probably because a call to st.ResetWorkingFilterClause was not made before start processing the first filter clause)", bndg)
	}
	if workingFilter.Binding != "" {
		return fmt.Errorf("invalid binding %q on filter clause since already set to %q", bndg, workingFilter.Binding)
	}

	workingFilter.Binding = bndg
	return nil
}

// addValueToWorkingFilter takes the given value and tries to add it to workingFilter.
func addValueToWorkingFilter(value string, workingFilter *FilterClause) error {
	if workingFilter == nil {
		return fmt.Errorf("could not add value %q to nil filter clause (which is still nil probably because a call to st.ResetWorkingFilterClause was not made before start processing the first filter clause)", value)
	}
	if workingFilter.Value != "" {
		return fmt.Errorf("invalid value %q on filter clause since already set to %q", value, workingFilter.Value)
	}

	workingFilter.Value = value
	return nil
}

// validateFilterClause returns an error if the given filter clause is either invalid or incomplete.
func validateFilterClause(f *FilterClause) error {
	if f == nil {
		return fmt.Errorf("nil filter clause")
	}
	if f.Operation.IsEmpty() {
		return fmt.Errorf("filter clause Operation is missing")
	}
	if f.Binding == "" {
		return fmt.Errorf("filter clause Binding is missing")
	}
	if f.Value == "" && filter.OperationRequiresValue[f.Operation] {
		return fmt.Errorf("filter clause Value is required for filter Operation %q", f.Operation)
	}
	if f.Value != "" && !filter.OperationRequiresValue[f.Operation] {
		return fmt.Errorf("filter clause Value is not required for filter Operation %q", f.Operation)
	}

	return nil
}

// whereFilterClause returns an element hook that updates the working filter clause and,
// if the filter clause is complete, populates the filters list of the statement.
func whereFilterClause() ElementHook {
	var hook ElementHook

	hook = func(st *Statement, ce ConsumedElement) (ElementHook, error) {
		if ce.IsSymbol() {
			return hook, nil
		}

		tkn := ce.Token()
		switch tkn.Type {
		case lexer.ItemFilterFunction:
			err := addOperationToWorkingFilter(tkn.Text, st.WorkingFilter())
			if err != nil {
				return nil, err
			}
			return hook, nil
		case lexer.ItemBinding:
			err := addBindingToWorkingFilter(tkn.Text, st.WorkingFilter())
			if err != nil {
				return nil, err
			}
			return hook, nil
		case lexer.ItemLiteral:
			err := addValueToWorkingFilter(tkn.Text, st.WorkingFilter())
			if err != nil {
				return nil, err
			}
			return hook, nil
		case lexer.ItemRPar:
			if err := validateFilterClause(st.WorkingFilter()); err != nil {
				return nil, fmt.Errorf("could not add invalid working filter %q to the statement filters list: %v", st.WorkingFilter(), err)
			}
			st.AddWorkingFilterClause()
		}

		return hook, nil
	}

	return hook
}

// varAccumulator returns an element hook that updates the object
// modifiers on the working graph clause.
func varAccumulator() ElementHook {
	var (
		hook         ElementHook
		lastNopToken *lexer.Token
	)
	hook = func(st *Statement, ce ConsumedElement) (ElementHook, error) {
		if ce.IsSymbol() {
			return hook, nil
		}
		tkn := ce.Token()
		p := st.WorkingProjection()
		switch tkn.Type {
		case lexer.ItemBinding:
			if p.Binding == "" {
				p.Binding = tkn.Text
			} else {
				if lastNopToken != nil && lastNopToken.Type == lexer.ItemAs {
					p.Alias = tkn.Text
					lastNopToken = nil
					st.AddWorkingProjection()
				} else {
					return nil, fmt.Errorf("invalid token %s for variable projection %s", tkn.Type, p)
				}
			}
		case lexer.ItemAs:
			lastNopToken = tkn
		case lexer.ItemSum, lexer.ItemCount:
			p.OP = tkn.Type
		case lexer.ItemDistinct:
			p.Modifier = tkn.Type
		case lexer.ItemComma:
			st.AddWorkingProjection()
		default:
			lastNopToken = nil
		}
		return hook, nil
	}
	return hook
}

// bindingsGraphChecker validate that all input bindings are provided by the
// graph pattern.
func bindingsGraphChecker() ClauseHook {
	var hook ClauseHook
	hook = func(s *Statement, _ Symbol) (ClauseHook, error) {
		// Force working projection flush.
		s.AddWorkingProjection()
		bs := s.BindingsMap()
		for _, b := range s.InputBindings() {
			if _, ok := bs[b]; !ok {
				return nil, fmt.Errorf("specified binding %s not found in where clause, only %v bindings are available", b, s.Bindings())
			}
		}
		return hook, nil
	}
	return hook
}

// groupByBindings collects the bindings listed in the group by clause.
func groupByBindings() ElementHook {
	var hook ElementHook
	hook = func(st *Statement, ce ConsumedElement) (ElementHook, error) {
		if ce.IsSymbol() {
			return hook, nil
		}
		tkn := ce.Token()
		if tkn.Type == lexer.ItemBinding {
			st.groupBy = append(st.groupBy, tkn.Text)
		}
		return hook, nil
	}
	return hook
}

// groupByBindingsChecker checks that all group by bindings are valid output
// bindings.
func groupByBindingsChecker() ClauseHook {
	var hook ClauseHook
	hook = func(s *Statement, _ Symbol) (ClauseHook, error) {
		// Force working projection flush.
		var idxs map[int]bool
		idxs = make(map[int]bool)
		for _, gb := range s.groupBy {
			found := false
			for idx, prj := range s.projection {
				if gb == prj.Alias || (prj.Alias == "" && gb == prj.Binding) {
					if prj.OP != lexer.ItemError || prj.Modifier != lexer.ItemError {
						return nil, fmt.Errorf("GROUP BY %s binding cannot refer to an aggregation function", gb)
					}
					idxs[idx] = true
					found = true
				}
			}
			if !found {
				return nil, fmt.Errorf("invalid GROUP BY binging %s; available bindings %v", gb, s.OutputBindings())
			}
		}
		for idx, prj := range s.projection {
			if idxs[idx] {
				continue
			}
			if len(s.groupBy) > 0 && prj.OP == lexer.ItemError {
				return nil, fmt.Errorf("Binding %q not listed on GROUP BY requires an aggregation function", prj.Binding)
			}
			if len(s.groupBy) == 0 && prj.OP != lexer.ItemError {
				s := prj.Alias
				if s == "" {
					s = prj.Binding
				}
				return nil, fmt.Errorf("Binding %q with aggregation %s function requires GROUP BY clause", s, prj.OP)
			}
		}
		return hook, nil
	}
	return hook
}

// orderByBindings collects the bindings listed in the order by clause.
func orderByBindings() ElementHook {
	var hook func(st *Statement, ce ConsumedElement) (ElementHook, error)
	hook = func(st *Statement, ce ConsumedElement) (ElementHook, error) {
		if ce.IsSymbol() {
			return hook, nil
		}
		tkn := ce.Token()
		switch tkn.Type {
		case lexer.ItemBinding:
			st.orderBy = append(st.orderBy, table.SortConfig{{Binding: tkn.Text}}...)
		case lexer.ItemAsc:
			st.orderBy[len(st.orderBy)-1].Desc = false
		case lexer.ItemDesc:
			st.orderBy[len(st.orderBy)-1].Desc = true
		}
		return hook, nil
	}
	return hook
}

// orderByBindingsChecker checks that all order by bindings are valid output
// bindings.
func orderByBindingsChecker() ClauseHook {
	var hook ClauseHook
	hook = func(s *Statement, _ Symbol) (ClauseHook, error) {
		// Force working projection flush.
		outs := make(map[string]bool)
		for _, out := range s.OutputBindings() {
			outs[out] = true
		}
		seen, dups := make(map[string]bool), false
		for _, cfg := range s.orderBy {
			// Check there are no contradictions
			if b, ok := seen[cfg.Binding]; ok {
				if b != cfg.Desc {
					return nil, fmt.Errorf("inconsisting sorting direction for %q binding", cfg.Binding)
				}
				dups = true
			} else {
				seen[cfg.Binding] = cfg.Desc
			}
			// Check that the binding exist.
			if _, ok := outs[cfg.Binding]; !ok {
				return nil, fmt.Errorf("order by binding %q unknown; available bindings are %v", cfg.Binding, s.OutputBindings())
			}
		}
		// If dups exist rewrite the order by SortConfig.
		if dups {
			s.orderBy = table.SortConfig{}
			for b, d := range seen {
				s.orderBy = append(s.orderBy, table.SortConfig{{Binding: b, Desc: d}}...)
			}
		}
		return hook, nil
	}
	return hook
}

// havingExpression collects the tokens that form the HAVING clause.
func havingExpression() ElementHook {
	var hook ElementHook
	hook = func(st *Statement, ce ConsumedElement) (ElementHook, error) {
		if ce.IsSymbol() {
			return hook, nil
		}
		if ce.token.Type != lexer.ItemHaving {
			st.havingExpression = append(st.havingExpression, ce)
		}
		return hook, nil
	}
	return hook
}

// havingExpressionBuilder given the collected tokens that forms the having
// clause expression, it builds the expression to use when filtering values
// on the final result table.
func havingExpressionBuilder() ClauseHook {
	var hook ClauseHook
	hook = func(s *Statement, _ Symbol) (ClauseHook, error) {
		s.havingExpressionEvaluator = &AlwaysReturn{V: true}
		if len(s.havingExpression) > 0 {
			eval, err := NewEvaluator(s.havingExpression)
			if err != nil {
				return nil, err
			}
			s.havingExpressionEvaluator = eval
		}
		return hook, nil
	}
	return hook
}

// limitCollection collects the limit of rows to return as indicated by the
// LIMIT clause.
func limitCollection() ElementHook {
	var hook ElementHook
	hook = func(st *Statement, ce ConsumedElement) (ElementHook, error) {
		if ce.IsSymbol() || ce.token.Type == lexer.ItemLimit {
			return hook, nil
		}
		if ce.token.Type != lexer.ItemLiteral {
			return nil, fmt.Errorf("limit clause required an int64 literal; found %v instead", ce.token)
		}
		l, err := literal.DefaultBuilder().Parse(ce.token.Text)
		if err != nil {
			return nil, fmt.Errorf("failed to parse limit literal %q with error %v", ce.token.Text, err)
		}
		if l.Type() != literal.Int64 {
			return nil, fmt.Errorf("limit required an int64 value; found %s instead", l)
		}
		lv, err := l.Int64()
		if err != nil {
			return nil, fmt.Errorf("failed to retrieve the int64 value for literal %v with error %v", l, err)
		}
		st.limitSet, st.limit = true, lv
		return hook, nil
	}
	return hook
}

// collectGlobalBounds collects the global time bounds that should be applied
// to all temporal predicates.
func collectGlobalBounds() ElementHook {
	var (
		hook      ElementHook
		opToken   *lexer.Token
		lastToken *lexer.Token
	)
	hook = func(st *Statement, ce ConsumedElement) (ElementHook, error) {
		if ce.IsSymbol() {
			return hook, nil
		}
		tkn := ce.token
		switch tkn.Type {
		case lexer.ItemBefore, lexer.ItemAfter, lexer.ItemBetween:
			if lastToken != nil {
				return nil, fmt.Errorf("invalid token %v after already valid token %v", tkn, lastToken)
			}
			opToken, lastToken = tkn, tkn
		case lexer.ItemComma:
			if lastToken == nil || opToken.Type != lexer.ItemBetween {
				return nil, fmt.Errorf("token %v can only be used in a between clause; previous token %v instead", tkn, lastToken)
			}
			lastToken = tkn
		case lexer.ItemTime:
			if lastToken == nil {
				return nil, fmt.Errorf("invalid token %v without a global time modifier", tkn)
			}
			ta, err := time.Parse(time.RFC3339Nano, strings.TrimSpace(tkn.Text))
			if err != nil {
				return nil, fmt.Errorf("failed to parse global time bound in %s with error: %s", tkn.Text, err)
			}
			if lastToken.Type == lexer.ItemComma || lastToken.Type == lexer.ItemBefore {
				st.lookupOptions.UpperAnchor = &ta
				opToken, lastToken = nil, nil
			} else {
				st.lookupOptions.LowerAnchor = &ta
				if opToken.Type != lexer.ItemBetween {
					opToken, lastToken = nil, nil
				}
			}
		case lexer.ItemPredicateBound:
			bounds := strings.Split(strings.TrimSpace(tkn.Text), ",")
			if len(bounds) != 2 {
				return nil, fmt.Errorf("wrong number of bounds in predicate %s; want 2 got %d", tkn.Text, len(bounds))
			}
			lowBound, err := time.Parse(time.RFC3339Nano, strings.TrimSpace(bounds[0]))
			if err != nil {
				return nil, fmt.Errorf("failed to parse lower time bound in %s with error: %s", tkn.Text, err)
			}
			upBound, err := time.Parse(time.RFC3339Nano, strings.TrimSpace(bounds[1]))
			if err != nil {
				return nil, fmt.Errorf("failed to parse upper time bound in %s with error: %s", tkn.Text, err)
			}
			st.lookupOptions.LowerAnchor = &lowBound
			st.lookupOptions.UpperAnchor = &upBound
		default:
			return nil, fmt.Errorf("global bound found unexpected token %v", tkn)
		}
		return hook, nil
	}
	return hook
}

// InitWorkingConstructClause returns a clause hook to initialize a new working
// construct clause.
func InitWorkingConstructClause() ClauseHook {
	var hook ClauseHook
	hook = func(s *Statement, _ Symbol) (ClauseHook, error) {
		s.ResetWorkingConstructClause()
		return hook, nil
	}
	return hook
}

// NextWorkingConstructClause returns a clause hook to close the current working
// construct clause and start a new working construct clause.
func NextWorkingConstructClause() ClauseHook {
	var hook ClauseHook
	hook = func(s *Statement, _ Symbol) (ClauseHook, error) {
		s.AddWorkingConstructClause()
		return hook, nil
	}
	return hook
}

// constructSubject returns an element hook that updates the subject
// modifiers on the working construct clause.
func constructSubject() ElementHook {
	var hook ElementHook
	hook = func(st *Statement, ce ConsumedElement) (ElementHook, error) {
		if ce.IsSymbol() {
			return hook, nil
		}
		tkn := ce.Token()
		c := st.WorkingConstructClause()
		if c.S != nil {
			return nil, fmt.Errorf("invalid subject %v in construct clause, subject already set to %v", tkn.Type, c.S)
		}
		if c.SBinding != "" {
			return nil, fmt.Errorf("invalid subject %v in construct clause, subject already set to %v", tkn.Type, c.SBinding)
		}
		switch tkn.Type {
		case lexer.ItemNode, lexer.ItemBlankNode:
			n, err := ToNode(ce)
			if err != nil {
				return nil, err
			}
			c.S = n
		case lexer.ItemBinding:
			c.SBinding = tkn.Text
		}
		return hook, nil
	}
	return hook
}

// constructPredicate returns an element hook that updates the predicate
// modifiers on the current predicate-object pair of the working graph clause.
func constructPredicate() ElementHook {
	var hook ElementHook
	hook = func(st *Statement, ce ConsumedElement) (ElementHook, error) {
		if ce.IsSymbol() {
			return hook, nil
		}
		tkn := ce.Token()
		p := st.WorkingConstructClause().WorkingPredicateObjectPair()
		if p.P != nil {
			return nil, fmt.Errorf("invalid predicate %v in construct clause, predicate already set to %v", tkn.Type, p.P)
		}
		if p.PID != "" {
			return nil, fmt.Errorf("invalid predicate %v in construct clause, predicate already set to %v", tkn.Type, p.PID)
		}
		if p.PBinding != "" {
			return nil, fmt.Errorf("invalid predicate %v in construct clause, predicate already set to %v", tkn.Type, p.PBinding)
		}
		switch tkn.Type {
		case lexer.ItemPredicate:
			pred, pID, pAnchorBinding, pTemporal, err := processPredicate(ce)
			if err != nil {
				return nil, err
			}
			p.P, p.PID, p.PAnchorBinding, p.PTemporal = pred, pID, pAnchorBinding, pTemporal
		case lexer.ItemBinding:
			p.PBinding = tkn.Text
		}
		return hook, nil
	}
	return hook
}

// constructObject returns an element hook that updates the object
// modifiers on the current predicate-object pair of the working graph clause.
func constructObject() ElementHook {
	var hook ElementHook
	hook = func(st *Statement, ce ConsumedElement) (ElementHook, error) {
		if ce.IsSymbol() {
			return hook, nil
		}
		tkn := ce.Token()
		p := st.WorkingConstructClause().WorkingPredicateObjectPair()
		if p.O != nil {
			return nil, fmt.Errorf("invalid object %v in construct clause, object already set to %v", tkn.Text, p.O)
		}
		if p.OID != "" {
			return nil, fmt.Errorf("invalid object %v in construct clause, object already set to %v", tkn.Type, p.OID)
		}
		if p.OBinding != "" {
			return nil, fmt.Errorf("invalid object %v in construct clause, object already set to %v", tkn.Type, p.OBinding)
		}
		switch tkn.Type {
		case lexer.ItemNode, lexer.ItemBlankNode, lexer.ItemLiteral:
			obj, err := triple.ParseObject(tkn.Text, literal.DefaultBuilder())
			if err != nil {
				return nil, err
			}
			p.O = obj
		case lexer.ItemPredicate:
			var (
				pred *predicate.Predicate
				err  error
			)
			pred, p.OID, p.OAnchorBinding, p.OTemporal, err = processPredicate(ce)
			if err != nil {
				return nil, err
			}
			if pred != nil {
				p.O = triple.NewPredicateObject(pred)
			}
		case lexer.ItemBinding:
			p.OBinding = tkn.Text
		}
		return hook, nil
	}
	return hook
}

// NextWorkingConstructPredicateObjectPair returns a clause hook to close the current
// predicate-object pair and start a new predicate-object pair within the working
// construct clause.
func NextWorkingConstructPredicateObjectPair() ClauseHook {
	var hook ClauseHook
	hook = func(s *Statement, _ Symbol) (ClauseHook, error) {
		s.WorkingConstructClause().AddWorkingPredicateObjectPair()
		return hook, nil
	}
	return hook
}

// ShowClauseHook returns a clause hook for the show statement.
func ShowClauseHook() ClauseHook {
	var hook ClauseHook
	hook = func(s *Statement, _ Symbol) (ClauseHook, error) {
		s.sType = Show
		return hook, nil
	}
	return hook
}
