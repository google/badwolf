// Copyright 2016 Google Inc. All rights reserved.
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
	"time"

	"github.com/google/badwolf/bql/lexer"
	"github.com/google/badwolf/bql/table"
	"github.com/google/badwolf/triple/literal"
)

// Evaluator interface computes the evaluation of a boolean expression.
type Evaluator interface {
	// Evaluate computes the boolean value of the expression given a certain
	// results table row. It will return an
	// error if it could not be evaluated for the provided table row.
	Evaluate(r table.Row) (bool, error)
}

// AlwaysReturn evaluator always return the provided boolean value.
type AlwaysReturn struct {
	V bool
}

// Evaluate return the provided value.
func (a *AlwaysReturn) Evaluate(r table.Row) (bool, error) {
	return a.V, nil
}

// OP the operation to be use in the expression evaluation.
type OP int8

const (
	// LT represents '<'.
	LT OP = iota
	// GT represents '>'.
	GT
	// EQ represents '='.
	EQ
	// NOT represents 'not'.
	NOT
	// AND represents 'and'.
	AND
	// OR represents 'or'.
	OR
)

// String returns a readable string of the operation.
func (o OP) String() string {
	switch o {
	case LT:
		return "<"
	case GT:
		return ">"
	case EQ:
		return "="
	case NOT:
		return "not"
	case AND:
		return "and"
	case OR:
		return "or"
	default:
		return "@UNKNOWN@"
	}
}

// cellFromRow retrieves the value of a binding from a given row.
func cellFromRow(binding string, r table.Row) (*table.Cell, error) {
	val, ok := r[binding]
	if !ok {
		return nil, fmt.Errorf("value for binding %q not found in row %v", binding, r)
	}
	return val, nil
}

// formatCell formats a given cell into a trimmed comparable string.
func formatCell(c *table.Cell) (string, error) {
	if c.L != nil {
		return strings.TrimSpace(c.L.ToComparableString()), nil
	}
	if c.S != nil {
		formatted, err := literal.DefaultBuilder().Build(literal.Text, *c.S)
		if err != nil {
			return "", fmt.Errorf("formatCell failed, could not build a text literal from the string %q, got error: %v", *c.S, err)
		}
		return strings.TrimSpace(formatted.ToComparableString()), nil
	}
	return strings.TrimSpace(c.String()), nil
}

// evaluationNode represents the internal representation of one expression.
type evaluationNode struct {
	operation OP

	leftBinding  string
	rightBinding string
}

// Evaluate the expression.
func (e *evaluationNode) Evaluate(r table.Row) (bool, error) {
	// Binary evaluation.
	eval := func() (*table.Cell, *table.Cell, error) {
		leftBinding, err := cellFromRow(e.leftBinding, r)
		if err != nil {
			return nil, nil, fmt.Errorf("evaluationNode.Evaluate failed, the call for cellFromRow(%v, %v) returned error: %v", e.leftBinding, r, err)
		}
		rightBinding, err := cellFromRow(e.rightBinding, r)
		if err != nil {
			return nil, nil, fmt.Errorf("evaluationNode.Evaluate failed, the call for cellFromRow(%v, %v) returned error: %v", e.rightBinding, r, err)
		}
		return leftBinding, rightBinding, nil
	}

	leftBinding, rightBinding, err := eval()
	if err != nil {
		return false, err
	}

	// comparable string expressions for left and right tokens.
	var csEL, csER string
	csEL, err = formatCell(leftBinding)
	if err != nil {
		return false, fmt.Errorf("evaluationNode.Evaluate failed, the call for formatCell(%s) returned error: %v", leftBinding, err)
	}
	csER, err = formatCell(rightBinding)
	if err != nil {
		return false, fmt.Errorf("evaluationNode.Evaluate failed, the call for formatCell(%s) returned error: %v", rightBinding, err)
	}

	switch e.operation {
	case EQ:
		return csEL == csER, nil
	case LT:
		return csEL < csER, nil
	case GT:
		return csEL > csER, nil
	default:
		return false, fmt.Errorf("boolean evaluation requires a boolean operation; found %q instead", e.operation)
	}
}

// comparisonForLiteral represents the internal representation of an expression of comparison between a binding and a literal.
type comparisonForLiteral struct {
	operation OP

	leftBinding  string
	rightLiteral string
}

func (e *comparisonForLiteral) Evaluate(r table.Row) (bool, error) {
	leftBinding, err := cellFromRow(e.leftBinding, r)
	if err != nil {
		return false, fmt.Errorf("comparisonForLiteral.Evaluate failed, the call for cellFromRow(%v, %v) returned error: %v", e.leftBinding, r, err)
	}
	if leftBinding.L == nil && leftBinding.S == nil {
		return false, nil
	}

	rightLiteral, err := literal.DefaultBuilder().Parse(e.rightLiteral)
	if err != nil {
		return false, fmt.Errorf("comparisonForLiteral.Evaluate failed, could not parse literal from the string %q, got error: %v", e.rightLiteral, err)
	}
	if leftBinding.S != nil && rightLiteral.Type() != literal.Text {
		return false, fmt.Errorf("a string binding can only be compared with a literal of type text, got literal %q instead", rightLiteral)
	}

	if leftBinding.L != nil && leftBinding.L.Type() != rightLiteral.Type() {
		return false, nil
	}

	// comparable string expressions for left and right tokens.
	var csEL, csER string
	csEL, err = formatCell(leftBinding)
	if err != nil {
		return false, fmt.Errorf("comparisonForLiteral.Evaluate failed, the call for formatCell(%s) returned error: %v", leftBinding, err)
	}
	csER = rightLiteral.ToComparableString()

	switch e.operation {
	case EQ:
		return csEL == csER, nil
	case LT:
		return csEL < csER, nil
	case GT:
		return csEL > csER, nil
	default:
		return false, fmt.Errorf("boolean evaluation requires a boolean operation; found %q instead", e.operation)
	}
}

// comparisonForNodeLiteral represents the internal representation of an expression of comparison between a binding and a node literal.
type comparisonForNodeLiteral struct {
	operation OP

	leftBinding      string
	rightNodeLiteral string
}

func (e *comparisonForNodeLiteral) Evaluate(r table.Row) (bool, error) {
	leftBinding, err := cellFromRow(e.leftBinding, r)
	if err != nil {
		return false, fmt.Errorf("comparisonForNodeLiteral.Evaluate failed, the call for cellFromRow(%v, %v) returned error: %v", e.leftBinding, r, err)
	}
	if leftBinding.S != nil {
		return false, fmt.Errorf("a string binding can only be compared with a literal of type text, got literal %q instead", strings.TrimSpace(e.rightNodeLiteral))
	}
	if leftBinding.N == nil {
		return false, nil
	}

	// comparable string expressions for left and right tokens.
	var csEL, csER string
	csEL, err = formatCell(leftBinding)
	if err != nil {
		return false, fmt.Errorf("comparisonForNodeLiteral.Evaluate failed, the call for formatCell(%s) returned error: %v", leftBinding, err)
	}
	csER = strings.TrimSpace(e.rightNodeLiteral)

	switch e.operation {
	case EQ:
		return csEL == csER, nil
	default:
		return false, fmt.Errorf(`comparisonForNodeLiteral.Evaluate got operation %q, but it accepts only the "=" operation. For ">" and "<" think about extracting bindings with the keywords ID/TYPE and using them for comparisons`, e.operation)
	}
}

// comparisonForTimeLiteral is the internal representation of an expression of comparison between a binding and a time literal.
type comparisonForTimeLiteral struct {
	operation OP

	leftBinding      string
	rightTimeLiteral string
}

func (e *comparisonForTimeLiteral) Evaluate(r table.Row) (bool, error) {
	leftBinding, err := cellFromRow(e.leftBinding, r)
	if err != nil {
		return false, fmt.Errorf("comparisonForTimeLiteral.Evaluate failed, the call for cellFromRow(%v, %v) returned error: %v", e.leftBinding, r, err)
	}
	if leftBinding.S != nil {
		return false, fmt.Errorf("a string binding can only be compared with a literal of type text, got literal %q instead", strings.TrimSpace(e.rightTimeLiteral))
	}
	if leftBinding.T == nil {
		return false, nil
	}

	timeBinding := leftBinding.T
	timeLiteral, err := time.Parse(time.RFC3339Nano, strings.TrimSpace(e.rightTimeLiteral))
	if err != nil {
		return false, fmt.Errorf("comparisonForTimeLiteral.Evaluate failed, could not parse time from the string %q, got error: %v", strings.TrimSpace(e.rightTimeLiteral), err)
	}

	switch e.operation {
	case EQ:
		return timeBinding.Equal(timeLiteral), nil
	case LT:
		return timeBinding.Before(timeLiteral), nil
	case GT:
		return timeBinding.After(timeLiteral), nil
	default:
		return false, fmt.Errorf("boolean evaluation requires a boolean operation; found %q instead", e.operation)
	}
}

// comparisonForPredicateLiteral represents the internal representation of an expression of comparison between a binding and a predicate literal.
type comparisonForPredicateLiteral struct {
	operation OP

	leftBinding           string
	rightPredicateLiteral string
}

func (e *comparisonForPredicateLiteral) Evaluate(r table.Row) (bool, error) {
	leftBinding, err := cellFromRow(e.leftBinding, r)
	if err != nil {
		return false, fmt.Errorf("comparisonForPredicateLiteral.Evaluate failed, the call for cellFromRow(%v, %v) returned error: %v", e.leftBinding, r, err)
	}
	if leftBinding.S != nil {
		return false, fmt.Errorf("a string binding can only be compared with a literal of type text, got literal %q instead", strings.TrimSpace(e.rightPredicateLiteral))
	}
	if leftBinding.P == nil {
		return false, nil
	}

	// comparable string expressions for left and right tokens.
	var csEL, csER string
	csEL, err = formatCell(leftBinding)
	if err != nil {
		return false, fmt.Errorf("comparisonForPredicateLiteral.Evaluate failed, the call for formatCell(%s) returned error: %v", leftBinding, err)
	}
	csER = strings.TrimSpace(e.rightPredicateLiteral)

	switch e.operation {
	case EQ:
		return csEL == csER, nil
	default:
		return false, fmt.Errorf(`comparisonForPredicateLiteral.Evaluate got operation %q, but it accepts only the "=" operation. For ">" and "<" think about extracting bindings with the keywords ID/AT and using them for comparisons`, e.operation)
	}
}

// NewEvaluationExpression creates a new evaluator for two bindings in a row.
func NewEvaluationExpression(op OP, lB, rB string) (Evaluator, error) {
	l, r := strings.TrimSpace(lB), strings.TrimSpace(rB)
	if l == "" || r == "" {
		return nil, fmt.Errorf("bindings cannot be empty; got %q, %q", l, r)
	}
	switch op {
	case EQ, LT, GT:
		return &evaluationNode{
			operation:    op,
			leftBinding:  lB,
			rightBinding: rB,
		}, nil
	default:
		return nil, errors.New("evaluation expressions require the operation to be one for the following '=', '<', '>'")
	}
}

// NewEvaluationExpressionForLiteral creates a new evaluator for binding and literal.
func NewEvaluationExpressionForLiteral(op OP, lB, rL string) (Evaluator, error) {
	l, r := strings.TrimSpace(lB), strings.TrimSpace(rL)
	if l == "" || r == "" {
		return nil, fmt.Errorf("operands cannot be empty; got %q, %q", l, r)
	}
	switch op {
	case EQ, LT, GT:
		return &comparisonForLiteral{
			operation:    op,
			leftBinding:  l,
			rightLiteral: r,
		}, nil
	default:
		return nil, errors.New("evaluation expressions require the operation to be one for the following '=', '<', '>'")
	}
}

// NewEvaluationExpressionForNodeLiteral creates a new evaluator for binding and node literal.
func NewEvaluationExpressionForNodeLiteral(op OP, lB, rNL string) (Evaluator, error) {
	l, r := strings.TrimSpace(lB), strings.TrimSpace(rNL)
	if l == "" || r == "" {
		return nil, fmt.Errorf("operands cannot be empty; got %q, %q", l, r)
	}
	switch op {
	case EQ, LT, GT:
		return &comparisonForNodeLiteral{
			operation:        op,
			leftBinding:      l,
			rightNodeLiteral: r,
		}, nil
	default:
		return nil, errors.New("evaluation expressions require the operation to be one for the following '=', '<', '>'")
	}
}

// NewEvaluationExpressionForTimeLiteral creates a new evaluator for binding and time literal.
func NewEvaluationExpressionForTimeLiteral(op OP, lB, rTL string) (Evaluator, error) {
	l, r := strings.TrimSpace(lB), strings.TrimSpace(rTL)
	if l == "" || r == "" {
		return nil, fmt.Errorf("evaluation expression got operands %q and %q, but operands cannot be empty", l, r)
	}
	switch op {
	case EQ, LT, GT:
		return &comparisonForTimeLiteral{
			operation:        op,
			leftBinding:      l,
			rightTimeLiteral: r,
		}, nil
	default:
		return nil, errors.New("evaluation expressions require the operation to be one of the following: '=', '<', '>'")
	}
}

// NewEvaluationExpressionForPredicateLiteral creates a new evaluator for binding and predicate literal.
func NewEvaluationExpressionForPredicateLiteral(op OP, lB, rTL string) (Evaluator, error) {
	l, r := strings.TrimSpace(lB), strings.TrimSpace(rTL)
	if l == "" || r == "" {
		return nil, fmt.Errorf("evaluation expression got operands %q and %q, but operands cannot be empty", l, r)
	}
	switch op {
	case EQ, LT, GT:
		return &comparisonForPredicateLiteral{
			operation:             op,
			leftBinding:           l,
			rightPredicateLiteral: r,
		}, nil
	default:
		return nil, errors.New("evaluation expressions require the operation to be one of the following: '=', '<', '>'")
	}
}

// booleanNode represents the internal representation of one expression.
type booleanNode struct {
	op OP

	lS bool
	lE Evaluator
	rS bool
	rE Evaluator
}

// Evaluate the expression.
func (e *booleanNode) Evaluate(r table.Row) (bool, error) {
	// Binary evaluation.
	evalLeft := func() (bool, error) {
		if !e.lS {
			return false, fmt.Errorf(`boolean operations require a left operator; found "(%v, %v)" instead`, e.lE, e.rE)
		}
		return e.lE.Evaluate(r)
	}
	evalRight := func() (bool, error) {
		if !e.rS {
			return false, fmt.Errorf(`boolean operations require a right operator; found "(%v, %v)" instead`, e.lE, e.rE)
		}
		return e.rE.Evaluate(r)
	}

	eL, err := evalLeft()
	if err != nil {
		return false, err
	}
	switch e.op {
	case AND:
		// Shortcut evaluation.
		if !eL {
			return false, nil
		}
		return evalRight()
	case OR:
		// Shortcut evaluation.
		if eL {
			return true, nil
		}
		return evalRight()
	case NOT:
		return !eL, nil
	default:
		return false, fmt.Errorf("boolean evaluation requires a boolen operation; found %q instead", e.op)
	}
}

// NewBinaryBooleanExpression creates a new binary boolean evaluator.
func NewBinaryBooleanExpression(op OP, lE, rE Evaluator) (Evaluator, error) {
	switch op {
	case AND, OR:
		return &booleanNode{
			op: op,
			lS: true,
			lE: lE,
			rS: true,
			rE: rE,
		}, nil
	default:
		return nil, errors.New("binary boolean expressions require the operation to be one of the following: 'and', 'or'")
	}
}

// NewUnaryBooleanExpression creates a new unary boolean evaluator.
func NewUnaryBooleanExpression(op OP, lE Evaluator) (Evaluator, error) {
	switch op {
	case NOT:
		return &booleanNode{
			op: op,
			lS: true,
			lE: lE,
			rS: false,
		}, nil
	default:
		return nil, errors.New("unary boolean expressions require the operation to be the following: 'not'")
	}
}

// NewEvaluator construct an evaluator given a sequence of tokens. It will
// return a descriptive error if it could build it properly.
func NewEvaluator(ce []ConsumedElement) (Evaluator, error) {
	e, tailCEs, err := internalNewEvaluator(ce)
	if err != nil {
		return nil, err
	}
	if len(tailCEs) > 1 || (len(tailCEs) == 1 && tailCEs[0].Token().Type != lexer.ItemRPar) {
		return nil, fmt.Errorf("failed to consume all token; left over %v", tailCEs)
	}
	return e, nil
}

// internalNewEvaluator create and evaluation and returns the left overs.
func internalNewEvaluator(ce []ConsumedElement) (Evaluator, []ConsumedElement, error) {
	if len(ce) == 0 {
		return nil, nil, errors.New("cannot create an evaluator from an empty sequence of tokens")
	}
	head, tail := ce[0], ce[1:]
	tkn := head.Token()

	// Not token.
	if tkn.Type == lexer.ItemNot {
		tailEval, tailCEs, err := internalNewEvaluator(tail)
		if err != nil {
			return nil, tailCEs, err
		}
		e, err := NewUnaryBooleanExpression(NOT, tailEval)
		if err != nil {
			return nil, tailCEs, err
		}
		return e, tailCEs, nil
	}

	// Binding token.
	if tkn.Type == lexer.ItemBinding {
		if len(tail) < 2 {
			return nil, nil, fmt.Errorf("cannot create a binary evaluation operand for %v", ce)
		}
		opTkn, bndTkn := tail[0].Token(), tail[1].Token()
		var op OP
		switch opTkn.Type {
		case lexer.ItemEQ:
			op = EQ
		case lexer.ItemLT:
			op = LT
		case lexer.ItemGT:
			op = GT
		default:
			return nil, nil, fmt.Errorf("cannot create a binary evaluation operand for %v", opTkn)
		}

		if bndTkn.Type == lexer.ItemBinding {
			e, err := NewEvaluationExpression(op, tkn.Text, bndTkn.Text)
			if err != nil {
				return nil, nil, err
			}
			var res []ConsumedElement
			if len(tail) > 2 {
				res = tail[2:]
			}
			return e, res, nil
		}

		if bndTkn.Type == lexer.ItemLiteral {
			e, err := NewEvaluationExpressionForLiteral(op, tkn.Text, bndTkn.Text)
			if err != nil {
				return nil, nil, err
			}
			var res []ConsumedElement
			if len(tail) > 2 {
				res = tail[2:]
			}
			return e, res, nil
		}

		if bndTkn.Type == lexer.ItemNode {
			e, err := NewEvaluationExpressionForNodeLiteral(op, tkn.Text, bndTkn.Text)
			if err != nil {
				return nil, nil, err
			}
			var res []ConsumedElement
			if len(tail) > 2 {
				res = tail[2:]
			}
			return e, res, nil
		}

		if bndTkn.Type == lexer.ItemTime {
			e, err := NewEvaluationExpressionForTimeLiteral(op, tkn.Text, bndTkn.Text)
			if err != nil {
				return nil, nil, err
			}
			var res []ConsumedElement
			if len(tail) > 2 {
				res = tail[2:]
			}
			return e, res, nil
		}

		if bndTkn.Type == lexer.ItemPredicate {
			e, err := NewEvaluationExpressionForPredicateLiteral(op, tkn.Text, bndTkn.Text)
			if err != nil {
				return nil, nil, err
			}
			var res []ConsumedElement
			if len(tail) > 2 {
				res = tail[2:]
			}
			return e, res, nil
		}

		return nil, nil, fmt.Errorf("cannot build a binary evaluation operand with right operand %v", bndTkn)
	}

	// LPar Token.
	if tkn.Type == lexer.ItemLPar {
		tailEval, ce, err := internalNewEvaluator(tail)
		if err != nil {
			return nil, nil, err
		}
		if len(ce) < 1 {
			return nil, nil, errors.New("incomplete parenthesis expression; missing ')'")
		}
		head, tail = ce[0], ce[1:]
		if head.Token().Type != lexer.ItemRPar {
			return nil, nil, fmt.Errorf("missing right parenthesis in expression; found %v instead", head)
		}
		if len(tail) > 1 {
			// Binary boolean expression.
			opTkn := tail[0].Token()
			var op OP
			switch opTkn.Type {
			case lexer.ItemAnd:
				op = AND
			case lexer.ItemOr:
				op = OR
			default:
				return nil, nil, fmt.Errorf("cannot create a binary boolean evaluation operand for %v", opTkn)
			}
			rTailEval, ceResTail, err := internalNewEvaluator(tail[1:])
			if err != nil {
				return nil, nil, err
			}
			ev, err := NewBinaryBooleanExpression(op, tailEval, rTailEval)
			if err != nil {
				return nil, nil, err
			}
			return ev, ceResTail, nil
		}
		return tailEval, tail, nil
	}

	tkns := make([]string, 0, len(ce))
	for _, e := range ce {
		tkns = append(tkns, fmt.Sprintf("%q", e.token.Type.String()))
	}
	return nil, nil, fmt.Errorf("could not create an evaluator for condition {%s}", strings.Join(tkns, ","))
}
