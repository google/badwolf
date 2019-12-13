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
	"reflect"
	"strings"

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
	// LT represents '<'
	LT OP = iota
	// GT represents '>''
	GT
	// EQ represents '=''
	EQ
	// NOT represents 'not'
	NOT
	// AND represents 'and'
	AND
	// OR represents 'or'
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

// evaluationNode represents the internal representation of one expression.
type evaluationNode struct {
	op OP
	lB string
	rB string
}

// comparisonForNodeLiteral represents the internal representation of a expression of comparison between a binding and a node literal.
type comparisonForNodeLiteral struct {
	op OP

	lB  string
	rNL string
}

func (e *comparisonForNodeLiteral) Evaluate(r table.Row) (bool, error) {
	// Binary evaluation
	eval := func() (*table.Cell, error) {
		var (
			eL *table.Cell
			ok bool
		)
		eL, ok = r[e.lB]
		if !ok {
			return nil, fmt.Errorf("comparison operations require the binding value for %q for row %q to exist", e.lB, r)
		}
		return eL, nil
	}

	cs := func(c *table.Cell) string {
		if c.L != nil {
			return strings.TrimSpace(c.L.ToComparableString())
		}
		return strings.TrimSpace(c.String())
	}

	eL, err := eval()
	if err != nil {
		return false, err
	}
	csEL, csER := cs(eL), strings.TrimSpace(e.rNL)
	switch e.op {
	case EQ:
		return reflect.DeepEqual(csEL, csER), nil
	case LT:
		return cs(eL) < csER, nil
	case GT:
		return csEL > csER, nil
	default:
		return false, fmt.Errorf("boolean evaluation require a boolean operation; found %q instead", e.op)
	}
}

// comparisonForLiteralNode represents the internal representation of a expression of comparison between a literal and a binding.
type comparisonForLiteralNode struct {
	op OP

	literalOnLeft bool

	lS string
	rS string
}

func formatCell(c *table.Cell) string {
	if c.L != nil {
		return strings.TrimSpace(c.L.ToComparableString())
	}
	return strings.TrimSpace(c.String())
}

func (e *comparisonForLiteralNode) Evaluate(r table.Row) (bool, error) {
	// Binary evaluation
	getValue := func(binding string) (*table.Cell, error) {
		var (
			val *table.Cell
			ok  bool
		)
		val, ok = r[binding]
		if !ok {
			return nil, fmt.Errorf("comparison operations require the binding value for %q for row %q to exist", binding, r)
		}
		return val, nil
	}

	csLit := func(lit string) (string, error) {
		n, err := litutils.DefaultBuilder().Parse(lit)
		if err != nil {
			return "", err
		}
		return n.ToComparableString(), nil
	}

	leftCell, err := getValue(e.lS)
	if err != nil {
		return false, err
	}

	var (
		csEL, csER string
	)

	csEL = formatCell(leftCell)
	csER, err = csLit(e.rS)
	if err != nil {
		return false, err
	}
	switch e.op {
	case EQ:
		return csEL == csER, nil
	case LT:
		return csEL < csER, nil
	case GT:
		return csEL > csER, nil
	default:
		return false, fmt.Errorf("boolean evaluation require a boolean operation; found %q instead", e.op)
	}
}

// Evaluate the expression.
func (e *evaluationNode) Evaluate(r table.Row) (bool, error) {
	// Binary evaluation
	eval := func() (*table.Cell, *table.Cell, error) {
		var (
			eL, eR *table.Cell
			ok     bool
		)
		eL, ok = r[e.lB]
		if !ok {
			return nil, nil, fmt.Errorf("comparison operations require the binding value for %q for row %q to exist", e.lB, r)
		}
		eR, ok = r[e.rB]
		if !ok {
			return nil, nil, fmt.Errorf("comparison operations require the binding value for %q for row %q to exist", e.rB, r)
		}
		return eL, eR, nil
	}

	eL, eR, err := eval()
	if err != nil {
		return false, err
	}
	csEL, csER := formatCell(eL), formatCell(eR)
	switch e.op {
	case EQ:
		return reflect.DeepEqual(csEL, csER), nil
	case LT:
		return csEL < csER, nil
	case GT:
		return csEL > csER, nil
	default:
		return false, fmt.Errorf("boolean evaluation require a boolen operation; found %q instead", e.op)
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
			op: op,
			lB: lB,
			rB: rB,
		}, nil
	default:
		return nil, errors.New("evaluation expressions require the operation to be one for the following '=', '<', '>'")
	}
}

// NewEvaluationExpressionForLiterals creates a new evaluator for binding and literal.
func NewEvaluationExpressionForLiterals(op OP, lB, rL string) (Evaluator, error) {
	l, r := strings.TrimSpace(lB), strings.TrimSpace(rL)
	if l == "" || r == "" {
		return nil, fmt.Errorf("operands cannot be empty; got %q, %q", l, r)
	}
	switch op {
	case EQ, LT, GT:
		return &comparisonForLiteral{
			op: op,
			lS: l,
			rS: r,
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
			op:  op,
			lB:  l,
			rNL: r,
		}, nil
	default:
		return nil, errors.New("evaluation expressions require the operation to be one for the following '=', '<', '>'")
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
	// Binary evaluation
	eval := func(binary bool) (bool, bool, error) {
		var (
			eL, eR     bool
			errL, errR error
		)
		if !e.lS {
			return false, false, fmt.Errorf("boolean operations require a left operator; found (%q, %q) instead", e.lE, e.rE)
		}
		eL, errL = e.lE.Evaluate(r)
		if errL != nil {
			return false, false, errL
		}
		if binary {
			if !e.rS {
				return false, false, fmt.Errorf("boolean operations require a left operator; found (%q, %q) instead", e.lE, e.rE)
			}
			eR, errR = e.rE.Evaluate(r)
			if errR != nil {
				return false, false, errR
			}
		}
		return eL, eR, nil
	}

	switch e.op {
	case AND:
		eL, eR, err := eval(true)
		if err != nil {
			return false, err
		}
		return eL && eR, nil
	case OR:
		eL, eR, err := eval(true)
		if err != nil {
			return false, err
		}
		return eL || eR, nil
	case NOT:
		eL, _, err := eval(false)
		if err != nil {
			return false, err
		}
		return !eL, nil
	default:
		return false, fmt.Errorf("boolean evaluation require a boolen operation; found %q instead", e.op)
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
		return nil, errors.New("binary boolean expressions require the operation to be one for the follwing 'and', 'or'")
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
		return nil, errors.New("unary boolean expressions require the operation to be one for the follwing 'not'")
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

	// Not token
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

	// Binding token
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

		if bndTkn.Type == lexer.ItemLiteral {
			e, err := NewEvaluationExpressionForLiterals(op, tkn.Text, bndTkn.Text)
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

	// LPar Token
	if tkn.Type == lexer.ItemLPar {
		tailEval, ce, err := internalNewEvaluator(tail)
		if err != nil {
			return nil, nil, err
		}
		if len(ce) < 1 {
			return nil, nil, errors.New("incomplete parentesis expression; missing ')'")
		}
		head, tail = ce[0], ce[1:]
		if head.Token().Type != lexer.ItemRPar {
			return nil, nil, fmt.Errorf("missing right parentesis in expression; found %v instead", head)
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

	var tkns []string
	for _, e := range ce {
		tkns = append(tkns, fmt.Sprintf("%q", e.token.Type))
	}
	return nil, nil, fmt.Errorf("could not create an evaluator for condition {%s}", strings.Join(tkns, ","))
}
