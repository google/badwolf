// Package triple implements and allows to manipulate Badwolf triples.
package triple

import (
	"fmt"
	"regexp"
	"strings"

	"badwolf.org/badwolf/triple/literal"
	"badwolf.org/badwolf/triple/node"
	"badwolf.org/badwolf/triple/predicate"
)

// ObjectType describes the type of data boxed in the object
type ObjectType uint8

const (
	// Node type of the boxed element in the object.
	Node ObjectType = iota
	// Literal type of the boxed element in the object.
	Literal
)

// String pretty prints the type of the boxed object.
func (o ObjectType) String() string {
	switch o {
	case Node:
		return "node"
	case Literal:
		return "literal"
	default:
		return "UNKNOWN"
	}
}

// Object is the box that either contains a literal or a node.
type Object struct {
	n *node.Node
	l *literal.Literal
}

// String pretty prints the object.
func (o *Object) String() string {
	if o.n != nil {
		return o.n.String()
	}
	if o.l != nil {
		return o.l.String()
	}
	return "@@@INVALID_OBJECTS@@@"
}

// ParseObject attempts to parse and object.
func ParseObject(s string, b literal.Builder) (*Object, error) {
	n, err := node.Parse(s)
	if err != nil {
		l, err := b.Parse(s)
		if err != nil {
			return nil, err
		}
		return NewLiteralObject(l), nil
	}
	return NewNodeObject(n), nil
}

// NewNodeObject returns a new object that boxes a node.
func NewNodeObject(n *node.Node) *Object {
	return &Object{
		n: n,
	}
}

// NewLiteralObject returns a new object that boxes a literal.
func NewLiteralObject(l *literal.Literal) *Object {
	return &Object{
		l: l,
	}
}

// Triple describes a the <subject predicate object> used by BadWolf.
type Triple struct {
	s *node.Node
	p *predicate.Predicate
	o *Object
}

// NewTriple creates a new triple.
func NewTriple(s *node.Node, p *predicate.Predicate, o *Object) (*Triple, error) {
	if s == nil || p == nil || o == nil {
		return nil, fmt.Errorf("triple.NewTriple cannot create triples from nil components in <%v %v %v>", s, p, o)
	}
	return &Triple{
		s: s,
		p: p,
		o: o,
	}, nil
}

// String marshals the triple into pretty string.
func (t *Triple) String() string {
	return fmt.Sprintf("%s\t%s\t%s", t.s, t.p, t.o)
}

var (
	pSplit *regexp.Regexp
	oSplit *regexp.Regexp
)

func init() {
	pSplit = regexp.MustCompile(">\\s+\"")
	oSplit = regexp.MustCompile("]\\s+/")
}

// ParseTriple process the provided text and tries to create a triple. It asumes
// that the provided text contains only one triple.
func ParseTriple(line string, b literal.Builder) (*Triple, error) {
	raw := strings.TrimSpace(line)
	idxp := pSplit.FindIndex([]byte(raw))
	idxo := oSplit.FindIndex([]byte(raw))
	if len(idxp) == 0 || len(idxo) == 0 {
		return nil, fmt.Errorf("triple.Parse could not split s p o  out of %s", raw)
	}
	ss, sp, so := raw[0:idxp[0]+1], raw[idxp[1]-1:idxo[0]+1], raw[idxo[1]-1:]
	s, err := node.Parse(ss)
	if err != nil {
		return nil, fmt.Errorf("triple.Parse failed to parse subject %s with error %v", ss, err)
	}
	p, err := predicate.Parse(sp)
	if err != nil {
		return nil, fmt.Errorf("triple.Parse failed to parse predicate %s with error %v", sp, err)
	}
	o, err := ParseObject(so, b)
	if err != nil {
		return nil, fmt.Errorf("triple.Parse failed to parse object %s with error %v", so, err)
	}
	return NewTriple(s, p, o)
}
