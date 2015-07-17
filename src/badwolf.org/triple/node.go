// Package node deines BadWolf nodes.
package node

import (
	"fmt"
	"strings"
)

// Type describes the type of the node.
type Type struct {
	t string
}

// String returns a pretty printing representation of type.
func (t *Type) String() string {
	return t.t
}

// Covariant checks if given two types A and B, A covariant B if B _is a_ A.
// In other word, A _covariant_ B if B is a prefix of A.
func (t *Type) Covariant(ot *Type) bool {
	return strings.HasPrefix(t.t, ot.t)
}

// ID represents a node ID.
type ID struct {
	id string
}

// String returns a pretty printing representation of ID.
func (i *ID) String() string {
	return i.id
}

// Node describes a node in a BadWolf graph.
type Node struct {
	Type *Type
	ID   *ID
}

// String returns a pretty printing representation of Node.
func (n *Node) String() string {
	return fmt.Sprintf("%s<%s>", n.Type, n.ID)
}

// Covariant checks if the types of two nodes is covariant.
func (n *Node) Covariant(on *Node) bool {
	return n.Type.Covariant(on.Type)
}

// NewType creates a new type from plain string.
func NewType(t string) (*Type, error) {
	if strings.ContainsAny(t, " \t\n\r") {
		return nil, fmt.Errorf("NewType(%q) does not allow spaces", t)
	}
	if !strings.HasPrefix(t, "/") || strings.HasSuffix(t, "/") {
		return nil, fmt.Errorf("NewType(%q) should start with a '/' and do not end with '/'", t)
	}
	return &Type{
		t: t,
	}, nil
}

// NewID create a new ID from a plain string.
func NewID(id string) (*ID, error) {
	if strings.ContainsAny(id, "<>") {
		return nil, fmt.Errorf("NewID(%q) does not allow '<' or '>'", id)
	}
	return &ID{
		id: id,
	}, nil
}

// NewNode returns a new node constructed from a type and an ID.
func NewNode(t *Type, id *ID) *Node {
	return &Node{
		Type: t,
		ID:   id,
	}
}

// NewNodeFromStrings returns a new node constructed from a type and ID
// represented as plain strings.
func NewNodeFromStrings(sT, sID string) (*Node, error) {
	t, err := NewType(sT)
	if err != nil {
		return nil, err
	}
	n, err := NewID(sID)
	if err != nil {
		return nil, err
	}
	return NewNode(t, n), nil
}
