// Package node provides the abstraction to build and use BadWolf nodes.
package node

import (
	"fmt"
	"hash/crc64"
	"os"
	"os/user"
	"strings"
	"time"
)

// Type describes the type of the node.
type Type string

// String converts a type to its string form.
func (t *Type) String() string {
	return string(*t)
}

// Covariant checks if given two types A and B, A covariant B if B _is a_ A.
// In other word, A _covariant_ B if B is a prefix of A.
func (t *Type) Covariant(ot *Type) bool {
	return strings.HasPrefix(t.String(), ot.String())
}

// ID represents a node ID.
type ID string

// String converts a ID to its string form.
func (i *ID) String() string {
	return string(*i)
}

// Node describes a node in a BadWolf graph.
type Node struct {
	t  *Type
	id *ID
}

// Type returns the type of the node.
func (n *Node) Type() *Type {
	return n.t
}

// ID returns the ID of the node.
func (n *Node) ID() *ID {
	return n.id
}

// String returns a pretty printing representation of Node.
func (n *Node) String() string {
	return fmt.Sprintf("%s<%s>", n.t.String(), n.id.String())
}

// Parse returns a node given a pretty printed representation of Node.
func Parse(s string) (*Node, error) {
	raw := strings.TrimSpace(s)
	idx := strings.Index(raw, "<")
	if idx < 0 {
		return nil, fmt.Errorf("node.Parser: invalid format, could not find ID in %v", raw)
	}
	t, err := NewType(raw[:idx])
	if err != nil {
		return nil, fmt.Errorf("node.Parser: invalid type %q, %v", raw[:idx], err)
	}
	if raw[len(raw)-1] != '>' {
		return nil, fmt.Errorf("node.Parser: pretty printing should finish with '>' in %q", raw)
	}
	id, err := NewID(raw[idx+1 : len(raw)-1])
	if err != nil {
		return nil, fmt.Errorf("node.Parser: invalid ID in %q, %v", raw, err)
	}
	return NewNode(t, id), nil
}

// Covariant checks if the types of two nodes is covariant.
func (n *Node) Covariant(on *Node) bool {
	return n.t.Covariant(on.t)
}

// NewType creates a new type from plain string.
func NewType(t string) (*Type, error) {
	if strings.ContainsAny(t, " \t\n\r") {
		return nil, fmt.Errorf("node.NewType(%q) does not allow spaces", t)
	}
	if !strings.HasPrefix(t, "/") || strings.HasSuffix(t, "/") {
		return nil, fmt.Errorf("node.NewType(%q) should start with a '/' and do not end with '/'", t)
	}
	nt := Type(t)
	return &nt, nil
}

// NewID create a new ID from a plain string.
func NewID(id string) (*ID, error) {
	if strings.ContainsAny(id, "<>") {
		return nil, fmt.Errorf("node.NewID(%q) does not allow '<' or '>'", id)
	}
	nID := ID(id)
	return &nID, nil
}

// NewNode returns a new node constructed from a type and an ID.
func NewNode(t *Type, id *ID) *Node {
	return &Node{
		t:  t,
		id: id,
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

// The channel to recover the next unique value used to create a blank node.
var (
	nextVal chan string
	tBlank  Type
)

func init() {
	// Create the hashing function.
	hasher := crc64.New(crc64.MakeTable(crc64.ECMA))
	h := func(s string) uint64 {
		hasher.Reset()
		hasher.Write([]byte(s))
		return hasher.Sum64()
	}

	// Get the current user name.
	osU, err := user.Current()
	u := "UNKNOW"
	if err == nil {
		u = osU.Username
	}

	// Create the constant to make build a unique ID.
	start := uint64(time.Now().UnixNano())
	user := h(u)
	pid := uint64(os.Getpid())
	// Initialize the channel and blank node type.
	nextVal, tBlank = make(chan string), Type("/_")
	go func() {
		cnt := uint64(0)
		for {
			nextVal <- fmt.Sprintf("%x:%x:%x:%x", start, user, pid, cnt)
			cnt++
		}
	}()
}

// NewBlankNode creates a new blank node. The blank node ID is guaranteed to
// be uique in BadWolf.
func NewBlankNode() *Node {
	id := ID(<-nextVal)
	return &Node{
		t:  &tBlank,
		id: &id,
	}
}
