package predicate

import (
	"fmt"
	"strings"
	"time"
)

// Type describes the two type of predicates in BadWolf
type Type uint8

const (
	// Immutable predicates are always valid and not bound to any time anchor.
	Immutable Type = iota
	// Temporal predicates are anchored in the time continuum and valid depending
	// on the reasoning engine and the granularity of the reasoning.
	Temporal
)

// String returns a pretty printed type.
func (t Type) String() string {
	switch t {
	case Immutable:
		return "IMMUTABLE"
	case Temporal:
		return "TEMPORAL"
	default:
		return "UNKNOWN"
	}
}

// ID represents a predicate ID.
type ID string

// String converts a ID to its string form.
func (i *ID) String() string {
	return string(*i)
}

// Predicate represents a BadWolf predicate.
type Predicate struct {
	id     ID
	anchor *time.Time
}

// String returns the pretty printed version of the predicate.
func (p *Predicate) String() string {
	if p.anchor == nil {
		return fmt.Sprintf("%q@[]", p.id)
	}
	return fmt.Sprintf("%q@[%s]", p.id, p.anchor.Format(time.RFC3339Nano))
}

// Parse converts a pretty printed predicate into a predicate.
func Parse(s string) (*Predicate, error) {
	raw := strings.TrimSpace(s)
	if raw == "" {
		return nil, fmt.Errorf("predicate.Parse cannot create predicate from empty string %q", s)
	}
	if raw[0] != '"' {
		return nil, fmt.Errorf("predicate.Parse failed to parse since string does not start with \" in %s", s)
	}
	idx := strings.Index(raw, "\"@[")
	if idx < 0 {
		return nil, fmt.Errorf("predicate.Parse could not find anchor definition in %s", raw)
	}
	id, ta := raw[1:idx], raw[idx+3:len(raw)-1]
	if ta == "" {
		return &Predicate{
			id: ID(id),
		}, nil
	}
	pta, err := time.Parse(time.RFC3339Nano, ta)
	if err != nil {
		return nil, fmt.Errorf("predicate.Parse failed to parse time anchor %s in %s with error %v", ta, raw, err)
	}
	return &Predicate{
		id:     ID(id),
		anchor: &pta,
	}, nil
}

// ID returns the ID of the predicate.
func (p *Predicate) ID() ID {
	return p.id
}

// Type returns the type of the predicate.
func (p *Predicate) Type() Type {
	if p.anchor == nil {
		return Immutable
	}
	return Temporal
}

// TimeAnchor attempts to return the time anchor of a predicate if its type is
// temporal.
func (p *Predicate) TimeAnchor() (*time.Time, error) {
	if p.anchor == nil {
		return nil, fmt.Errorf("predicate.TimeAnchor cannot return anchor for immutable predicate %v", p)
	}
	return p.anchor, nil
}

// NewImmutable creates a new immutable predicate.
func NewImmutable(id string) *Predicate {
	return &Predicate{
		id: ID(id),
	}
}

// NewTemporal creates a new temporal predicate.
func NewTemporal(id string, t time.Time) *Predicate {
	return &Predicate{
		id:     ID(id),
		anchor: &t,
	}
}
