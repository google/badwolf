package literal

import "fmt"

// Type represents the type contained in a literal.
type Type uint8

const (
	// Bool indicates that the type contained in the literal is a bool.
	Bool Type = iota
	// Int64 indicates that the type contained in the literal is an int64.
	Int64
	// Float64 indicates that the type contained in the literal is a float64.
	Float64
	// Text indicates that the type contained in the literal is a string.
	Text
	// Blob indicates that the type contained in the literal is a []byte.
	Blob
)

// Strings returns the pretty printing version of the type
func (t Type) String() string {
	switch t {
	case Bool:
		return "bool"
	case Int64:
		return "int64"
	case Float64:
		return "float64"
	case Text:
		return "text"
	case Blob:
		return "blob"
	default:
		return "UNKNOWN"
	}
}

// Value represents the value contained in the literal.
type Value interface {
	Bool() (bool, error)
	Int64() (int64, error)
	Float64() (float64, error)
	Text() (string, error)
	Blob() ([]byte, error)
	Interface() interface{}
}

// Literal is a data container for arbitrary immutable data.
type Literal interface {
	Value
	Type() Type
}

// Builder interface provides a standar way to build literals given a type and
// a given value.
type Builder interface {
	Build(t Type, v interface{}) (Literal, error)
}

// A singleton used to build all literals.
var defaultBuilder Builder

func init() {
	defaultBuilder = &unboundBuilder{}
}

// The deatuls bilder is unbound. This allows to create a literal arbitrarily
// long.
type unboundBuilder struct{}

// The implementation of all literals.
type literal struct {
	t Type
	v interface{}
}

// Type returns the type of a literal.
func (l *literal) Type() Type {
	return l.t
}

// String eturns a string representation of the literal.
func (l *literal) String() string {
	return fmt.Sprintf("\"%v\"^^type:%v", l.Interface(), l.Type())
}

// Bool returns the value of a literal as a boolean.
func (l *literal) Bool() (bool, error) {
	if l.t != Bool {
		return false, fmt.Errorf("literal is of type %v; cannot be converted to a bool", l.t)
	}
	return l.v.(bool), nil
}

// Int64 returns the value of a literal as an int64.
func (l *literal) Int64() (int64, error) {
	if l.t != Int64 {
		return 0, fmt.Errorf("literal is of type %v; cannot be converted to a int64", l.t)
	}
	return l.v.(int64), nil
}

// Float64 returns the value of a literal as a float64.
func (l *literal) Float64() (float64, error) {
	if l.t != Float64 {
		return 0, fmt.Errorf("literal is of type %v; cannot be converted to a flaot64", l.t)
	}
	return l.v.(float64), nil
}

// Text returns the value of a literal as a string.
func (l *literal) Text() (string, error) {
	if l.t != Text {
		return "", fmt.Errorf("literal is of type %v; cannot be converted to a string", l.t)
	}
	return l.v.(string), nil
}

// Blob returns the value of a literal as a []byte.
func (l *literal) Blob() ([]byte, error) {
	if l.t != Blob {
		return nil, fmt.Errorf("literal is of type %v; cannot be converted to a []byte", l.t)
	}
	return l.v.([]byte), nil
}

// Interface returns the value as a simple interface{}.
func (l *literal) Interface() interface{} {
	return l.v
}

// Build creates a new unboud literal from a type and a value.
func (b *unboundBuilder) Build(t Type, v interface{}) (Literal, error) {
	switch v.(type) {
	case bool:
		if t != Bool {
			return nil, fmt.Errorf("type %s does not match type of value %v", t, v)
		}
	case int64:
		if t != Int64 {
			return nil, fmt.Errorf("type %s does not match type of value %v", t, v)
		}
	case float64:
		if t != Float64 {
			return nil, fmt.Errorf("type %s does not match type of value %v", t, v)
		}
	case string:
		if t != Text {
			return nil, fmt.Errorf("type %s does not match type of value %v", t, v)
		}
	case []byte:
		if t != Blob {
			return nil, fmt.Errorf("type %s does not match type of value %v", t, v)
		}
	default:
		return nil, fmt.Errorf("type %s is not supported when building literals", t)
	}
	return &literal{
		t: t,
		v: v,
	}, nil
}

// DefaultBuilder returns a builder with no constraints or checks.
func DefaultBuilder() Builder {
	return defaultBuilder
}

// boundedBuilder implements a literal builder where strings and blobs are
// guaranteed of being of bounded size
type boundedBuilder struct {
	max int
}

// Build creates a new literal of bounded size.
func (b *boundedBuilder) Build(t Type, v interface{}) (Literal, error) {
	switch v.(type) {
	case string:
		if l := len(v.(string)); l > b.max {
			return nil, fmt.Errorf("cannot create literal due to size of %v (%d>%d)", v, l, b.max)
		}
	case []byte:
		if l := len(v.([]byte)); l > b.max {
			return nil, fmt.Errorf("cannot create literal due to size of %v (%d>%d)", v, l, b.max)
		}
	}
	return defaultBuilder.Build(t, v)
}

// NewBoundedBuilder creates a builder that that guarantess that no literal will
// be created if the size of the string or a blob is bigger than the provided
// maximum.
func NewBoundedBuilder(max int) Builder {
	return &boundedBuilder{max: max}
}
