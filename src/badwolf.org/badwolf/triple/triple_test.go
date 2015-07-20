package triple

import (
	"testing"

	"badwolf.org/badwolf/triple/literal"
	"badwolf.org/badwolf/triple/node"
	"badwolf.org/badwolf/triple/predicate"
)

func getTestData(t *testing.T) (*node.Node, *predicate.Predicate, *Object) {
	s, err := node.Parse("/some/type<some id>")
	if err != nil {
		t.Fatalf("Failed to create test node")
	}
	p, err := predicate.Parse("\"foo\"@[]")
	if err != nil {
		t.Fatalf("Failed to create test predicate")
	}
	o := NewNodeObject(s)
	return s, p, o
}

func TestEmptyTripleFail(t *testing.T) {
	s, p, o := getTestData(t)
	table := []struct {
		s *node.Node
		p *predicate.Predicate
		o *Object
	}{
		{nil, nil, nil},
		{s, nil, nil},
		{nil, p, nil},
		{nil, nil, o},
		{s, p, nil},
		{s, nil, o},
		{nil, p, o},
	}
	for _, tc := range table {
		if tr, err := NewTriple(tc.s, tc.p, tc.o); err == nil {
			t.Errorf("triple.NewTriple should have never created a partial triple as %s", tr)
		}
	}
}

func TestPrettyTriple(t *testing.T) {
	s, p, o := getTestData(t)
	tr, err := NewTriple(s, p, o)
	if err != nil {
		t.Fatalf("triple.NewTriple shoulds not fail to create triple wih error %v", err)
	}
	if got, want := tr.String(), "/some/type<some id>\t\"foo\"@[]\t/some/type<some id>"; got != want {
		t.Errorf("triple.String failed to return a valid prety printed string; got %s, want %s", got, err)
	}
}

func TestParsetriple(t *testing.T) {
	s := "/some/type<some id>\t\"foo\"@[]\t/some/type<some id>"
	if _, err := ParseTriple(s, literal.DefaultBuilder()); err != nil {
		t.Errorf("triple.Parse failed to parse valid triple %s with error %v", s, err)
	}
}
