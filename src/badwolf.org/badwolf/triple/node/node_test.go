package node

import "testing"

func TestNewID(t *testing.T) {
	if wID, err := NewID("<"); err == nil {
		t.Errorf("node.NewID(\"<\") should have never validated ID %v", wID)
	}
	if wID, err := NewID("<"); err == nil {
		t.Errorf("node.NewID(\"<\") should have never validated ID %v", wID)
	}
	id, err := NewID("some_id")
	if err != nil {
		t.Errorf("node.NewID(\"some_id\") failed with error %v", err)
	}
	if got, want := id.String(), "some_id"; got != want {
		t.Errorf("node.NewID did not create a valid ID; got %v, want %v", got, want)
	}
}

func TestNewType(t *testing.T) {
	table := []struct {
		v   string
		msg string
	}{
		{"foo", "node.NewType should have never create a Type for a string that does not start with '/'"},
		{"/foo/", "node.NewType should have never create a Type for a string that ends with '/'"},
		{"/foo ", "node.NewType should have never create a Type for a string that contains ' '"},
		{"/foo\t", "node.NewType should have never create a Type for a string that contains '\\t'"},
		{"/foo\n", "node.NewType should have never create a Type for a string that contains '\\n'"},
		{"/foo\r", "node.NewType should have never create a Type for a string that contains '\\r'"},
	}
	for _, c := range table {
		if _, err := NewType(c.v); err == nil {
			t.Error(c.msg)
		}
	}
}

func TestNewTypeString(t *testing.T) {
	tA, err := NewType("/some/type")
	if err != nil {
		t.Errorf("node.NewType(\"/some/type\") should never fail with error %v", err)
	}
	tB, err := NewType("/some/type/a")
	if err != nil {
		t.Errorf("node.NewType(\"/some/type/a\") should never fail with error %v", err)
	}
	if tA.Covariant(tB) {
		t.Errorf("node.Covariant: %q should not be market as covariant of %q", tA, tB)
	}
	if !tB.Covariant(tA) {
		t.Errorf("node.Covariant: %q should not be market as covariant of %q", tB, tA)
	}
}

func TestNewNodeFromString(t *testing.T) {
	nA, err := NewNodeFromStrings("/some/type", "id_1")
	if err != nil {
		t.Errorf("node.NewNodeFromStrings(\"/some/type\") should never fail with error %v", err)
	}
	if got, want := nA.String(), "/some/type<id_1>"; got != want {
		t.Errorf("node.String new created node does not conform with the format; got %q, want %q", got, want)
	}
	nB, err := NewNodeFromStrings("/some/type/a", "id_2")
	if err != nil {
		t.Errorf("node.NewNodeFromStrings(\"/some/type/a\") should never fail with error %v", err)
	}
	if got, want := nB.String(), "/some/type/a<id_2>"; got != want {
		t.Errorf("node.String new created node does not conform with the format; got %q, want %q", got, want)
	}
	if nA.Covariant(nB) {
		t.Errorf("node.Covariant: %q should not be market as covariant of %q", nA, nB)
	}
	if !nB.Covariant(nA) {
		t.Errorf("node.h Covariant: %q should not be market as covariant of %q", nB, nA)
	}
}

func TestParse(t *testing.T) {
	table := []struct {
		s string
		v bool
	}{
		// Valid text nodes.
		{"/foo<123>", true},
		// Invalid text nodes.
	}
	for _, tc := range table {
		_, err := Parse(tc.s)
		if tc.v && err != nil {
			t.Errorf("node.Parse: failed to parse %q; %v", tc.s, err)
		}
		if !tc.v && err == nil {
			t.Errorf("node.Parse: failed to reject invalid %q", tc.s)
		}
	}
}
