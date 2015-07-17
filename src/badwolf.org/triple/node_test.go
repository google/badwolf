package node

import "testing"

func TestNewID(t *testing.T) {
	if wID, err := NewID("<"); err == nil {
		t.Errorf("NewID(\"<\") should have never validated ID %v", wID)
	}
	if wID, err := NewID("<"); err == nil {
		t.Errorf("NewID(\"<\") should have never validated ID %v", wID)
	}
	id, err := NewID("some_id")
	if err != nil {
		t.Errorf("NewID(\"some_id\") failed with error %v", err)
	}
	if got, want := id.String(), "some_id"; got != want {
		t.Errorf("NewID did not create a valid ID; got %v, want %v", got, want)
	}
}

func TestNewType(t *testing.T) {
	table := []struct {
		v   string
		msg string
	}{
		{"foo", "NewType should have never create a Type for a string that does not start with '/'"},
		{"/foo/", "NewType should have never create a Type for a string that ends with '/'"},
		{"/foo ", "NewType should have never create a Type for a string that contains ' '"},
		{"/foo\t", "NewType should have never create a Type for a string that contains '\\t'"},
		{"/foo\n", "NewType should have never create a Type for a string that contains '\\n'"},
		{"/foo\r", "NewType should have never create a Type for a string that contains '\\r'"},
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
		t.Errorf("NewType(\"/some/type\") should never fail with error %v", err)
	}
	tB, err := NewType("/some/type/a")
	if err != nil {
		t.Errorf("NewType(\"/some/type/a\") should never fail with error %v", err)
	}
	if tA.Covariant(tB) {
		t.Errorf("Covariant: %q should not be market as covariant of %q", tA, tB)
	}
	if !tB.Covariant(tA) {
		t.Errorf("Covariant: %q should not be market as covariant of %q", tB, tA)
	}
}

func TestNewNodeFromString(t *testing.T) {
	nA, err := NewNodeFromStrings("/some/type", "id_1")
	if err != nil {
		t.Errorf("NewNodeFromStrings(\"/some/type\") should never fail with error %v", err)
	}
	if got, want := nA.String(), "/some/type<id_1>"; got != want {
		t.Errorf("New created node does not conform with the format; got %q, want %q", got, want)
	}
	nB, err := NewNodeFromStrings("/some/type/a", "id_2")
	if err != nil {
		t.Errorf("NewNodeFromStrings(\"/some/type/a\") should never fail with error %v", err)
	}
	if got, want := nB.String(), "/some/type/a<id_2>"; got != want {
		t.Errorf("New created node does not conform with the format; got %q, want %q", got, want)
	}
	if nA.Covariant(nB) {
		t.Errorf("Covariant: %q should not be market as covariant of %q", nA, nB)
	}
	if !nB.Covariant(nA) {
		t.Errorf("Covariant: %q should not be market as covariant of %q", nB, nA)
	}
}
