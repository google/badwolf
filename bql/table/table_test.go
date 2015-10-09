package table

import "testing"

func TestNew(t *testing.T) {
	testTable := []struct {
		bs  []string
		err bool
	}{
		{[]string{}, false},
		{[]string{"?foo"}, false},
		{[]string{"?foo", "?bar"}, false},
		{[]string{"?foo", "?bar"}, false},
		{[]string{"?foo", "?bar", "?foo", "?bar"}, true},
	}
	for _, entry := range testTable {
		if _, err := New(entry.bs); (err == nil) == entry.err {
			t.Errorf("table.Name failed; want %v for %v ", entry.err, entry.bs)
		}
	}
}
