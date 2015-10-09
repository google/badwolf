// Package table export the table that contains the results of a BQL query.
package table

import "fmt"

// Table contains the results of a BQL query.
type Table struct {
	bs  []string
	mbs map[string]bool
}

// New returns a new table that can hold data for the the given bindings. The,
// table creation will fail if there are repeated bindings.
func New(bs []string) (*Table, error) {
	m := make(map[string]bool)
	for _, b := range bs {
		m[b] = true
	}
	if len(m) != len(bs) {
		return nil, fmt.Errorf("table.New does not allow duplicated bindings in %s", bs)
	}
	return &Table{
		bs:  bs,
		mbs: m,
	}, nil
}
