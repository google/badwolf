// Copyright 2015 Google Inc. All rights reserved.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package planner

import (
	"fmt"

	"github.com/google/badwolf/bql/semantic"
	"github.com/google/badwolf/bql/table"
	"github.com/google/badwolf/storage"
	"github.com/google/badwolf/triple"
)

// simpleFetch returns a table containing the data specified by the graph
// clause by querying the provided stora. Will return an error if it had poblems
// retrieveing the data.
func simpleFetch(gs []storage.Graph, cls *semantic.GraphClause) (*table.Table, error) {
	tbl, err := table.New(cls.Bindings())
	if err != nil {
		return nil, err
	}
	if cls.S != nil && cls.P != nil && cls.O != nil {
		// Fully qualified triple.
		// TODO(xllora): Implement.
		return nil, nil
	}
	if cls.S != nil && cls.P != nil && cls.O == nil {
		// SP request.
		// TODO(xllora): Implement.
		return nil, nil
	}
	if cls.S != nil && cls.P == nil && cls.O != nil {
		// SO request.
		// TODO(xllora): Implement.
		return nil, nil
	}
	if cls.S == nil && cls.P != nil && cls.O != nil {
		// PO request.
		// TODO(xllora): Implement.
		return nil, nil
	}
	if cls.S != nil && cls.P == nil && cls.O == nil {
		// S request.
		// TODO(xllora): Implement.
		return nil, nil
	}
	if cls.S == nil && cls.P != nil && cls.O == nil {
		// P request.
		// TODO(xllora): Implement.
		return nil, nil
	}
	if cls.S == nil && cls.P == nil && cls.O != nil {
		// O request.
		// TODO(xllora): Implement.
		return nil, nil
	}
	if cls.S == nil && cls.P == nil && cls.O == nil {
		// Full data request.
		for _, g := range gs {
			ts, err := g.Triples()
			if err != nil {
				return nil, err
			}
			if err := addTriples(ts, cls, tbl); err != nil {
				return nil, err
			}
		}
		return tbl, nil
	}

	return nil, fmt.Errorf("planner.simpleFetch could not recognize request in clause %v", cls)
}

// addTriples add all the retrieved triples from the graphs into the results
// table. The semantic graph clause is also passed to be able to identify what
// bindings to set.
func addTriples(ts storage.Triples, cls *semantic.GraphClause, tbl *table.Table) error {
	for t := range ts {
		r, err := tripleToRow(t, cls)
		if err != nil {
			return err
		}
		tbl.AddRow(r)
	}
	return nil
}

// objectToCell returns a cell containing the data boxed in the object.
func objectToCell(o *triple.Object) (*table.Cell, error) {
	c := &table.Cell{}
	if n, err := o.Node(); err == nil {
		c.N = n
		return c, nil
	}
	if p, err := o.Predicate(); err == nil {
		c.P = p
		return c, nil
	}
	if l, err := o.Literal(); err == nil {
		c.L = l
		return c, nil
	}
	return nil, fmt.Errorf("unknown object type in object %q", o)
}

// tripleToRow converts a triple into a row using the binndings specidfied
// on the graph clause.
func tripleToRow(t *triple.Triple, cls *semantic.GraphClause) (table.Row, error) {
	r, s, p, o := make(table.Row), t.S(), t.P(), t.O()
	if cls.SBinding != "" {
		r[cls.SBinding] = &table.Cell{N: s}
	}
	if cls.SAlias != "" {
		r[cls.SAlias] = &table.Cell{N: s}
	}
	if cls.STypeAlias != "" {
		r[cls.STypeAlias] = &table.Cell{S: s.Type().String()}
	}
	if cls.SIDAlias != "" {
		r[cls.SIDAlias] = &table.Cell{S: s.ID().String()}
	}
	if cls.PBinding != "" {
		r[cls.PBinding] = &table.Cell{P: p}
	}
	if cls.OBinding != "" {
		// Extract the object type.
		if n, err := o.Node(); err == nil {
			r[cls.OBinding] = &table.Cell{N: n}
		} else {
			if p, err := o.Predicate(); err == nil {
				r[cls.OBinding] = &table.Cell{P: p}
			} else {
				if l, err := o.Literal(); err == nil {
					r[cls.OBinding] = &table.Cell{L: l}
				} else {
					return nil, fmt.Errorf("unknown object type in triple %s", t)
				}
			}
		}
	}
	return r, nil
}
