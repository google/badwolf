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
	"context"
	"fmt"
	"io"
	"reflect"
	"sync"

	"github.com/google/badwolf/bql/planner/tracer"
	"github.com/google/badwolf/bql/semantic"
	"github.com/google/badwolf/bql/table"
	"github.com/google/badwolf/storage"
	"github.com/google/badwolf/triple"
	"github.com/google/badwolf/triple/node"
	"github.com/google/badwolf/triple/predicate"
)

// updateTimeBounds updates the time bounds use for the lookup based on the
// provided graph clause.
func updateTimeBounds(lo *storage.LookupOptions, cls *semantic.GraphClause) *storage.LookupOptions {
	nlo := &storage.LookupOptions{
		MaxElements: lo.MaxElements,
		LowerAnchor: lo.LowerAnchor,
		UpperAnchor: lo.UpperAnchor,
	}
	if cls.PLowerBound != nil {
		if lo.LowerAnchor == nil || (lo.LowerAnchor != nil && cls.PLowerBound.After(*lo.LowerAnchor)) {
			nlo.LowerAnchor = cls.PLowerBound
		}
	}
	if cls.PUpperBound != nil {
		if lo.UpperAnchor == nil || (lo.UpperAnchor != nil && cls.PUpperBound.Before(*lo.UpperAnchor)) {
			nlo.UpperAnchor = cls.PUpperBound
		}
	}
	return nlo
}

// updateTimeBoundsForRow updates the time bounds use for the lookup based on
// the provided graph clause.
func updateTimeBoundsForRow(lo *storage.LookupOptions, cls *semantic.GraphClause, r table.Row) (*storage.LookupOptions, error) {
	lo = updateTimeBounds(lo, cls)
	if cls.PLowerBoundAlias != "" {
		v, ok := r[cls.PLowerBoundAlias]
		if ok && v.T == nil {
			return nil, fmt.Errorf("invalid time anchor value %v for bound %s", v, cls.PLowerBoundAlias)
		}
		if lo.LowerAnchor == nil || (lo.LowerAnchor != nil && v.T.After(*lo.LowerAnchor)) {
			lo.LowerAnchor = v.T
		}
	}
	if cls.PUpperBoundAlias != "" {
		v, ok := r[cls.PUpperBoundAlias]
		if ok && v.T == nil {
			return nil, fmt.Errorf("invalid time anchor value %v for bound %s", v, cls.PUpperBoundAlias)
		}
		if lo.UpperAnchor == nil || (lo.UpperAnchor != nil && v.T.After(*lo.UpperAnchor)) {
			lo.UpperAnchor = v.T
		}
	}
	nlo := updateTimeBounds(lo, cls)
	return nlo, nil
}

// simpleExist returns true if the triple exist. Return the unfeasible state,
// the table and the error if present.
func simpleExist(ctx context.Context, gs []storage.Graph, cls *semantic.GraphClause, t *triple.Triple) (bool, *table.Table, error) {
	unfeasible := true
	tbl, err := table.New(cls.Bindings())
	if err != nil {
		return true, nil, err
	}
	for _, g := range gs {
		b, err := g.Exist(ctx, t)
		if err != nil {
			return true, nil, err
		}
		if b {
			unfeasible = false
			ts := make(chan *triple.Triple, 1)
			ts <- t
			close(ts)
			if err := addTriples(ts, cls, tbl); err != nil {
				return true, nil, err
			}
		}
	}
	return unfeasible, tbl, nil
}

// simpleFetch returns a table containing the data specified by the graph
// clause by querying the provided stora. Will return an error if it had poblems
// retrieveing the data.
func simpleFetch(ctx context.Context, gs []storage.Graph, cls *semantic.GraphClause, lo *storage.LookupOptions, stmLimit int64, chanSize int, w io.Writer) (*table.Table, error) {
	s, p, o := cls.S, cls.P, cls.O
	lo = updateTimeBounds(lo, cls)
	tbl, err := table.New(cls.Bindings())
	if err != nil {
		return nil, err
	}
	if s != nil && p != nil && o != nil {
		// Fully qualified triple.
		t, err := triple.New(s, p, o)
		if err != nil {
			return nil, err
		}
		tracer.Trace(w, func() []string {
			return []string{fmt.Sprintf("g.Exist(%v, %v)", t, lo)}
		})
		for _, g := range gs {
			b, err := g.Exist(ctx, t)
			if err != nil {
				return nil, err
			}
			if b {
				ts := make(chan *triple.Triple, 1)
				ts <- t
				close(ts)
				if err := addTriples(ts, cls, tbl); err != nil {
					return nil, err
				}
			}
		}
		return tbl, nil
	}
	if s != nil && p != nil && o == nil {
		// SP request.
		for _, g := range gs {
			var (
				oErr error
				aErr error
				lErr error
				wg   sync.WaitGroup
			)
			tracer.Trace(w, func() []string {
				return []string{fmt.Sprintf("g.Objects(%v, %v, %v)", s, p, lo)}
			})
			wg.Add(2)
			os := make(chan *triple.Object, chanSize)
			go func() {
				defer wg.Done()
				oErr = g.Objects(ctx, s, p, lo, os)
			}()
			ts := make(chan *triple.Triple, chanSize)
			go func() {
				defer wg.Done()
				aErr = addTriples(ts, cls, tbl)
			}()
			for o := range os {
				if lErr != nil {
					// Drain the channel to avoid leaking goroutines.
					continue
				}
				t, err := triple.New(s, p, o)
				if err != nil {
					lErr = err
					continue
				}
				ts <- t
			}
			close(ts)
			wg.Wait()
			if oErr != nil {
				return nil, oErr
			}
			if aErr != nil {
				return nil, aErr
			}
			if lErr != nil {
				return nil, lErr
			}
		}
		return tbl, nil
	}
	if s != nil && p == nil && o != nil {
		// SO request.
		for _, g := range gs {
			var (
				pErr error
				aErr error
				lErr error
				wg   sync.WaitGroup
			)
			tracer.Trace(w, func() []string {
				return []string{fmt.Sprintf("g.PredicatesForSubjectAndObject(%v, %v, %v)", s, o, lo)}
			})
			wg.Add(2)
			ps := make(chan *predicate.Predicate, chanSize)
			go func() {
				defer wg.Done()
				pErr = g.PredicatesForSubjectAndObject(ctx, s, o, lo, ps)
			}()
			ts := make(chan *triple.Triple, chanSize)
			go func() {
				defer wg.Done()
				aErr = addTriples(ts, cls, tbl)
			}()
			for p := range ps {
				if lErr != nil {
					// Drain the channel to avoid leaking goroutines.
					continue
				}
				t, err := triple.New(s, p, o)
				if err != nil {
					lErr = err
					continue
				}
				ts <- t
			}
			close(ts)
			wg.Wait()
			if pErr != nil {
				return nil, pErr
			}
			if aErr != nil {
				return nil, aErr
			}
			if lErr != nil {
				return nil, lErr
			}
		}
		return tbl, nil
	}
	if s == nil && p != nil && o != nil {
		// PO request.
		for _, g := range gs {
			var (
				pErr error
				aErr error
				lErr error
				wg   sync.WaitGroup
			)
			tracer.Trace(w, func() []string {
				return []string{fmt.Sprintf("g.Subjects(%v, %v, %v)", p, o, lo)}
			})
			wg.Add(2)
			ss := make(chan *node.Node, chanSize)
			go func() {
				defer wg.Done()
				pErr = g.Subjects(ctx, p, o, lo, ss)
			}()
			ts := make(chan *triple.Triple, chanSize)
			go func() {
				defer wg.Done()
				aErr = addTriples(ts, cls, tbl)
			}()
			for s := range ss {
				if lErr != nil {
					// Drain the channel to avoid leaking goroutines.
					continue
				}
				t, err := triple.New(s, p, o)
				if err != nil {
					lErr = err
					continue
				}
				ts <- t
			}
			close(ts)
			wg.Wait()
			if pErr != nil {
				return nil, pErr
			}
			if aErr != nil {
				return nil, aErr
			}
			if lErr != nil {
				return nil, lErr
			}
		}
		return tbl, nil
	}
	if s != nil && p == nil && o == nil {
		// S request.
		for _, g := range gs {
			var (
				tErr error
				aErr error
				wg   sync.WaitGroup
			)
			tracer.Trace(w, func() []string {
				return []string{fmt.Sprintf("g.TriplesForSubject(%v, %v)", s, lo)}
			})
			ts := make(chan *triple.Triple, chanSize)
			wg.Add(1)
			go func() {
				defer wg.Done()
				tErr = g.TriplesForSubject(ctx, s, lo, ts)
			}()
			aErr = addTriples(ts, cls, tbl)
			wg.Wait()
			if tErr != nil {
				return nil, tErr
			}
			if aErr != nil {
				return nil, aErr
			}
		}
		return tbl, nil
	}
	if s == nil && p != nil && o == nil {
		// P request.
		for _, g := range gs {
			var (
				tErr error
				aErr error
				wg   sync.WaitGroup
			)
			tracer.Trace(w, func() []string {
				return []string{fmt.Sprintf("g.TriplesForPredicate(%v, %v)", p, lo)}
			})
			ts := make(chan *triple.Triple, chanSize)
			wg.Add(1)
			go func() {
				defer wg.Done()
				tErr = g.TriplesForPredicate(ctx, p, lo, ts)
			}()
			aErr = addTriples(ts, cls, tbl)
			wg.Wait()
			if tErr != nil {
				return nil, tErr
			}
			if aErr != nil {
				return nil, aErr
			}
		}
		return tbl, nil
	}
	if s == nil && p == nil && o != nil {
		// O request.
		for _, g := range gs {
			var (
				tErr error
				wg   sync.WaitGroup
			)
			tracer.Trace(w, func() []string {
				return []string{fmt.Sprintf("g.TriplesForObject(%v, %v)", o, lo)}
			})
			ts := make(chan *triple.Triple, chanSize)
			wg.Add(1)
			go func() {
				defer wg.Done()
				tErr = g.TriplesForObject(ctx, o, lo, ts)
			}()
			aErr := addTriples(ts, cls, tbl)
			wg.Wait()
			if tErr != nil {
				return nil, tErr
			}
			if aErr != nil {
				return nil, aErr
			}
		}
		return tbl, nil
	}
	if s == nil && p == nil && o == nil {
		// Full data request.
		for _, g := range gs {
			var (
				tErr error
				aErr error
				wg   sync.WaitGroup
			)
			tracer.Trace(w, func() []string {
				return []string{fmt.Sprintf("g.Triples(%v)", lo)}
			})
			ts := make(chan *triple.Triple, chanSize)
			wg.Add(1)
			go func() {
				defer wg.Done()
				// Push global limit down.
				nlo := *lo
				if stmLimit > 0 {
					nlo.MaxElements = int(stmLimit)
				}
				tErr = g.Triples(ctx, &nlo, ts)
			}()
			aErr = addTriples(ts, cls, tbl)
			wg.Wait()
			if tErr != nil {
				return nil, tErr
			}
			if aErr != nil {
				return nil, aErr
			}
		}
		return tbl, nil
	}

	return nil, fmt.Errorf("planner.simpleFetch could not recognize request in clause %v", cls)
}

// addTriples add all the retrieved triples from the graphs into the results
// table. The semantic graph clause is also passed to be able to identify what
// bindings to set.
func addTriples(ts <-chan *triple.Triple, cls *semantic.GraphClause, tbl *table.Table) error {
	for t := range ts {
		if cls.PID != "" {
			// The triples need to be filtered.
			if string(t.Predicate().ID()) != cls.PID {
				continue
			}
			if cls.PTemporal {
				if t.Predicate().Type() != predicate.Temporal {
					continue
				}
				ta, err := t.Predicate().TimeAnchor()
				if err != nil {
					return fmt.Errorf("failed to retrieve time anchor from time predicate in triple %s with error %v", t, err)
				}
				// Need to check the bounds of the triple.
				if cls.PLowerBound != nil && cls.PLowerBound.After(*ta) {
					continue
				}
				if cls.PUpperBound != nil && cls.PUpperBound.Before(*ta) {
					continue
				}
			}
		}
		if cls.OID != "" {
			if p, err := t.Object().Predicate(); err == nil {
				// The triples need to be filtered.
				if string(p.ID()) != cls.OID {
					continue
				}
				if cls.OTemporal {
					if p.Type() != predicate.Temporal {
						continue
					}
					ta, err := p.TimeAnchor()
					if err != nil {
						return fmt.Errorf("failed to retrieve time anchor from time predicate in triple %s with error %v", t, err)
					}
					// Need to check the bounds of the triple.
					if cls.OLowerBound != nil && cls.OLowerBound.After(*ta) {
						continue
					}
					if cls.OUpperBound != nil && cls.OUpperBound.Before(*ta) {
						continue
					}
				}
			}
		}
		r, err := tripleToRow(t, cls)
		if err != nil {
			return err
		}
		if r != nil {
			tbl.AddRow(r)
		}
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
	r, s, p, o := make(table.Row), t.Subject(), t.Predicate(), t.Object()

	// Enforce binding validity inside te clause.
	bnd := make(map[string]*table.Cell)
	validBinding := func(k string, v *table.Cell) bool {
		c, ok := bnd[k]
		bnd[k] = v
		if !ok {
			return true
		}
		if reflect.DeepEqual(c, v) {
			return true
		}
		return false
	}

	// Subject related bindings.
	if cls.SBinding != "" {
		c := &table.Cell{N: s}
		r[cls.SBinding] = c
		if !validBinding(cls.SBinding, c) {
			return nil, nil
		}
	}
	if cls.SAlias != "" {
		c := &table.Cell{N: s}
		r[cls.SAlias] = c
		if !validBinding(cls.SAlias, c) {
			return nil, nil
		}
	}
	if cls.STypeAlias != "" {
		c := &table.Cell{S: table.CellString(s.Type().String())}
		r[cls.STypeAlias] = c
		if !validBinding(cls.STypeAlias, c) {
			return nil, nil
		}
	}
	if cls.SIDAlias != "" {
		c := &table.Cell{S: table.CellString(s.ID().String())}
		r[cls.SIDAlias] = c
		if !validBinding(cls.SIDAlias, c) {
			return nil, nil
		}
	}

	// Predicate related bindings.
	if cls.PBinding != "" {
		c := &table.Cell{P: p}
		r[cls.PBinding] = c
		if !validBinding(cls.PBinding, c) {
			return nil, nil
		}
	}
	if cls.PAlias != "" {
		c := &table.Cell{P: p}
		r[cls.PAlias] = c
		if !validBinding(cls.PAlias, c) {
			return nil, nil
		}
	}
	if cls.PIDAlias != "" {
		c := &table.Cell{S: table.CellString(string(p.ID()))}
		r[cls.PIDAlias] = c
		if !validBinding(cls.PIDAlias, c) {
			return nil, nil
		}
	}
	if cls.PAnchorBinding != "" {
		if p.Type() != predicate.Temporal {
			return nil, fmt.Errorf("cannot retrieve the time anchor value for non temporal predicate %q in binding %q", p, cls.PAnchorBinding)
		}
		t, err := p.TimeAnchor()
		if err != nil {
			return nil, fmt.Errorf("failed to retrieve the time anchor value for predicate %q in binding %q with error %v", p, cls.PAnchorBinding, err)
		}
		c := &table.Cell{T: t}
		r[cls.PAnchorBinding] = c
		if !validBinding(cls.PAnchorBinding, c) {
			return nil, nil
		}
	}

	if cls.PAnchorAlias != "" {
		if p.Type() != predicate.Temporal {
			return nil, fmt.Errorf("cannot retrieve the time anchor value for non temporal predicate %q in binding %q", p, cls.PAnchorAlias)
		}
		t, err := p.TimeAnchor()
		if err != nil {
			return nil, fmt.Errorf("failed to retrieve the time anchor value for predicate %q in binding %q with error %v", p, cls.PAnchorAlias, err)
		}
		c := &table.Cell{T: t}
		r[cls.PAnchorAlias] = c
		if !validBinding(cls.PAnchorAlias, c) {
			return nil, nil
		}
	}

	// Object related bindings.
	if cls.OBinding != "" {
		// Extract the object type.
		c, err := objectToCell(o)
		if err != nil {
			return nil, err
		}
		r[cls.OBinding] = c
		if !validBinding(cls.OBinding, c) {
			return nil, nil
		}
	}
	if cls.OAlias != "" {
		// Extract the object type.
		c, err := objectToCell(o)
		if err != nil {
			return nil, err
		}
		r[cls.OAlias] = c
		if !validBinding(cls.OAlias, c) {
			return nil, nil
		}
	}
	if cls.OTypeAlias != "" {
		n, err := o.Node()
		if err != nil {
			return nil, err
		}
		c := &table.Cell{S: table.CellString(n.Type().String())}
		r[cls.OTypeAlias] = c
		if !validBinding(cls.OTypeAlias, c) {
			return nil, nil
		}
	}
	if cls.OIDAlias != "" {
		n, err := o.Node()
		if err == nil {
			r[cls.OIDAlias] = &table.Cell{S: table.CellString(n.ID().String())}
		} else {
			p, err := o.Predicate()
			if err != nil {
				return nil, err
			}
			c := &table.Cell{S: table.CellString(string(p.ID()))}
			r[cls.OIDAlias] = c
			if !validBinding(cls.OIDAlias, c) {
				return nil, nil
			}
		}
	}
	if cls.OAnchorBinding != "" {
		p, err := o.Predicate()
		if err != nil {
			return nil, err
		}
		ts, err := p.TimeAnchor()
		if err != nil {
			return nil, err
		}
		c := &table.Cell{T: ts}
		r[cls.OAnchorBinding] = c
		if !validBinding(cls.OAnchorBinding, c) {
			return nil, nil
		}
	}
	if cls.OAnchorAlias != "" {
		p, err := o.Predicate()
		if err != nil {
			return nil, err
		}
		ts, err := p.TimeAnchor()
		if err != nil {
			return nil, err
		}
		c := &table.Cell{T: ts}
		r[cls.OAnchorAlias] = c
		if !validBinding(cls.OAnchorAlias, c) {
			return nil, nil
		}
	}

	return r, nil
}
