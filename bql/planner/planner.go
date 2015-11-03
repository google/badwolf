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

// Package planner contains all the machinery to transform the semantic output
// into an actionable plan.
package planner

import (
	"errors"
	"fmt"
	"strings"
	"sync"

	"github.com/google/badwolf/bql/semantic"
	"github.com/google/badwolf/bql/table"
	"github.com/google/badwolf/storage"
	"github.com/google/badwolf/triple"
)

// Excecutor interface unifies the execution of statements.
type Excecutor interface {
	// Execute runs the proposed plan for a given statement.
	Excecute() (*table.Table, error)
}

// createPlan encapsulates the sequence of instructions that need to be
// excecuted in order to satisfy the exceution of a valid create BQL statement.
type createPlan struct {
	stm   *semantic.Statement
	store storage.Store
}

// Execute creates the indicated graphs.
func (p *createPlan) Excecute() (*table.Table, error) {
	t, err := table.New([]string{})
	if err != nil {
		return nil, err
	}
	errs := []string{}
	for _, g := range p.stm.Graphs() {
		if _, err := p.store.NewGraph(g); err != nil {
			errs = append(errs, err.Error())
		}
	}
	if len(errs) > 0 {
		return nil, errors.New(strings.Join(errs, "; "))
	}
	return t, nil
}

// dropPlan encapsulates the sequence of instructions that need to be
// excecuted in order to satisfy the exceution of a valid drop BQL statement.
type dropPlan struct {
	stm   *semantic.Statement
	store storage.Store
}

// Execute drops the indicated graphs.
func (p *dropPlan) Excecute() (*table.Table, error) {
	t, err := table.New([]string{})
	if err != nil {
		return nil, err
	}
	errs := []string{}
	for _, g := range p.stm.Graphs() {
		if err := p.store.DeleteGraph(g); err != nil {
			errs = append(errs, err.Error())
		}
	}
	if len(errs) > 0 {
		return nil, errors.New(strings.Join(errs, "; "))
	}
	return t, nil
}

// insertPlan encapsulates the sequence of instructions that need to be
// excecuted in order to satisfy the exceution of a valid insert BQL statement.
type insertPlan struct {
	stm   *semantic.Statement
	store storage.Store
}

type updater func(storage.Graph, []*triple.Triple) error

func update(stm *semantic.Statement, store storage.Store, f updater) error {
	var (
		mu   sync.Mutex
		wg   sync.WaitGroup
		errs []string
	)
	appendError := func(err error) {
		mu.Lock()
		defer mu.Unlock()
		errs = append(errs, err.Error())
	}

	for _, graphBinding := range stm.Graphs() {
		wg.Add(1)
		go func(graph string) {
			defer wg.Done()
			g, err := store.Graph(graph)
			if err != nil {
				appendError(err)
				return
			}
			err = f(g, stm.Data())
			if err != nil {
				appendError(err)
			}
		}(graphBinding)
	}
	wg.Wait()
	if len(errs) > 0 {
		return errors.New(strings.Join(errs, "; "))
	}
	return nil
}

// Execute inserts the provided data into the indicated graphs.
func (p *insertPlan) Excecute() (*table.Table, error) {
	t, err := table.New([]string{})
	if err != nil {
		return nil, err
	}
	return t, update(p.stm, p.store, func(g storage.Graph, d []*triple.Triple) error {
		return g.AddTriples(d)
	})
}

// deletePlan encapsulates the sequence of instructions that need to be
// excecuted in order to satisfy the exceution of a valid delete BQL statement.
type deletePlan struct {
	stm   *semantic.Statement
	store storage.Store
}

// Execute deletes the provided data into the indicated graphs.
func (p *deletePlan) Excecute() (*table.Table, error) {
	t, err := table.New([]string{})
	if err != nil {
		return nil, err
	}
	return t, update(p.stm, p.store, func(g storage.Graph, d []*triple.Triple) error {
		return g.RemoveTriples(d)
	})
}

// queryPlan encapsulates the sequence of instructions that need to be
// excecuted in order to satisfy the exceution of a valid query BQL statement.
type queryPlan struct {
	// Plan input.
	stm   *semantic.Statement
	store storage.Store
	// Prepared plan information.
	bndgs     []string
	grfsNames []string
	grfs      []storage.Graph
	cls       []*semantic.GraphClause
	tbl       *table.Table
}

// newQueryPlan returns a new query plan ready to be excecuted.
func newQueryPlan(store storage.Store, stm *semantic.Statement) (*queryPlan, error) {
	bs := []string{}
	for _, b := range stm.Bindings() {
		bs = append(bs, b)
	}
	t, err := table.New([]string{})
	if err != nil {
		return nil, err
	}
	var gs []storage.Graph
	for _, g := range stm.Graphs() {
		ng, err := store.Graph(g)
		if err != nil {
			return nil, err
		}
		gs = append(gs, ng)
	}
	return &queryPlan{
		stm:       stm,
		store:     store,
		bndgs:     bs,
		grfs:      gs,
		grfsNames: stm.Graphs(),
		cls:       stm.SortedGraphPatternClauses(),
		tbl:       t,
	}, nil
}

// processClause retrives the triples for the provided triple given the
// information available.
func (p *queryPlan) processClause(cls *semantic.GraphClause, lo *storage.LookupOptions) error {
	// This method decides how to process the clause based on the current
	// list of bindings solved and data available.
	exist, total := 0, 0
	for _, b := range cls.Bindings() {
		total++
		if p.tbl.HasBinding(b) {
			exist++
		}
	}
	if exist == 0 {
		// Data is new.
		tbl, err := simpleFetch(p.grfs, cls, lo)
		if err != nil {
			return err
		}
		if len(p.tbl.Bindings()) > 0 {
			return p.tbl.DotProduct(tbl)
		}
		return p.tbl.AppendTable(tbl)
	}
	if exist > 0 && exist < total {
		// TODO(xllora): Data is partially binded, retrieve data either extends
		// the row with the new bindings or filters it out if now new bindings
		// are available.
		return nil
	}
	if exist > 0 && exist == total {
		// TODO(xllora): Since all bindings in the clause are already solved,
		// the clause becomes a fully specified triple. If the triple does not
		// exist the row will be deleted.
		return nil
	}
	// Somethign is wrong with the code.
	return fmt.Errorf("queryPlan.processClause(%v) should have never failed to resolve the clause", cls)
}

// processGraphPattern proces the query graph pattern to retrieve the
// data from the specified graphs.
func (p *queryPlan) processGraphPattern(lo *storage.LookupOptions) error {
	for _, cls := range p.cls {
		// The current planner is based on naively excecuting clauses by
		// specificity.
		if err := p.processClause(cls, lo); err != nil {
			return err
		}
	}
	return nil
}

// Execute queries the indicated graphs.
func (p *queryPlan) Excecute() (*table.Table, error) {
	// Retrieve the data.
	lo := &storage.LookupOptions{}
	if err := p.processGraphPattern(lo); err != nil {
		return nil, err
	}
	return p.tbl, nil
}

// New create a new executable plan given a semantic BQL statement.
func New(store storage.Store, stm *semantic.Statement) (Excecutor, error) {
	switch stm.Type() {
	case semantic.Query:
		return newQueryPlan(store, stm)
	case semantic.Insert:
		return &insertPlan{
			stm:   stm,
			store: store,
		}, nil
	case semantic.Delete:
		return &deletePlan{
			stm:   stm,
			store: store,
		}, nil
	case semantic.Create:
		return &createPlan{
			stm:   stm,
			store: store,
		}, nil
	case semantic.Drop:
		return &dropPlan{
			stm:   stm,
			store: store,
		}, nil
	default:
		return nil, fmt.Errorf("planner.New: unknown statement type in statement %v", stm)
	}
}
