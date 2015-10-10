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
	stm   *semantic.Statement
	store storage.Store
}

// Execute queries the indicated graphs.
func (p *queryPlan) Excecute() (*table.Table, error) {
	return nil, errors.New("planner.queryPlan: Excecute method not implemented")
}

// New create a new executable plan given a semantic BQL statement.
func New(store storage.Store, stm *semantic.Statement) (Excecutor, error) {
	switch stm.Type() {
	case semantic.Query:
		return &queryPlan{
			stm:   stm,
			store: store,
		}, nil
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
