// Copyright 2018 Google Inc. All rights reserved.
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

// Package memoization implements a passthrough driver with memoization
// of the partial query results.
package memoization

import (
	"context"
	"errors"
	"fmt"
	"strings"
	"sync"

	"github.com/google/badwolf/storage"
	"github.com/google/badwolf/triple"
	"github.com/google/badwolf/triple/node"
	"github.com/google/badwolf/triple/predicate"
	"github.com/pborman/uuid"
)

// storeMmemoizer implements the memoization.
type storeMemoizer struct {
	s storage.Store
}

// New returns a new memoized driver.
func New(s storage.Store) storage.Store {
	return &storeMemoizer{
		s: s,
	}
}

// Name returns the ID of the backend being used.
func (s *storeMemoizer) Name(ctx context.Context) string {
	return s.s.Name(ctx)
}

// Version returns the version of the driver implementation.
func (s *storeMemoizer) Version(ctx context.Context) string {
	return s.s.Version(ctx)
}

// NewGraph creates a new graph. Creating an already existing graph
// should return an error.
func (s *storeMemoizer) NewGraph(ctx context.Context, id string) (storage.Graph, error) {
	g, err := s.s.NewGraph(ctx, id)
	if err != nil {
		return nil, err
	}
	return &graphMemoizer{
		g:    g,
		memN: make(map[string][]*node.Node),
		memP: make(map[string][]*predicate.Predicate),
		memO: make(map[string][]*triple.Object),
		memT: make(map[string][]*triple.Triple),
		memE: make(map[string]bool),
	}, nil
}

// Graph returns an existing graph if available. Getting a non existing
// graph should return an error.
func (s *storeMemoizer) Graph(ctx context.Context, id string) (storage.Graph, error) {
	g, err := s.s.Graph(ctx, id)
	if err != nil {
		return nil, err
	}
	return &graphMemoizer{
		g:    g,
		memN: make(map[string][]*node.Node),
		memP: make(map[string][]*predicate.Predicate),
		memO: make(map[string][]*triple.Object),
		memT: make(map[string][]*triple.Triple),
		memE: make(map[string]bool),
	}, nil
}

// DeleteGraph deletes an existing graph. Deleting a non existing graph
// should return an error.
func (s *storeMemoizer) DeleteGraph(ctx context.Context, id string) error {
	return s.s.DeleteGraph(ctx, id)
}

// GraphNames returns the current available graph names in the store.
func (s *storeMemoizer) GraphNames(ctx context.Context, names chan<- string) error {
	return s.s.GraphNames(ctx, names)
}

// graphMemoizer memoizers partial query results.
type graphMemoizer struct {
	g storage.Graph

	mu   sync.RWMutex
	memN map[string][]*node.Node
	memP map[string][]*predicate.Predicate
	memO map[string][]*triple.Object
	memT map[string][]*triple.Triple
	memE map[string]bool
}

// ID returns the id for this graph.
func (g *graphMemoizer) ID(ctx context.Context) string {
	return g.g.ID(ctx)
}

// AddTriples adds the triples to the storage. Adding a triple that already
// exists should not fail.
func (g *graphMemoizer) AddTriples(ctx context.Context, ts []*triple.Triple) error {
	g.mu.Lock()
	// Update operations reset the memoization.
	g.memN = make(map[string][]*node.Node)
	g.memP = make(map[string][]*predicate.Predicate)
	g.memO = make(map[string][]*triple.Object)
	g.memT = make(map[string][]*triple.Triple)
	g.memE = make(map[string]bool)
	g.mu.Unlock()

	return g.g.AddTriples(ctx, ts)
}

// RemoveTriples removes the triples from the storage. Removing triples that
// are not present on the store should not fail.
func (g *graphMemoizer) RemoveTriples(ctx context.Context, ts []*triple.Triple) error {
	g.mu.Lock()
	// Update operations reset the memoization.
	g.memN = make(map[string][]*node.Node)
	g.memP = make(map[string][]*predicate.Predicate)
	g.memO = make(map[string][]*triple.Object)
	g.memT = make(map[string][]*triple.Triple)
	g.memE = make(map[string]bool)
	g.mu.Unlock()

	return g.g.RemoveTriples(ctx, ts)
}

func combinedUUID(op string, lo *storage.LookupOptions, uuids ...uuid.UUID) string {
	var ss []string
	for _, id := range uuids {
		ss = append(ss, id.String())
	}
	return fmt.Sprintf("%s:%s:%s", op, lo.UUID().String(), strings.Join(ss, ":"))
}

// Objects pushes to the provided channel the objects for the given object and
// predicate. The function does not return immediately.
//
// Given a subject and a predicate, this method retrieves the objects of
// triples that match them. By default, if does not limit the maximum number
// of possible objects returned, unless properly specified by provided lookup
// options.
//
// If the provided predicate is immutable it will return all the possible
// subject values or the number of max elements specified. There is no
// requirement on how to sample the returned max elements.
//
// If the predicate is an unanchored temporal triple and no time anchors are
// provided in the lookup options, it will return all the available objects.
// If time anchors are provided, it will return all the values anchored in the
// provided time window. If max elements is also provided as part of the
// lookup options it will return at most max elements. There is no
// specifications on how that sample should be conducted.
func (g *graphMemoizer) Objects(ctx context.Context, s *node.Node, p *predicate.Predicate, lo *storage.LookupOptions, objs chan<- *triple.Object) error {
	k := combinedUUID("Objects", lo, s.UUID(), p.UUID())
	g.mu.RLock()
	v := g.memO[k]
	g.mu.RUnlock()
	if v != nil {
		// Return the memoized results.
		defer close(objs)
		for _, o := range v {
			select {
			case <-ctx.Done():
				return nil
			case objs <- o:
				// Nothing to do.
			}
		}
		return nil
	}

	// Query and memoize the results.
	c := make(chan *triple.Object)
	defer close(objs)

	var (
		err   error
		wg    sync.WaitGroup
		mobjs []*triple.Object
	)
	wg.Add(1)
	go func() {
		err = g.g.Objects(ctx, s, p, lo, c)
		wg.Done()
	}()

	for o := range c {
		select {
		case <-ctx.Done():
			return errors.New("context cancelled")
		case objs <- o:
			// memoize the object.
			mobjs = append(mobjs, o)
		}
	}
	wg.Wait()
	g.mu.Lock()
	g.memO[k] = mobjs
	g.mu.Unlock()
	return err
}

// Subject pushes to the provided channel the subjects for the give predicate
// and object. The function does not return immediately. The caller is
// expected to detach them into a go routine.
//
// Given a predicate and an object, this method retrieves the subjects of
// triples that matches them. By default, it does not limit the maximum number
// of possible subjects returned, unless properly specified by provided lookup
// options.
//
// If the provided predicate is immutable it will return all the possible
// subject values or the number of max elements specified. There is no
// requirement on how to sample the returned max elements.
//
// If the predicate is an unanchored temporal triple and no time anchors are
// provided in the lookup options, it will return all the available subjects.
// If time anchors are provided, it will return all the values anchored in the
// provided time window. If max elements is also provided as part of the
// lookup options it will return the at most max elements. There is no
// specifications on how that sample should be conducted.
func (g *graphMemoizer) Subjects(ctx context.Context, p *predicate.Predicate, o *triple.Object, lo *storage.LookupOptions, subs chan<- *node.Node) error {
	k := combinedUUID("Subjects", lo, p.UUID(), o.UUID())
	g.mu.RLock()
	v := g.memN[k]
	g.mu.RUnlock()
	if v != nil {
		// Return the memoized results.
		defer close(subs)
		for _, s := range v {
			select {
			case <-ctx.Done():
				return nil
			case subs <- s:
				// Nothing to do.
			}
		}
		return nil
	}

	// Query and memoize the results.
	c := make(chan *node.Node)
	defer close(subs)

	var (
		err   error
		wg    sync.WaitGroup
		msubs []*node.Node
	)
	wg.Add(1)
	go func() {
		err = g.g.Subjects(ctx, p, o, lo, c)
		wg.Done()
	}()

	for s := range c {
		select {
		case <-ctx.Done():
			return errors.New("context cancelled")
		case subs <- s:
			// memoize the object.
			msubs = append(msubs, s)
		}
	}
	wg.Wait()
	g.mu.Lock()
	g.memN[k] = msubs
	g.mu.Unlock()
	return err
}

// PredicatesForSubject pushes to the provided channel all the predicates
// known for the given subject. The function does not return immediately.
// The caller is expected to detach them into a go routine.
//
// If the lookup options provide a max number of elements the function will
// return a sample of the available predicates. If time anchor bounds are
// provided in the lookup options, only predicates matching the provided
// type window would be return. Same sampling consideration apply if max
// element is provided.
func (g *graphMemoizer) PredicatesForSubject(ctx context.Context, s *node.Node, lo *storage.LookupOptions, prds chan<- *predicate.Predicate) error {
	k := combinedUUID("PredicatesForSubject", lo, s.UUID())
	g.mu.RLock()
	v := g.memP[k]
	g.mu.RUnlock()
	if v != nil {
		// Return the memoized results.
		defer close(prds)
		for _, p := range v {
			select {
			case <-ctx.Done():
				return nil
			case prds <- p:
				// Nothing to do.
			}
		}
		return nil
	}

	// Query and memoize the results.
	c := make(chan *predicate.Predicate)
	defer close(prds)

	var (
		err    error
		wg     sync.WaitGroup
		mpreds []*predicate.Predicate
	)
	wg.Add(1)
	go func() {
		err = g.g.PredicatesForSubject(ctx, s, lo, c)
		wg.Done()
	}()

	for p := range c {
		select {
		case <-ctx.Done():
			return errors.New("context cancelled")
		case prds <- p:
			// memoize the object.
			mpreds = append(mpreds, p)
		}
	}
	wg.Wait()
	g.mu.Lock()
	g.memP[k] = mpreds
	g.mu.Unlock()
	return err
}

// PredicatesForObject pushes to the provided channel all the predicates known
// for the given object. The function does not return immediately. The caller
// is expected to detach them into a go routine.
//
// If the lookup options provide a max number of elements the function will
// return a sample of the available predicates. If time anchor bounds are
// provided in the lookup options, only predicates matching the provided type
// window would be return. Same sampling consideration apply if max element
// is provided.
func (g *graphMemoizer) PredicatesForObject(ctx context.Context, o *triple.Object, lo *storage.LookupOptions, prds chan<- *predicate.Predicate) error {
	k := combinedUUID("PredicatesForObject", lo, o.UUID())
	g.mu.RLock()
	v := g.memP[k]
	g.mu.RUnlock()
	if v != nil {
		// Return the memoized results.
		defer close(prds)
		for _, p := range v {
			select {
			case <-ctx.Done():
				return nil
			case prds <- p:
				// Nothing to do.
			}
		}
		return nil
	}

	// Query and memoize the results.
	c := make(chan *predicate.Predicate)
	defer close(prds)

	var (
		err    error
		wg     sync.WaitGroup
		mpreds []*predicate.Predicate
	)
	wg.Add(1)
	go func() {
		err = g.g.PredicatesForObject(ctx, o, lo, c)
		wg.Done()
	}()

	for p := range c {
		select {
		case <-ctx.Done():
			return errors.New("context cancelled")
		case prds <- p:
			// memoize the object.
			mpreds = append(mpreds, p)
		}
	}
	wg.Wait()
	g.mu.Lock()
	g.memP[k] = mpreds
	g.mu.Unlock()
	return err
}

// PredicatesForSubjectAndObject pushes to the provided channel all predicates
// available for the given subject and object. The function does not return
// immediately. The caller is expected to detach them into a go routine.
//
// If the lookup options provide a max number of elements the function will
// return a sample of the available predicates. If time anchor bounds are
// provided in the lookup options, only predicates matching the provided type
// window would be return. Same sampling consideration apply if max element is
// provided.
func (g *graphMemoizer) PredicatesForSubjectAndObject(ctx context.Context, s *node.Node, o *triple.Object, lo *storage.LookupOptions, prds chan<- *predicate.Predicate) error {
	k := combinedUUID("PredicatesForSubjectAndObject", lo, s.UUID(), o.UUID())
	g.mu.RLock()
	v := g.memP[k]
	g.mu.RUnlock()
	if v != nil {
		// Return the memoized results.
		defer close(prds)
		for _, p := range v {
			select {
			case <-ctx.Done():
				return nil
			case prds <- p:
				// Nothing to do.
			}
		}
		return nil
	}

	// Query and memoize the results.
	c := make(chan *predicate.Predicate)
	defer close(prds)

	var (
		err    error
		wg     sync.WaitGroup
		mpreds []*predicate.Predicate
	)
	wg.Add(1)
	go func() {
		err = g.g.PredicatesForSubjectAndObject(ctx, s, o, lo, c)
		wg.Done()
	}()

	for p := range c {
		select {
		case <-ctx.Done():
			return errors.New("context cancelled")
		case prds <- p:
			// memoize the object.
			mpreds = append(mpreds, p)
		}
	}
	wg.Wait()
	g.mu.Lock()
	g.memP[k] = mpreds
	g.mu.Unlock()
	return err
}

// TriplesForSubject pushes to the provided channel all triples available for
// the given subject. The function does not return immediately. The caller
// is expected to detach them into a go routine.
//
// If the lookup options provide a max number of elements the function will
// return a sample of the available triples. If time anchor bounds are
// provided in the lookup options, only predicates matching the provided type
// window would be return. Same sampling consideration apply if max element is
// provided.
func (g *graphMemoizer) TriplesForSubject(ctx context.Context, s *node.Node, lo *storage.LookupOptions, trpls chan<- *triple.Triple) error {
	k := combinedUUID("TriplesForSubject", lo, s.UUID())
	g.mu.RLock()
	v := g.memT[k]
	g.mu.RUnlock()
	if v != nil {
		// Return the memoized results.
		defer close(trpls)
		for _, t := range v {
			select {
			case <-ctx.Done():
				return nil
			case trpls <- t:
				// Nothing to do.
			}
		}
		return nil
	}

	// Query and memoize the results.
	c := make(chan *triple.Triple)
	defer close(trpls)

	var (
		err error
		wg  sync.WaitGroup
		mts []*triple.Triple
	)
	wg.Add(1)
	go func() {
		err = g.g.TriplesForSubject(ctx, s, lo, c)
		wg.Done()
	}()

	for t := range c {
		select {
		case <-ctx.Done():
			return errors.New("context cancelled")
		case trpls <- t:
			// memoize the object.
			mts = append(mts, t)
		}
	}
	wg.Wait()
	g.mu.Lock()
	g.memT[k] = mts
	g.mu.Unlock()
	return err
}

// TriplesForPredicate pushes to the provided channel all triples available
// for the given predicate.The function does not return immediatel. The
// caller is expected to detach them into a go routine.
//
// If the lookup options provide a max number of elements the function will
// return a sample of the available triples. If time anchor bounds are
// provided in the lookup options, only predicates matching the provided type
// window would be return. Same sampling consideration apply if max element is
// provided.
func (g *graphMemoizer) TriplesForPredicate(ctx context.Context, p *predicate.Predicate, lo *storage.LookupOptions, trpls chan<- *triple.Triple) error {
	k := combinedUUID("TriplesForPredicate", lo, p.UUID())
	g.mu.RLock()
	v := g.memT[k]
	g.mu.RUnlock()
	if v != nil {
		// Return the memoized results.
		defer close(trpls)
		for _, t := range v {
			select {
			case <-ctx.Done():
				return nil
			case trpls <- t:
				// Nothing to do.
			}
		}
		return nil
	}

	// Query and memoize the results.
	c := make(chan *triple.Triple)
	defer close(trpls)

	var (
		err error
		wg  sync.WaitGroup
		mts []*triple.Triple
	)
	wg.Add(1)
	go func() {
		err = g.g.TriplesForPredicate(ctx, p, lo, c)
		wg.Done()
	}()

	for t := range c {
		select {
		case <-ctx.Done():
			return errors.New("context cancelled")
		case trpls <- t:
			// memoize the object.
			mts = append(mts, t)
		}
	}
	wg.Wait()
	g.mu.Lock()
	g.memT[k] = mts
	g.mu.Unlock()
	return err
}

// TriplesForObject pushes to the provided channel all triples available for
// the given object. The function does not return immediately. The caller is
// expected to detach them into a go routine.
//
// If the lookup options provide a max number of elements the function will
// return a sample of the available triples. If time anchor bounds are
// provided in the lookup options, only predicates matching the provided type
// window would be return. Same sampling consideration apply if max element is
// provided.
func (g *graphMemoizer) TriplesForObject(ctx context.Context, o *triple.Object, lo *storage.LookupOptions, trpls chan<- *triple.Triple) error {
	k := combinedUUID("TriplesForObject", lo, o.UUID())
	g.mu.RLock()
	v := g.memT[k]
	g.mu.RUnlock()
	if v != nil {
		// Return the memoized results.
		defer close(trpls)
		for _, t := range v {
			select {
			case <-ctx.Done():
				return nil
			case trpls <- t:
				// Nothing to do.
			}
		}
		return nil
	}

	// Query and memoize the results.
	c := make(chan *triple.Triple)
	defer close(trpls)

	var (
		err error
		wg  sync.WaitGroup
		mts []*triple.Triple
	)
	wg.Add(1)
	go func() {
		err = g.g.TriplesForObject(ctx, o, lo, c)
		wg.Done()
	}()

	for t := range c {
		select {
		case <-ctx.Done():
			return errors.New("context cancelled")
		case trpls <- t:
			// memoize the object.
			mts = append(mts, t)
		}
	}
	wg.Wait()
	g.mu.Lock()
	g.memT[k] = mts
	g.mu.Unlock()
	return err
}

// TriplesForSubjectAndPredicate pushes to the provided channel all triples
// available for the given subject and predicate. The function does not return
// immediately. The caller is expected to detach them into a go routine.
//
// If the lookup options provide a max number of elements the function will
// return a sample of the available triples. If time anchor bounds are
// provided in the lookup options, only predicates matching the provided type
// window would be return. Same sampling consideration apply if max element is
// provided.
func (g *graphMemoizer) TriplesForSubjectAndPredicate(ctx context.Context, s *node.Node, p *predicate.Predicate, lo *storage.LookupOptions, trpls chan<- *triple.Triple) error {
	k := combinedUUID("TriplesForSubjectAndPredicate", lo, s.UUID(), p.UUID())
	g.mu.RLock()
	v := g.memT[k]
	g.mu.RUnlock()
	if v != nil {
		// Return the memoized results.
		defer close(trpls)
		for _, t := range v {
			select {
			case <-ctx.Done():
				return nil
			case trpls <- t:
				// Nothing to do.
			}
		}
		return nil
	}

	// Query and memoize the results.
	c := make(chan *triple.Triple)
	defer close(trpls)

	var (
		err error
		wg  sync.WaitGroup
		mts []*triple.Triple
	)
	wg.Add(1)
	go func() {
		err = g.g.TriplesForSubjectAndPredicate(ctx, s, p, lo, c)
		wg.Done()
	}()

	for t := range c {
		select {
		case <-ctx.Done():
			return errors.New("context cancelled")
		case trpls <- t:
			// memoize the object.
			mts = append(mts, t)
		}
	}
	wg.Wait()
	g.mu.Lock()
	g.memT[k] = mts
	g.mu.Unlock()
	return err
}

// TriplesForPredicateAndObject pushes to the provided channel all triples
// available for the given predicate and object. The function does not return
// immediately. The caller is expected to detach them into a go routine.
//
// If the lookup options provide a max number of elements the function will
// return a sample of the available triples. If time anchor bounds are
// provided in the lookup options, only predicates matching the provided type
// window would be return. Same sampling consideration apply if max element is
// provided.
func (g *graphMemoizer) TriplesForPredicateAndObject(ctx context.Context, p *predicate.Predicate, o *triple.Object, lo *storage.LookupOptions, trpls chan<- *triple.Triple) error {
	k := combinedUUID("TriplesForPredicateAndObject", lo, p.UUID(), o.UUID())
	g.mu.RLock()
	v := g.memT[k]
	g.mu.RUnlock()
	if v != nil {
		// Return the memoized results.
		defer close(trpls)
		for _, t := range v {
			select {
			case <-ctx.Done():
				return nil
			case trpls <- t:
				// Nothing to do.
			}
		}
		return nil
	}

	// Query and memoize the results.
	c := make(chan *triple.Triple)
	defer close(trpls)

	var (
		err error
		wg  sync.WaitGroup
		mts []*triple.Triple
	)
	wg.Add(1)
	go func() {
		err = g.g.TriplesForPredicateAndObject(ctx, p, o, lo, c)
		wg.Done()
	}()

	for t := range c {
		select {
		case <-ctx.Done():
			return errors.New("context cancelled")
		case trpls <- t:
			// memoize the object.
			mts = append(mts, t)
		}
	}
	wg.Wait()
	g.mu.Lock()
	g.memT[k] = mts
	g.mu.Unlock()
	return err
}

// Exist checks if the provided triple exists on the store.
func (g *graphMemoizer) Exist(ctx context.Context, t *triple.Triple) (bool, error) {
	k := combinedUUID("Exist", storage.DefaultLookup, t.UUID())
	g.mu.RLock()
	v, ok := g.memE[k]
	g.mu.RUnlock()
	if ok {
		// Return the memoized results.
		return v, nil
	}

	// Query and memoize the results.
	b, err := g.g.Exist(ctx, t)
	if err == nil {
		g.mu.Lock()
		g.memE[k] = b
		g.mu.Unlock()
	}
	return b, err
}

// Triples pushes to the provided channel all available triples in the graph.
// The function does not return immediately but spawns a goroutine to satisfy
// elements in the channel.
func (g *graphMemoizer) Triples(ctx context.Context, lo *storage.LookupOptions, trpls chan<- *triple.Triple) error {
	k := combinedUUID("Triples", lo)
	g.mu.RLock()
	v := g.memT[k]
	g.mu.RUnlock()
	if v != nil {
		// Return the memoized results.
		defer close(trpls)
		for _, t := range v {
			select {
			case <-ctx.Done():
				return nil
			case trpls <- t:
				// Nothing to do.
			}
		}
		return nil
	}

	// Query and memoize the results.
	c := make(chan *triple.Triple)
	defer close(trpls)

	var (
		err error
		wg  sync.WaitGroup
		mts []*triple.Triple
	)
	wg.Add(1)
	go func() {
		err = g.g.Triples(ctx, lo, c)
		wg.Done()
	}()

	for t := range c {
		select {
		case <-ctx.Done():
			return errors.New("context cancelled")
		case trpls <- t:
			// memoize the object.
			mts = append(mts, t)
		}
	}
	wg.Wait()
	g.mu.Lock()
	g.memT[k] = mts
	g.mu.Unlock()
	return err
}
