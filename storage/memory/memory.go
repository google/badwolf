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

// Package memory provide a volatile memory-based implementation of the
// storage.Store and storage.Graph interfaces.
package memory

import (
	"fmt"
	"strings"
	"sync"

	"github.com/google/badwolf/storage"
	"github.com/google/badwolf/triple"
	"github.com/google/badwolf/triple/node"
	"github.com/google/badwolf/triple/predicate"
)

// DefaultStore provides a volatile in memory store.
var DefaultStore storage.Store

func init() {
	DefaultStore = NewStore()
}

type memoryStore struct {
	graphs map[string]storage.Graph
	rwmu   sync.RWMutex
}

// NewStore creates a new memory store.
func NewStore() storage.Store {
	return &memoryStore{
		graphs: make(map[string]storage.Graph),
	}
}

// Name returns the ID of the backend being used.
func (s *memoryStore) Name() string {
	return "MEMORY_STORE"
}

// Version returns the version of the driver implementation.
func (s *memoryStore) Version() string {
	return "0.1.vcli"
}

// NewGraph creates a new graph.
func (s *memoryStore) NewGraph(id string) (storage.Graph, error) {
	g := &memory{
		id:    id,
		idx:   make(map[string]*triple.Triple),
		idxS:  make(map[string]map[string]*triple.Triple),
		idxP:  make(map[string]map[string]*triple.Triple),
		idxO:  make(map[string]map[string]*triple.Triple),
		idxSP: make(map[string]map[string]*triple.Triple),
		idxPO: make(map[string]map[string]*triple.Triple),
		idxSO: make(map[string]map[string]*triple.Triple),
	}

	s.rwmu.Lock()
	defer s.rwmu.Unlock()
	if _, ok := s.graphs[id]; ok {
		return nil, fmt.Errorf("memory.NewGraph(%q): graph alredy exists", id)
	}
	s.graphs[id] = g
	return g, nil
}

// Graph return an existing graph if available. Getting a non existing
// graph should return and error.
func (s *memoryStore) Graph(id string) (storage.Graph, error) {
	s.rwmu.RLock()
	defer s.rwmu.RUnlock()
	if g, ok := s.graphs[id]; ok {
		return g, nil
	}
	return nil, fmt.Errorf("memory.Graph(%q): graph does not exist", id)
}

// DeleteGraph with delete an existing graph. Deleting a non existing graph
// should return and error.
func (s *memoryStore) DeleteGraph(id string) error {
	s.rwmu.Lock()
	defer s.rwmu.Unlock()
	if _, ok := s.graphs[id]; ok {
		delete(s.graphs, id)
		return nil
	}
	return fmt.Errorf("memory.DeleteGraph(%q): graph does not exist", id)
}

// memory provides an imemory volatile implemention of the storage API.
type memory struct {
	id    string
	rwmu  sync.RWMutex
	idx   map[string]*triple.Triple
	idxS  map[string]map[string]*triple.Triple
	idxP  map[string]map[string]*triple.Triple
	idxO  map[string]map[string]*triple.Triple
	idxSP map[string]map[string]*triple.Triple
	idxPO map[string]map[string]*triple.Triple
	idxSO map[string]map[string]*triple.Triple
}

// ID returns the id for this graph.
func (m *memory) ID() string {
	return m.id
}

// AddTriples adds the triples to the storage.
func (m *memory) AddTriples(ts []*triple.Triple) error {
	for _, t := range ts {
		guid := t.GUID()
		sGUID := t.S().GUID()
		pGUID := t.P().GUID()
		oGUID := t.O().GUID()
		// Update master index
		m.rwmu.Lock()
		m.idx[guid] = t

		if _, ok := m.idxS[sGUID]; !ok {
			m.idxS[sGUID] = make(map[string]*triple.Triple)
		}
		m.idxS[sGUID][guid] = t

		if _, ok := m.idxP[pGUID]; !ok {
			m.idxP[pGUID] = make(map[string]*triple.Triple)
		}
		m.idxP[pGUID][guid] = t

		if _, ok := m.idxO[oGUID]; !ok {
			m.idxO[oGUID] = make(map[string]*triple.Triple)
		}
		m.idxO[oGUID][guid] = t

		key := strings.Join([]string{sGUID, pGUID}, ":")
		if _, ok := m.idxSP[key]; !ok {
			m.idxSP[key] = make(map[string]*triple.Triple)
		}
		m.idxSP[key][guid] = t

		key = strings.Join([]string{pGUID, oGUID}, ":")
		if _, ok := m.idxPO[key]; !ok {
			m.idxPO[key] = make(map[string]*triple.Triple)
		}
		m.idxPO[key][guid] = t

		key = strings.Join([]string{sGUID, oGUID}, ":")
		if _, ok := m.idxSO[key]; !ok {
			m.idxSO[key] = make(map[string]*triple.Triple)
		}
		m.idxSO[key][guid] = t

		m.rwmu.Unlock()
	}
	return nil
}

// RemoveTriples removes the trilpes from the storage.
func (m *memory) RemoveTriples(ts []*triple.Triple) error {
	for _, t := range ts {
		guid := t.GUID()
		sGUID := t.S().GUID()
		pGUID := t.P().GUID()
		oGUID := t.O().GUID()
		// Update master index
		m.rwmu.Lock()
		delete(m.idx, guid)
		delete(m.idxS[sGUID], guid)
		delete(m.idxP[pGUID], guid)
		delete(m.idxO[oGUID], guid)

		key := strings.Join([]string{sGUID, pGUID}, ":")
		delete(m.idxSP[key], guid)
		if len(m.idxSP[key]) == 0 {
			delete(m.idxSP, key)
		}

		key = strings.Join([]string{pGUID, oGUID}, ":")
		delete(m.idxPO[key], guid)
		if len(m.idxPO[key]) == 0 {
			delete(m.idxPO, key)
		}

		key = strings.Join([]string{sGUID, oGUID}, ":")
		delete(m.idxSO[key], guid)
		if len(m.idxSO[key]) == 0 {
			delete(m.idxSO, key)
		}

		m.rwmu.Unlock()
	}
	return nil
}

// checker provides the mechanics to check if a predicate/triple should be
// considered on a cerain operation.
type checker struct {
	max bool
	c   int
	o   *storage.LookupOptions
}

// newChecer creates a new checker for a given LookupOptions configuration.
func newChecker(o *storage.LookupOptions) *checker {
	b := false
	if o.MaxElements > 0 {
		b = true
	}
	return &checker{
		max: b,
		c:   o.MaxElements,
		o:   o,
	}
}

// CheckAndUpdate checks if a predicate should be considered and it also updates
// the internal state in case counts are needed.
func (c *checker) CheckAndUpdate(p *predicate.Predicate) bool {
	if c.max {
		if c.c <= 0 {
			return false
		}
		c.c--
	}
	if p.Type() == predicate.Immutable {
		return true
	}
	t, _ := p.TimeAnchor()
	if c.o.LowerAnchor != nil && t.Before(*c.o.LowerAnchor) {
		return false
	}
	if c.o.UpperAnchor != nil && t.After(*c.o.UpperAnchor) {
		return false
	}
	return true
}

// Objects returns the objects for the give object and predicate.
func (m *memory) Objects(s *node.Node, p *predicate.Predicate, lo *storage.LookupOptions) (storage.Objects, error) {
	sGUID := s.GUID()
	pGUID := p.GUID()
	spIdx := strings.Join([]string{sGUID, pGUID}, ":")
	m.rwmu.RLock()
	objs := make(chan *triple.Object, len(m.idxSP[spIdx]))
	go func() {
		ckr := newChecker(lo)
		for _, t := range m.idxSP[spIdx] {
			if ckr.CheckAndUpdate(t.P()) {
				objs <- t.O()
			}
		}
		m.rwmu.RUnlock()
		close(objs)
	}()
	return objs, nil
}

// Subject returns the subjects for the give predicate and object.
func (m *memory) Subjects(p *predicate.Predicate, o *triple.Object, lo *storage.LookupOptions) (storage.Nodes, error) {
	pGUID := p.GUID()
	oGUID := o.GUID()
	poIdx := strings.Join([]string{pGUID, oGUID}, ":")
	m.rwmu.RLock()
	subs := make(chan *node.Node, len(m.idxPO[poIdx]))
	go func() {
		ckr := newChecker(lo)
		for _, t := range m.idxPO[poIdx] {
			if ckr.CheckAndUpdate(t.P()) {
				subs <- t.S()
			}
		}
		m.rwmu.RUnlock()
		close(subs)
	}()
	return subs, nil
}

// PredicatesForSubjecAndObject returns all predicates available for the
// given subject and object.
func (m *memory) PredicatesForSubjectAndObject(s *node.Node, o *triple.Object, lo *storage.LookupOptions) (storage.Predicates, error) {
	sGUID := s.GUID()
	oGUID := o.GUID()
	soIdx := strings.Join([]string{sGUID, oGUID}, ":")
	m.rwmu.RLock()
	preds := make(chan *predicate.Predicate, len(m.idxSO[soIdx]))
	go func() {
		ckr := newChecker(lo)
		for _, t := range m.idxSO[soIdx] {
			if ckr.CheckAndUpdate(t.P()) {
				preds <- t.P()
			}
		}
		m.rwmu.RUnlock()
		close(preds)
	}()
	return preds, nil
}

// PredicatesForSubject returns all the predicats know for the given
// subject.
func (m *memory) PredicatesForSubject(s *node.Node, lo *storage.LookupOptions) (storage.Predicates, error) {
	sGUID := s.GUID()
	m.rwmu.RLock()
	preds := make(chan *predicate.Predicate, len(m.idxS[sGUID]))
	go func() {
		ckr := newChecker(lo)
		for _, t := range m.idxS[sGUID] {
			if ckr.CheckAndUpdate(t.P()) {
				preds <- t.P()
			}
		}
		m.rwmu.RUnlock()
		close(preds)
	}()
	return preds, nil
}

// PredicatesForObject returns all the predicats know for the given
// object.
func (m *memory) PredicatesForObject(o *triple.Object, lo *storage.LookupOptions) (storage.Predicates, error) {
	oGUID := o.GUID()
	m.rwmu.RLock()
	preds := make(chan *predicate.Predicate, len(m.idxO[oGUID]))
	go func() {
		ckr := newChecker(lo)
		for _, t := range m.idxO[oGUID] {
			if ckr.CheckAndUpdate(t.P()) {
				preds <- t.P()
			}
		}
		m.rwmu.RUnlock()
		close(preds)
	}()
	return preds, nil
}

// TriplesForSubject returns all triples available for a given subect.
func (m *memory) TriplesForSubject(s *node.Node, lo *storage.LookupOptions) (storage.Triples, error) {
	sGUID := s.GUID()
	m.rwmu.RLock()
	triples := make(chan *triple.Triple, len(m.idxS[sGUID]))
	go func() {
		ckr := newChecker(lo)
		for _, t := range m.idxS[sGUID] {
			if ckr.CheckAndUpdate(t.P()) {
				triples <- t
			}
		}
		m.rwmu.RUnlock()
		close(triples)
	}()
	return triples, nil
}

// TriplesForPredicate returns all triples available for a given predicate.
func (m *memory) TriplesForPredicate(p *predicate.Predicate, lo *storage.LookupOptions) (storage.Triples, error) {
	pGUID := p.GUID()
	m.rwmu.RLock()
	triples := make(chan *triple.Triple, len(m.idxP[pGUID]))
	go func() {
		ckr := newChecker(lo)
		for _, t := range m.idxP[pGUID] {
			if ckr.CheckAndUpdate(t.P()) {
				triples <- t
			}
		}
		m.rwmu.RUnlock()
		close(triples)
	}()
	return triples, nil
}

// TriplesForObject returns all triples available for a given object.
func (m *memory) TriplesForObject(o *triple.Object, lo *storage.LookupOptions) (storage.Triples, error) {
	oGUID := o.GUID()
	m.rwmu.RLock()
	triples := make(chan *triple.Triple, len(m.idxO[oGUID]))
	go func() {
		ckr := newChecker(lo)
		for _, t := range m.idxO[oGUID] {
			if ckr.CheckAndUpdate(t.P()) {
				triples <- t
			}
		}
		m.rwmu.RUnlock()
		close(triples)
	}()
	return triples, nil
}

// TriplesForSubjectAndPredicate returns all triples available for the given
// subject and predicate.
func (m *memory) TriplesForSubjectAndPredicate(s *node.Node, p *predicate.Predicate, lo *storage.LookupOptions) (storage.Triples, error) {
	sGUID := s.GUID()
	pGUID := p.GUID()
	spIdx := strings.Join([]string{sGUID, pGUID}, ":")
	m.rwmu.RLock()
	triples := make(chan *triple.Triple, len(m.idxSP[spIdx]))
	go func() {
		ckr := newChecker(lo)
		for _, t := range m.idxSP[spIdx] {
			if ckr.CheckAndUpdate(t.P()) {
				triples <- t
			}
		}
		m.rwmu.RUnlock()
		close(triples)
	}()
	return triples, nil
}

// TriplesForPredicateAndObject returns all triples available for the given
// predicate and object.
func (m *memory) TriplesForPredicateAndObject(p *predicate.Predicate, o *triple.Object, lo *storage.LookupOptions) (storage.Triples, error) {
	pGUID := p.GUID()
	oGUID := o.GUID()
	poIdx := strings.Join([]string{pGUID, oGUID}, ":")
	m.rwmu.RLock()
	triples := make(chan *triple.Triple, len(m.idxPO[poIdx]))
	go func() {
		ckr := newChecker(lo)
		for _, t := range m.idxPO[poIdx] {
			if ckr.CheckAndUpdate(t.P()) {
				triples <- t
			}
		}
		m.rwmu.RUnlock()
		close(triples)
	}()
	return triples, nil
}

// Exists checks if the provided triple exist on the store.
func (m *memory) Exist(t *triple.Triple) (bool, error) {
	guid := t.GUID()
	m.rwmu.RLock()
	_, ok := m.idx[guid]
	m.rwmu.RUnlock()
	return ok, nil
}

// Triples allows to iterate over all available triples.
func (m *memory) Triples() (storage.Triples, error) {
	triples := make(chan *triple.Triple, len(m.idx))
	go func() {
		for _, t := range m.idx {
			triples <- t
		}
		close(triples)
	}()
	return triples, nil
}
