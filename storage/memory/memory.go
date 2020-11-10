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

// Package memory provides a volatile memory-based implementation of the
// storage.Store and storage.Graph interfaces.
package memory

import (
	"context"
	"fmt"
	"sync"
	"time"

	"github.com/google/badwolf/bql/planner/filter"
	"github.com/google/badwolf/storage"
	"github.com/google/badwolf/triple"
	"github.com/google/badwolf/triple/node"
	"github.com/google/badwolf/triple/predicate"
)

const initialAllocation = 10000

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
func (s *memoryStore) Name(ctx context.Context) string {
	return "VOLATILE"
}

// Version returns the version of the driver implementation.
func (s *memoryStore) Version(ctx context.Context) string {
	return "0.2.vcli"
}

// NewGraph creates a new graph.
func (s *memoryStore) NewGraph(ctx context.Context, id string) (storage.Graph, error) {
	g := &memory{
		id:    id,
		idx:   make(map[string]*triple.Triple, initialAllocation),
		idxS:  make(map[string]map[string]*triple.Triple, initialAllocation),
		idxP:  make(map[string]map[string]*triple.Triple, initialAllocation),
		idxO:  make(map[string]map[string]*triple.Triple, initialAllocation),
		idxSP: make(map[string]map[string]*triple.Triple, initialAllocation),
		idxPO: make(map[string]map[string]*triple.Triple, initialAllocation),
		idxSO: make(map[string]map[string]*triple.Triple, initialAllocation),
	}

	s.rwmu.Lock()
	defer s.rwmu.Unlock()
	if _, ok := s.graphs[id]; ok {
		return nil, fmt.Errorf("memory.NewGraph(%q): graph already exists", id)
	}
	s.graphs[id] = g
	return g, nil
}

// Graph returns an existing graph if available. Getting a non existing
// graph should return an error.
func (s *memoryStore) Graph(ctx context.Context, id string) (storage.Graph, error) {
	s.rwmu.RLock()
	defer s.rwmu.RUnlock()
	if g, ok := s.graphs[id]; ok {
		return g, nil
	}
	return nil, fmt.Errorf("memory.Graph(%q): graph does not exist", id)
}

// DeleteGraph deletes an existing graph. Deleting a non existing graph
// should return an error.
func (s *memoryStore) DeleteGraph(ctx context.Context, id string) error {
	s.rwmu.Lock()
	defer s.rwmu.Unlock()
	if _, ok := s.graphs[id]; ok {
		delete(s.graphs, id)
		return nil
	}
	return fmt.Errorf("memory.DeleteGraph(%q): graph does not exist", id)
}

// GraphNames returns the current available graph names in the store.
func (s *memoryStore) GraphNames(ctx context.Context, names chan<- string) error {
	if names == nil {
		return fmt.Errorf("cannot provide an empty channel")
	}
	s.rwmu.RLock()
	defer s.rwmu.RUnlock()
	for k := range s.graphs {
		names <- k
	}
	close(names)
	return nil
}

// memory provides an memory-based volatile implementation of the graph API.
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
func (m *memory) ID(ctx context.Context) string {
	return m.id
}

// AddTriples adds the triples to the storage.
func (m *memory) AddTriples(ctx context.Context, ts []*triple.Triple) error {
	m.rwmu.Lock()
	defer m.rwmu.Unlock()
	for _, t := range ts {
		tuuid := UUIDToByteString(t.UUID())
		sUUID := UUIDToByteString(t.Subject().UUID())
		pUUID := UUIDToByteString(t.Predicate().PartialUUID())
		oUUID := UUIDToByteString(t.Object().UUID())
		// Update master index
		m.idx[tuuid] = t

		if _, ok := m.idxS[sUUID]; !ok {
			m.idxS[sUUID] = make(map[string]*triple.Triple)
		}
		m.idxS[sUUID][tuuid] = t

		if _, ok := m.idxP[pUUID]; !ok {
			m.idxP[pUUID] = make(map[string]*triple.Triple)
		}
		m.idxP[pUUID][tuuid] = t

		if _, ok := m.idxO[oUUID]; !ok {
			m.idxO[oUUID] = make(map[string]*triple.Triple)
		}
		m.idxO[oUUID][tuuid] = t

		key := sUUID + pUUID
		if _, ok := m.idxSP[key]; !ok {
			m.idxSP[key] = make(map[string]*triple.Triple)
		}
		m.idxSP[key][tuuid] = t

		key = pUUID + oUUID
		if _, ok := m.idxPO[key]; !ok {
			m.idxPO[key] = make(map[string]*triple.Triple)
		}
		m.idxPO[key][tuuid] = t

		key = sUUID + oUUID
		if _, ok := m.idxSO[key]; !ok {
			m.idxSO[key] = make(map[string]*triple.Triple)
		}
		m.idxSO[key][tuuid] = t
	}
	return nil
}

// RemoveTriples removes the triples from the storage.
func (m *memory) RemoveTriples(ctx context.Context, ts []*triple.Triple) error {
	for _, t := range ts {
		suuid := UUIDToByteString(t.UUID())
		sUUID := UUIDToByteString(t.Subject().UUID())
		pUUID := UUIDToByteString(t.Predicate().PartialUUID())
		oUUID := UUIDToByteString(t.Object().UUID())
		// Update master index
		m.rwmu.Lock()
		delete(m.idx, suuid)
		delete(m.idxS[sUUID], suuid)
		delete(m.idxP[pUUID], suuid)
		delete(m.idxO[oUUID], suuid)

		key := sUUID + pUUID
		delete(m.idxSP[key], suuid)
		if len(m.idxSP[key]) == 0 {
			delete(m.idxSP, key)
		}

		key = pUUID + oUUID
		delete(m.idxPO[key], suuid)
		if len(m.idxPO[key]) == 0 {
			delete(m.idxPO, key)
		}

		key = sUUID + oUUID
		delete(m.idxSO[key], suuid)
		if len(m.idxSO[key]) == 0 {
			delete(m.idxSO, key)
		}

		m.rwmu.Unlock()
	}
	return nil
}

// checker provides the mechanics to check if a predicate/triple should be
// considered on a certain operation.
type checker struct {
	max bool
	c   int
	o   *storage.LookupOptions
	op  *predicate.Predicate
	ota *time.Time
}

// newChecker creates a new checker for a given LookupOptions configuration.
func newChecker(o *storage.LookupOptions, op *predicate.Predicate) *checker {
	var ta *time.Time
	if op != nil {
		if t, err := op.TimeAnchor(); err == nil {
			ta = t
		}
	}
	return &checker{
		max: o.MaxElements > 0,
		c:   o.MaxElements,
		o:   o,
		op:  op,
		ota: ta,
	}
}

// CheckGlobalTimeBounds checks if a predicate should be considered given the global
// time bounds.
func (c *checker) CheckGlobalTimeBounds(p *predicate.Predicate) bool {
	if p.Type() == predicate.Immutable {
		return true
	}

	if t, err := p.TimeAnchor(); err == nil {
		if c.ota != nil && !c.ota.Equal(*t) {
			return false
		}
		if c.o.LowerAnchor != nil && t.Before(*c.o.LowerAnchor) {
			return false
		}
		if c.o.UpperAnchor != nil && t.After(*c.o.UpperAnchor) {
			return false
		}
	}

	return true
}

// CheckLimitAndUpdate checks the internal limit count if it is set and also updates
// the internal state for the case these counts are needed.
func (c *checker) CheckLimitAndUpdate() bool {
	if c.max && c.c <= 0 {
		return false
	}

	c.c--
	return true
}

// applyGlobalTimeBounds applies the global time bound constraints specified by the checker to
// the given triples, returning only the triples that satisfy these time bounds.
func applyGlobalTimeBounds(trpls map[string]*triple.Triple, ckr *checker) map[string]*triple.Triple {
	selectedTrpls := make(map[string]*triple.Triple)
	for uuid, t := range trpls {
		if t != nil && ckr.CheckGlobalTimeBounds(t.Predicate()) {
			selectedTrpls[uuid] = t
		}
	}

	return selectedTrpls
}

// isImmutableFilter executes the isImmutable filter operation over memoryTriples following filterOptions.
func isImmutableFilter(memoryTriples map[string]*triple.Triple, pQuery *predicate.Predicate, filterOptions *filter.StorageOptions) (map[string]*triple.Triple, error) {
	if filterOptions.Field != filter.PredicateField && filterOptions.Field != filter.ObjectField {
		return nil, fmt.Errorf("invalid field %q for %q filter operation, can accept only %q or %q", filterOptions.Field, filter.IsImmutable, filter.PredicateField, filter.ObjectField)
	}

	trps := make(map[string]*triple.Triple)
	for _, t := range memoryTriples {
		if pQuery != nil && pQuery.String() != t.Predicate().String() {
			continue
		}

		var p *predicate.Predicate
		if filterOptions.Field == filter.PredicateField {
			p = t.Predicate()
		} else if pObj, err := t.Object().Predicate(); filterOptions.Field == filter.ObjectField && err == nil {
			p = pObj
		} else {
			continue
		}

		if p.Type() != predicate.Immutable {
			continue
		}

		trps[t.UUID().String()] = t
	}

	return trps, nil
}

// isTemporalFilter executes the isTemporal filter operation over memoryTriples following filterOptions.
func isTemporalFilter(memoryTriples map[string]*triple.Triple, pQuery *predicate.Predicate, filterOptions *filter.StorageOptions) (map[string]*triple.Triple, error) {
	if filterOptions.Field != filter.PredicateField && filterOptions.Field != filter.ObjectField {
		return nil, fmt.Errorf("invalid field %q for %q filter operation, can accept only %q or %q", filterOptions.Field, filter.IsTemporal, filter.PredicateField, filter.ObjectField)
	}

	trps := make(map[string]*triple.Triple)
	for _, t := range memoryTriples {
		if pQuery != nil && pQuery.String() != t.Predicate().String() {
			continue
		}

		var p *predicate.Predicate
		if filterOptions.Field == filter.PredicateField {
			p = t.Predicate()
		} else if pObj, err := t.Object().Predicate(); filterOptions.Field == filter.ObjectField && err == nil {
			p = pObj
		} else {
			continue
		}

		if p.Type() != predicate.Temporal {
			continue
		}

		trps[t.UUID().String()] = t
	}

	return trps, nil
}

// latestFilter executes the latest filter operation over memoryTriples following filterOptions.
func latestFilter(memoryTriples map[string]*triple.Triple, pQuery *predicate.Predicate, filterOptions *filter.StorageOptions) (map[string]*triple.Triple, error) {
	if filterOptions.Field != filter.PredicateField && filterOptions.Field != filter.ObjectField {
		return nil, fmt.Errorf("invalid field %q for %q filter operation, can accept only %q or %q", filterOptions.Field, filter.Latest, filter.PredicateField, filter.ObjectField)
	}

	lastTA := make(map[string]*time.Time)
	trps := make(map[string]map[string]*triple.Triple)
	for _, t := range memoryTriples {
		if pQuery != nil && pQuery.String() != t.Predicate().String() {
			continue
		}

		var p *predicate.Predicate
		if filterOptions.Field == filter.PredicateField {
			p = t.Predicate()
		} else if pObj, err := t.Object().Predicate(); filterOptions.Field == filter.ObjectField && err == nil {
			p = pObj
		} else {
			continue
		}
		if p.Type() != predicate.Temporal {
			continue
		}

		ppUUID := p.PartialUUID().String()
		ta, err := p.TimeAnchor()
		if err != nil {
			return nil, err
		}
		if lta := lastTA[ppUUID]; lta == nil || ta.Sub(*lta) > 0 {
			trps[ppUUID] = map[string]*triple.Triple{t.UUID().String(): t}
			lastTA[ppUUID] = ta
		} else if ta.Sub(*lta) == 0 {
			trps[ppUUID][t.UUID().String()] = t
		}
	}

	trpsByUUID := make(map[string]*triple.Triple)
	for _, m := range trps {
		for tUUID, t := range m {
			trpsByUUID[tUUID] = t
		}
	}
	return trpsByUUID, nil
}

// executeFilter executes the proper filter operation over memoryTriples following the specifications given in filterOptions.
func executeFilter(memoryTriples map[string]*triple.Triple, pQuery *predicate.Predicate, filterOptions *filter.StorageOptions) (map[string]*triple.Triple, error) {
	switch filterOptions.Operation {
	case filter.Latest:
		return latestFilter(memoryTriples, pQuery, filterOptions)
	case filter.IsImmutable:
		return isImmutableFilter(memoryTriples, pQuery, filterOptions)
	case filter.IsTemporal:
		return isTemporalFilter(memoryTriples, pQuery, filterOptions)
	default:
		return nil, fmt.Errorf("filter operation %q not supported in the driver", filterOptions.Operation)
	}
}

// Objects published the objects for the give object and predicate to the
// provided channel.
func (m *memory) Objects(ctx context.Context, s *node.Node, p *predicate.Predicate, lo *storage.LookupOptions, objs chan<- *triple.Object) error {
	if objs == nil {
		return fmt.Errorf("cannot provide an empty channel")
	}

	sUUID := UUIDToByteString(s.UUID())
	pUUID := UUIDToByteString(p.PartialUUID())
	spIdx := sUUID + pUUID
	m.rwmu.RLock()
	defer m.rwmu.RUnlock()
	defer close(objs)

	ckr := newChecker(lo, p)
	selectedTrpls := applyGlobalTimeBounds(m.idxSP[spIdx], ckr)

	var err error
	if lo.LatestAnchor {
		if lo.FilterOptions != nil {
			return fmt.Errorf("cannot have LatestAnchor and FilterOptions used at the same time inside lookup options")
		}
		lo.FilterOptions = &filter.StorageOptions{
			Operation: filter.Latest,
			Field:     filter.PredicateField,
		}
		// To guarantee that "lo.FilterOptions" will be cleaned at the driver level, since it was artificially created at the driver level for "LatestAnchor".
		defer func() {
			lo.FilterOptions = (*filter.StorageOptions)(nil)
		}()
	}
	if lo.FilterOptions != nil {
		selectedTrpls, err = executeFilter(selectedTrpls, p, lo.FilterOptions)
		if err != nil {
			return err
		}
	}

	for _, t := range selectedTrpls {
		if t != nil && ckr.CheckLimitAndUpdate() {
			objs <- t.Object()
		}
	}

	return nil
}

// Subject publishes the subjects for the give predicate and object to the
// provided channel.
func (m *memory) Subjects(ctx context.Context, p *predicate.Predicate, o *triple.Object, lo *storage.LookupOptions, subjs chan<- *node.Node) error {
	if subjs == nil {
		return fmt.Errorf("cannot provide an empty channel")
	}

	pUUID := UUIDToByteString(p.PartialUUID())
	oUUID := UUIDToByteString(o.UUID())
	poIdx := pUUID + oUUID
	m.rwmu.RLock()
	defer m.rwmu.RUnlock()
	defer close(subjs)

	ckr := newChecker(lo, p)
	selectedTrpls := applyGlobalTimeBounds(m.idxPO[poIdx], ckr)

	var err error
	if lo.LatestAnchor {
		if lo.FilterOptions != nil {
			return fmt.Errorf("cannot have LatestAnchor and FilterOptions used at the same time inside lookup options")
		}
		lo.FilterOptions = &filter.StorageOptions{
			Operation: filter.Latest,
			Field:     filter.PredicateField,
		}
		// To guarantee that "lo.FilterOptions" will be cleaned at the driver level, since it was artificially created at the driver level for "LatestAnchor".
		defer func() {
			lo.FilterOptions = (*filter.StorageOptions)(nil)
		}()
	}
	if lo.FilterOptions != nil {
		selectedTrpls, err = executeFilter(selectedTrpls, p, lo.FilterOptions)
		if err != nil {
			return err
		}
	}

	for _, t := range selectedTrpls {
		if t != nil && ckr.CheckLimitAndUpdate() {
			subjs <- t.Subject()
		}
	}

	return nil
}

// PredicatesForSubjectAndObject publishes all predicates available for the
// given subject and object to the provided channel.
func (m *memory) PredicatesForSubjectAndObject(ctx context.Context, s *node.Node, o *triple.Object, lo *storage.LookupOptions, prds chan<- *predicate.Predicate) error {
	if prds == nil {
		return fmt.Errorf("cannot provide an empty channel")
	}

	sUUID := UUIDToByteString(s.UUID())
	oUUID := UUIDToByteString(o.UUID())
	soIdx := sUUID + oUUID
	m.rwmu.RLock()
	defer m.rwmu.RUnlock()
	defer close(prds)

	ckr := newChecker(lo, nil)
	selectedTrpls := applyGlobalTimeBounds(m.idxSO[soIdx], ckr)

	var err error
	if lo.LatestAnchor {
		if lo.FilterOptions != nil {
			return fmt.Errorf("cannot have LatestAnchor and FilterOptions used at the same time inside lookup options")
		}
		lo.FilterOptions = &filter.StorageOptions{
			Operation: filter.Latest,
			Field:     filter.PredicateField,
		}
		// To guarantee that "lo.FilterOptions" will be cleaned at the driver level, since it was artificially created at the driver level for "LatestAnchor".
		defer func() {
			lo.FilterOptions = (*filter.StorageOptions)(nil)
		}()
	}
	if lo.FilterOptions != nil {
		selectedTrpls, err = executeFilter(selectedTrpls, nil, lo.FilterOptions)
		if err != nil {
			return err
		}
	}

	for _, t := range selectedTrpls {
		if t != nil && ckr.CheckLimitAndUpdate() {
			prds <- t.Predicate()
		}
	}

	return nil
}

// PredicatesForSubject publishes all the predicates known for the given
// subject to the provided channel.
func (m *memory) PredicatesForSubject(ctx context.Context, s *node.Node, lo *storage.LookupOptions, prds chan<- *predicate.Predicate) error {
	if prds == nil {
		return fmt.Errorf("cannot provide an empty channel")
	}

	sUUID := UUIDToByteString(s.UUID())
	m.rwmu.RLock()
	defer m.rwmu.RUnlock()
	defer close(prds)

	ckr := newChecker(lo, nil)
	selectedTrpls := applyGlobalTimeBounds(m.idxS[sUUID], ckr)

	var err error
	if lo.LatestAnchor {
		if lo.FilterOptions != nil {
			return fmt.Errorf("cannot have LatestAnchor and FilterOptions used at the same time inside lookup options")
		}
		lo.FilterOptions = &filter.StorageOptions{
			Operation: filter.Latest,
			Field:     filter.PredicateField,
		}
		// To guarantee that "lo.FilterOptions" will be cleaned at the driver level, since it was artificially created at the driver level for "LatestAnchor".
		defer func() {
			lo.FilterOptions = (*filter.StorageOptions)(nil)
		}()
	}
	if lo.FilterOptions != nil {
		selectedTrpls, err = executeFilter(selectedTrpls, nil, lo.FilterOptions)
		if err != nil {
			return err
		}
	}

	for _, t := range selectedTrpls {
		if t != nil && ckr.CheckLimitAndUpdate() {
			prds <- t.Predicate()
		}
	}

	return nil
}

// PredicatesForObject publishes all the predicates known for the given object
// to the provided channel.
func (m *memory) PredicatesForObject(ctx context.Context, o *triple.Object, lo *storage.LookupOptions, prds chan<- *predicate.Predicate) error {
	if prds == nil {
		return fmt.Errorf("cannot provide an empty channel")
	}

	oUUID := UUIDToByteString(o.UUID())
	m.rwmu.RLock()
	defer m.rwmu.RUnlock()
	defer close(prds)

	ckr := newChecker(lo, nil)
	selectedTrpls := applyGlobalTimeBounds(m.idxO[oUUID], ckr)

	var err error
	if lo.LatestAnchor {
		if lo.FilterOptions != nil {
			return fmt.Errorf("cannot have LatestAnchor and FilterOptions used at the same time inside lookup options")
		}
		lo.FilterOptions = &filter.StorageOptions{
			Operation: filter.Latest,
			Field:     filter.PredicateField,
		}
		// To guarantee that "lo.FilterOptions" will be cleaned at the driver level, since it was artificially created at the driver level for "LatestAnchor".
		defer func() {
			lo.FilterOptions = (*filter.StorageOptions)(nil)
		}()
	}
	if lo.FilterOptions != nil {
		selectedTrpls, err = executeFilter(selectedTrpls, nil, lo.FilterOptions)
		if err != nil {
			return err
		}
	}

	for _, t := range selectedTrpls {
		if t != nil && ckr.CheckLimitAndUpdate() {
			prds <- t.Predicate()
		}
	}

	return nil
}

// TriplesForSubject publishes all triples available for the given subject to
// the provided channel.
func (m *memory) TriplesForSubject(ctx context.Context, s *node.Node, lo *storage.LookupOptions, trpls chan<- *triple.Triple) error {
	if trpls == nil {
		return fmt.Errorf("cannot provide an empty channel")
	}

	sUUID := UUIDToByteString(s.UUID())
	m.rwmu.RLock()
	defer m.rwmu.RUnlock()
	defer close(trpls)

	ckr := newChecker(lo, nil)
	selectedTrpls := applyGlobalTimeBounds(m.idxS[sUUID], ckr)

	var err error
	if lo.LatestAnchor {
		if lo.FilterOptions != nil {
			return fmt.Errorf("cannot have LatestAnchor and FilterOptions used at the same time inside lookup options")
		}
		lo.FilterOptions = &filter.StorageOptions{
			Operation: filter.Latest,
			Field:     filter.PredicateField,
		}
		// To guarantee that "lo.FilterOptions" will be cleaned at the driver level, since it was artificially created at the driver level for "LatestAnchor".
		defer func() {
			lo.FilterOptions = (*filter.StorageOptions)(nil)
		}()
	}
	if lo.FilterOptions != nil {
		selectedTrpls, err = executeFilter(selectedTrpls, nil, lo.FilterOptions)
		if err != nil {
			return err
		}
	}

	for _, t := range selectedTrpls {
		if t != nil && ckr.CheckLimitAndUpdate() {
			trpls <- t
		}
	}

	return nil
}

// TriplesForPredicate publishes all triples available for the given predicate
// to the provided channel.
func (m *memory) TriplesForPredicate(ctx context.Context, p *predicate.Predicate, lo *storage.LookupOptions, trpls chan<- *triple.Triple) error {
	if trpls == nil {
		return fmt.Errorf("cannot provide an empty channel")
	}

	pUUID := UUIDToByteString(p.PartialUUID())
	m.rwmu.RLock()
	defer m.rwmu.RUnlock()
	defer close(trpls)

	ckr := newChecker(lo, p)
	selectedTrpls := applyGlobalTimeBounds(m.idxP[pUUID], ckr)

	var err error
	if lo.LatestAnchor {
		if lo.FilterOptions != nil {
			return fmt.Errorf("cannot have LatestAnchor and FilterOptions used at the same time inside lookup options")
		}
		lo.FilterOptions = &filter.StorageOptions{
			Operation: filter.Latest,
			Field:     filter.PredicateField,
		}
		// To guarantee that "lo.FilterOptions" will be cleaned at the driver level, since it was artificially created at the driver level for "LatestAnchor".
		defer func() {
			lo.FilterOptions = (*filter.StorageOptions)(nil)
		}()
	}
	if lo.FilterOptions != nil {
		selectedTrpls, err = executeFilter(selectedTrpls, p, lo.FilterOptions)
		if err != nil {
			return err
		}
	}

	for _, t := range selectedTrpls {
		if t != nil && ckr.CheckLimitAndUpdate() {
			trpls <- t
		}
	}

	return nil
}

// TriplesForObject publishes all triples available for the given object to the
// provided channel.
func (m *memory) TriplesForObject(ctx context.Context, o *triple.Object, lo *storage.LookupOptions, trpls chan<- *triple.Triple) error {
	if trpls == nil {
		return fmt.Errorf("cannot provide an empty channel")
	}

	oUUID := UUIDToByteString(o.UUID())
	m.rwmu.RLock()
	defer m.rwmu.RUnlock()
	defer close(trpls)

	ckr := newChecker(lo, nil)
	selectedTrpls := applyGlobalTimeBounds(m.idxO[oUUID], ckr)

	var err error
	if lo.LatestAnchor {
		if lo.FilterOptions != nil {
			return fmt.Errorf("cannot have LatestAnchor and FilterOptions used at the same time inside lookup options")
		}
		lo.FilterOptions = &filter.StorageOptions{
			Operation: filter.Latest,
			Field:     filter.PredicateField,
		}
		// To guarantee that "lo.FilterOptions" will be cleaned at the driver level, since it was artificially created at the driver level for "LatestAnchor".
		defer func() {
			lo.FilterOptions = (*filter.StorageOptions)(nil)
		}()
	}
	if lo.FilterOptions != nil {
		selectedTrpls, err = executeFilter(selectedTrpls, nil, lo.FilterOptions)
		if err != nil {
			return err
		}
	}

	for _, t := range selectedTrpls {
		if t != nil && ckr.CheckLimitAndUpdate() {
			trpls <- t
		}
	}

	return nil
}

// TriplesForSubjectAndPredicate publishes all triples available for the given
// subject and predicate to the provided channel.
func (m *memory) TriplesForSubjectAndPredicate(ctx context.Context, s *node.Node, p *predicate.Predicate, lo *storage.LookupOptions, trpls chan<- *triple.Triple) error {
	if trpls == nil {
		return fmt.Errorf("cannot provide an empty channel")
	}

	sUUID := UUIDToByteString(s.UUID())
	pUUID := UUIDToByteString(p.PartialUUID())
	spIdx := sUUID + pUUID
	m.rwmu.RLock()
	defer m.rwmu.RUnlock()
	defer close(trpls)

	ckr := newChecker(lo, p)
	selectedTrpls := applyGlobalTimeBounds(m.idxSP[spIdx], ckr)

	var err error
	if lo.LatestAnchor {
		if lo.FilterOptions != nil {
			return fmt.Errorf("cannot have LatestAnchor and FilterOptions used at the same time inside lookup options")
		}
		lo.FilterOptions = &filter.StorageOptions{
			Operation: filter.Latest,
			Field:     filter.PredicateField,
		}
		// To guarantee that "lo.FilterOptions" will be cleaned at the driver level, since it was artificially created at the driver level for "LatestAnchor".
		defer func() {
			lo.FilterOptions = (*filter.StorageOptions)(nil)
		}()
	}
	if lo.FilterOptions != nil {
		selectedTrpls, err = executeFilter(selectedTrpls, p, lo.FilterOptions)
		if err != nil {
			return err
		}
	}

	for _, t := range selectedTrpls {
		if t != nil && ckr.CheckLimitAndUpdate() {
			trpls <- t
		}
	}

	return nil
}

// TriplesForPredicateAndObject publishes all triples available for the given
// predicate and object to the provided channel.
func (m *memory) TriplesForPredicateAndObject(ctx context.Context, p *predicate.Predicate, o *triple.Object, lo *storage.LookupOptions, trpls chan<- *triple.Triple) error {
	if trpls == nil {
		return fmt.Errorf("cannot provide an empty channel")
	}

	pUUID := UUIDToByteString(p.PartialUUID())
	oUUID := UUIDToByteString(o.UUID())
	poIdx := pUUID + oUUID
	m.rwmu.RLock()
	defer m.rwmu.RUnlock()
	defer close(trpls)

	ckr := newChecker(lo, p)
	selectedTrpls := applyGlobalTimeBounds(m.idxPO[poIdx], ckr)

	var err error
	if lo.LatestAnchor {
		if lo.FilterOptions != nil {
			return fmt.Errorf("cannot have LatestAnchor and FilterOptions used at the same time inside lookup options")
		}
		lo.FilterOptions = &filter.StorageOptions{
			Operation: filter.Latest,
			Field:     filter.PredicateField,
		}
		// To guarantee that "lo.FilterOptions" will be cleaned at the driver level, since it was artificially created at the driver level for "LatestAnchor".
		defer func() {
			lo.FilterOptions = (*filter.StorageOptions)(nil)
		}()
	}
	if lo.FilterOptions != nil {
		selectedTrpls, err = executeFilter(selectedTrpls, p, lo.FilterOptions)
		if err != nil {
			return err
		}
	}

	for _, t := range selectedTrpls {
		if t != nil && ckr.CheckLimitAndUpdate() {
			trpls <- t
		}
	}

	return nil
}

// Exist checks if the provided triple exists on the store.
func (m *memory) Exist(ctx context.Context, t *triple.Triple) (bool, error) {
	suuid := UUIDToByteString(t.UUID())
	m.rwmu.RLock()
	defer m.rwmu.RUnlock()
	_, ok := m.idx[suuid]
	return ok, nil
}

// Triples allows to iterate over all available triples by pushing them to the
// provided channel.
func (m *memory) Triples(ctx context.Context, lo *storage.LookupOptions, trpls chan<- *triple.Triple) error {
	if trpls == nil {
		return fmt.Errorf("cannot provide an empty channel")
	}

	m.rwmu.RLock()
	defer m.rwmu.RUnlock()
	defer close(trpls)

	ckr := newChecker(lo, nil)
	selectedTrpls := applyGlobalTimeBounds(m.idx, ckr)

	var err error
	if lo.LatestAnchor {
		if lo.FilterOptions != nil {
			return fmt.Errorf("cannot have LatestAnchor and FilterOptions used at the same time inside lookup options")
		}
		lo.FilterOptions = &filter.StorageOptions{
			Operation: filter.Latest,
			Field:     filter.PredicateField,
		}
		// To guarantee that "lo.FilterOptions" will be cleaned at the driver level, since it was artificially created at the driver level for "LatestAnchor".
		defer func() {
			lo.FilterOptions = (*filter.StorageOptions)(nil)
		}()
	}
	if lo.FilterOptions != nil {
		selectedTrpls, err = executeFilter(selectedTrpls, nil, lo.FilterOptions)
		if err != nil {
			return err
		}
	}

	for _, t := range selectedTrpls {
		if t != nil && ckr.CheckLimitAndUpdate() {
			trpls <- t
		}
	}

	return nil
}
