package memory

import (
	"strings"

	"badwolf.org/badwolf/storage"
	"badwolf.org/badwolf/triple"
	"badwolf.org/badwolf/triple/node"
	"badwolf.org/badwolf/triple/predicate"
)

// DefaultMemoryStore provides a volatile in memory store.
var DefaultMemoryStore storage.Store = &memoryStore{}

type memoryStore struct{}

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
	// TODO(xllora): Implement graph creation.
	return &memory{
		id:    id,
		idx:   make(map[string]*triple.Triple),
		idxS:  make(map[string]map[string]*triple.Triple),
		idxP:  make(map[string]map[string]*triple.Triple),
		idxO:  make(map[string]map[string]*triple.Triple),
		idxSP: make(map[string]map[string]*triple.Triple),
		idxPO: make(map[string]map[string]*triple.Triple),
		idxSO: make(map[string]map[string]*triple.Triple),
	}, nil
}

// memory provides an imemory volatile implemention of the storage API.
type memory struct {
	id    string
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

// addToIndex add a tirple ot an index without duplicates.
func addToIndex(idx map[string]map[string]*triple.Triple, iGUID, tGUID string, t *triple.Triple) {
	idx[iGUID][tGUID] = t
}

// AddTriples adds the triples to the storage.
func (m *memory) AddTriples(ts []*triple.Triple) error {
	for _, t := range ts {
		guid := t.GUID()
		sGUID := t.S().GUID()
		pGUID := t.P().GUID()
		oGUID := t.O().GUID()
		// Update master index
		m.idx[guid] = t
		m.idxS[sGUID][guid] = t
		m.idxP[pGUID][guid] = t
		m.idxO[oGUID][guid] = t
		m.idxSP[strings.Join([]string{sGUID, pGUID}, ":")][guid] = t
		m.idxPO[strings.Join([]string{pGUID, oGUID}, ":")][guid] = t
		m.idxSO[strings.Join([]string{sGUID, oGUID}, ":")][guid] = t
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
		delete(m.idx, guid)
		delete(m.idxS[sGUID], guid)
		delete(m.idxP[pGUID], guid)
		delete(m.idxO[oGUID], guid)
		delete(m.idxSP[strings.Join([]string{sGUID, pGUID}, ":")], guid)
		delete(m.idxPO[strings.Join([]string{pGUID, oGUID}, ":")], guid)
		delete(m.idxSO[strings.Join([]string{sGUID, oGUID}, ":")], guid)
	}
	return nil
}

// Objects returns the objects for the give object and predicate.
func (m *memory) Objects(s *node.Node, p *predicate.Predicate, lo *storage.LookupOptions) (storage.Objects, error) {
	sGUID := s.GUID()
	pGUID := p.GUID()
	objs := make(chan *triple.Object)
	go func() {
		for _, t := range m.idxSP[strings.Join([]string{sGUID, pGUID}, ":")] {
			objs <- t.O()
		}
		close(objs)
	}()
	return objs, nil
}

// Subject returns the subjects for the give predicate and object.
func (m *memory) Subjects(p *predicate.Predicate, o *triple.Object, lo *storage.LookupOptions) (storage.Nodes, error) {
	pGUID := p.GUID()
	oGUID := o.GUID()
	subs := make(chan *node.Node)
	go func() {
		for _, t := range m.idxPO[strings.Join([]string{pGUID, oGUID}, ":")] {
			subs <- t.S()
		}
		close(subs)
	}()
	return subs, nil
}

// PredicatesForSubjecAndObject returns all predicates available for the
// given subject and object.
func (m *memory) PredicatesForSubjectAndObject(s *node.Node, o *triple.Object, lo *storage.LookupOptions) (storage.Predicates, error) {
	sGUID := s.GUID()
	oGUID := o.GUID()
	preds := make(chan *predicate.Predicate)
	go func() {
		for _, t := range m.idxSO[strings.Join([]string{sGUID, oGUID}, ":")] {
			preds <- t.P()
		}
		close(preds)
	}()
	return preds, nil
}

// PredicatesForSubject returns all the predicats know for the given
// subject.
func (m *memory) PredicatesForSubject(s *node.Node, lo *storage.LookupOptions) (storage.Predicates, error) {
	sGUID := s.GUID()
	preds := make(chan *predicate.Predicate)
	go func() {
		for _, t := range m.idxS[sGUID] {
			preds <- t.P()
		}
		close(preds)
	}()
	return preds, nil
}

// PredicatesForObject returns all the predicats know for the given
// object.
func (m *memory) PredicatesForObject(o *triple.Object, lo *storage.LookupOptions) (storage.Predicates, error) {
	oGUID := o.GUID()
	preds := make(chan *predicate.Predicate)
	go func() {
		for _, t := range m.idxO[oGUID] {
			preds <- t.P()
		}
		close(preds)
	}()
	return preds, nil
}

// TriplesForSubject returns all triples available for a given subect.
func (m *memory) TriplesForSubject(s *node.Node, lo *storage.LookupOptions) (storage.Triples, error) {
	sGUID := s.GUID()
	triples := make(chan *triple.Triple)
	go func() {
		for _, t := range m.idxS[sGUID] {
			triples <- t
		}
		close(triples)
	}()
	return triples, nil
}

// TriplesForObject returns all triples available for a given object.
func (m *memory) TriplesForObject(o *triple.Object, lo *storage.LookupOptions) (storage.Triples, error) {
	oGUID := o.GUID()
	triples := make(chan *triple.Triple)
	go func() {
		for _, t := range m.idxO[oGUID] {
			triples <- t
		}
		close(triples)
	}()
	return triples, nil
}

// TriplesForSubjectAndPredicate returns all triples available for the given
// subject and predicate.
func (m *memory) TriplesForSubjectAndPredicate(s *node.Node, p *predicate.Predicate, lo *storage.LookupOptions) (storage.Triples, error) {
	sGUID := s.GUID()
	pGUID := p.GUID()
	triples := make(chan *triple.Triple)
	go func() {
		for _, t := range m.idxSP[strings.Join([]string{sGUID, pGUID}, ":")] {
			triples <- t
		}
		close(triples)
	}()
	return triples, nil
}

// TriplesForPredicateAndObject returns all triples available for the given
// predicate and object.
func (m *memory) TriplesForPredicateAndObject(p *predicate.Predicate, o *triple.Object, lo *storage.LookupOptions) (storage.Triples, error) {
	pGUID := p.GUID()
	oGUID := o.GUID()
	triples := make(chan *triple.Triple)
	go func() {
		for _, t := range m.idxPO[strings.Join([]string{pGUID, oGUID}, ":")] {
			triples <- t
		}
		close(triples)
	}()
	return triples, nil
}

// Exists checks if the provided triple exist on the store.
func (m *memory) Exist(t *triple.Triple) (bool, error) {
	guid := t.GUID()
	_, ok := m.idx[guid]
	return ok, nil
}
