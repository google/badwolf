package memory

import (
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
		id: id,
	}, nil
}

// memory provides an imemory volatile implemention of the storage API.
type memory struct {
	id string
}

// ID returns the id for this graph.
func (m *memory) ID() string {
	return m.id
}

// AddTriples adds the triples to the storage.
func (m *memory) AddTriples(ts []*triple.Triple) error {
	return nil
}

// RemoveTriples removes the trilpes from the storage.
func (m *memory) RemoveTriples(ts []*triple.Triple) error {
	return nil
}

// Objects returns the objects for the give object and predicate.
func (m *memory) Objects(s *node.Node, p *predicate.Predicate, lo *storage.LookupOptions) (storage.Objects, error) {
	return nil, nil
}

// Subject returns the subjects for the give predicate and object.
func (m *memory) Subjects(p *predicate.Predicate, o *triple.Object, lo *storage.LookupOptions) (storage.Nodes, error) {
	return nil, nil
}

// PredicatesForSubject returns all the predicats know for the given
// subject.
func (m *memory) PredicatesForSubject(s *node.Node, lo *storage.LookupOptions) (storage.Predicates, error) {
	return nil, nil
}

// PredicatesForObject returns all the predicats know for the given
// object.
func (m *memory) PredicatesForObject(o *triple.Object, lo *storage.LookupOptions) (storage.Predicates, error) {
	return nil, nil
}

// PredicatesForSubjecAndObject returns all predicates available for the
// given subject and object.
func (m *memory) PredicatesForSubjectAndObject(s *node.Node, o *triple.Object, lo *storage.LookupOptions) (storage.Predicates, error) {
	return nil, nil
}

// TriplesForSubject returns all triples available for a given subect.
func (m *memory) TriplesForSubject(s *node.Node, lo *storage.LookupOptions) (storage.Triples, error) {
	return nil, nil
}

// TriplesForObject returns all triples available for a given object.
func (m *memory) TriplesForObject(o *triple.Object, lo *storage.LookupOptions) (storage.Triples, error) {
	return nil, nil
}

// TriplesForSubjectAndPredicate returns all triples available for the given
// subject and predicate.
func (m *memory) TriplesForSubjectAndPredicate(s *node.Node, p *predicate.Predicate, lo *storage.LookupOptions) (storage.Triples, error) {
	return nil, nil
}

// TriplesForPredicateAndObject returns all triples available for the given
// predicate and object.
func (m *memory) TriplesForPredicateAndObject(p *predicate.Predicate, o *triple.Object, lo *storage.LookupOptions) (storage.Triples, error) {
	return nil, nil
}

// Exists checks if the provided triple exist on the store.
func (m *memory) Exist(t *triple.Triple) (bool, error) {
	return false, nil
}
