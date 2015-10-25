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

// Package storage provides the abstraction to build drivers for BadWolf.
package storage

import (
	"time"

	"github.com/google/badwolf/triple"
	"github.com/google/badwolf/triple/node"
	"github.com/google/badwolf/triple/predicate"
)

// Triples provides a read only channel of triples.
type Triples <-chan *triple.Triple

// Nodes provides a read only channel of nodes.
type Nodes <-chan *node.Node

// Predicates provides a read only channel of predicates.
type Predicates <-chan *predicate.Predicate

// Objects provides a read only channel of objects.
type Objects <-chan *triple.Object

// LookupOptions allows to specify the behavior of the lookup operations.
type LookupOptions struct {
	// MaxElements list the maximum number of elements to return. If not
	// set it returns all the lookup results.
	MaxElements int

	// LowerAnchor if provided represents the lower time anchor to be considered.
	LowerAnchor *time.Time

	// UpperArnchor if provided represents the upper time anchor to be considered.
	UpperAnchor *time.Time
}

// DefaultLookup provides the default lookup behavior.
var DefaultLookup = &LookupOptions{}

// Store interface describes the low lever API that allows to create new graphs.
type Store interface {
	// Name returns the ID of the backend being used.
	Name() string

	// Version returns the version of the driver implementation.
	Version() string

	// NewGraph creates a new graph.
	NewGraph(id string) (Graph, error)

	// Graph return an existing graph if available. Getting a non existing
	// graph should return and error.
	Graph(id string) (Graph, error)

	// DeleteGraph with delete an existing graph. Deleting a non existing graph
	// should return and error.
	DeleteGraph(id string) error
}

// Graph interface describes the low level API that storage drivers need
// to implment to provide a compliant graph storage that can be use with
// BadWolf.
type Graph interface {
	// ID returns the id for this graph.
	ID() string

	// AddTriples adds the triples to the storage. Adding a triple that already
	// exist should not fail.
	AddTriples(ts []*triple.Triple) error

	// RemoveTriples removes the trilpes from the storage. Removing triples that
	// are not present on the store shot not fail.
	RemoveTriples(ts []*triple.Triple) error

	// Objects returns the objects for the give object and predicate.
	//
	// Given a subject and a predicate, this method retrieves the objects of
	// triples that matches them. By default, if does not limit the maximum number
	// of possible objects returned, unless properly specified by in the lookup
	// options provided.
	//
	// If the provided predicate is immutable it will return all the possible
	// subject values or the number of max elements specified. There is no
	// requirement on how to sample the returned max elements.
	//
	// If the predicate is an unanchored temporal triple and no time anchors are
	// provided in the lookup options, it will return all the available objects.
	// If time anchors are provided, it will return all the values anchored in the
	// provided time window. If max elements is also provided as part of the
	// lookup options it will return the at most max elements. There is no
	// specifications on how that sample should be conducted.
	Objects(s *node.Node, p *predicate.Predicate, lo *LookupOptions) (Objects, error)

	// Subject returns the subjects for the give predicate and object.
	//
	// Given a predicate and an object, this method retrieves the subbjects of
	// triples that matches them. By default, if does not limit the maximum number
	// of possible subjects returned, unless properly specified by in the lookup
	// options provided.
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
	Subjects(p *predicate.Predicate, o *triple.Object, lo *LookupOptions) (Nodes, error)

	// PredicatesForSubject returns all the predicats know for the given
	// subject. If the lookup options provide a max number of elements the
	// function will return a sample of the available predicates. If time anchor
	// bounds are provided in the lookup options, only predicates matching the
	// the provided type window would be return. Same sampling consideration
	// apply if max element is provided.
	PredicatesForSubject(s *node.Node, lo *LookupOptions) (Predicates, error)

	// PredicatesForObject returns all the predicats know for the given
	// object. If the lookup options provide a max number of elements the
	// function will return a sample of the available predicates. If time anchor
	// bounds are provided in the lookup options, only predicates matching the
	// the provided type window would be return. Same sampling consideration
	// apply if max element is provided.
	PredicatesForObject(o *triple.Object, lo *LookupOptions) (Predicates, error)

	// PredicatesForSubjecAndObject returns all predicates available for the
	// given subject and object. If the lookup options provide a max number of
	// elements the function will return a sample of the available predicates.
	// If time anchor bounds are provided in the lookup options, only predicates
	// matching the the provided type window would be return. Same sampling
	// consideration apply if max element is provided.
	PredicatesForSubjectAndObject(s *node.Node, o *triple.Object, lo *LookupOptions) (Predicates, error)

	// TriplesForSubject returns all triples available for a given subect.
	// If the lookup options provide a max number of elements the function will
	// return a sample of the available triples. If time anchor bounds are
	// provided in the lookup options, only predicates matching the the provided
	// type window would be return. Same sampling consideration apply if max
	// element is provided.
	TriplesForSubject(s *node.Node, lo *LookupOptions) (Triples, error)

	// TriplesForObject returns all triples available for a given object.
	// If the lookup options provide a max number of elements the function will
	// return a sample of the available triples. If time anchor bounds are
	// provided in the lookup options, only predicates matching the the provided
	// type window would be return. Same sampling consideration apply if max
	// element is provided.
	TriplesForObject(o *triple.Object, lo *LookupOptions) (Triples, error)

	// TriplesForSubjectAndPredicate returns all triples available for the given
	// subject and predicate. If the lookup options provide a max number of
	// elements the function will return a sample of the available triples. If
	// time anchor bounds are provided in the lookup options, only predicates
	// matching the the provided type window would be return. Same sampling
	// consideration apply if max element is provided.
	TriplesForSubjectAndPredicate(s *node.Node, p *predicate.Predicate, lo *LookupOptions) (Triples, error)

	// TriplesForPredicateAndObject returns all triples available for the given
	// predicate and object. If the lookup options provide a max number of
	// elements the function will return a sample of the available triples. If
	// time anchor bounds are provided in the lookup options, only predicates
	// matching the the provided type window would be return. Same sampling
	// consideration apply if max element is provided.
	TriplesForPredicateAndObject(p *predicate.Predicate, o *triple.Object, lo *LookupOptions) (Triples, error)

	// Exists checks if the provided triple exist on the store.
	Exist(t *triple.Triple) (bool, error)

	// Triples allows to iterate over all available triples.
	Triples() (Triples, error)
}
