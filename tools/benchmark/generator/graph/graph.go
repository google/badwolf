// Copyright 2016 Google Inc. All rights reserved.
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

// Package graph contains the data generator to build arbitrary graph
// benchmark data.
package graph

import (
	"fmt"
	"math/rand"

	"github.com/google/badwolf/tools/benchmark/generator"
	"github.com/google/badwolf/triple"
	"github.com/google/badwolf/triple/node"
	"github.com/google/badwolf/triple/predicate"
)

// randomGraph generates a random graph using the number of provided nodes.
type randomGraph struct {
	nodes     int
	nodeType  *node.Type
	predicate *predicate.Predicate
}

// NewRandomGraph creates a new random graph generator.
func NewRandomGraph(n int) (generator.Generator, error) {
	if n < 1 {
		return nil, fmt.Errorf("invalid number of nodes %d<1", n)
	}
	nt, err := node.NewType("/gn")
	if err != nil {
		return nil, err
	}
	p, err := predicate.NewImmutable("follow")
	if err != nil {
		return nil, err
	}
	return &randomGraph{
		nodes:     n,
		nodeType:  nt,
		predicate: p,
	}, nil
}

// newNode returns a new node for the given identifier.
func (r *randomGraph) newNode(i int) (*node.Node, error) {
	id, err := node.NewID(fmt.Sprintf("%d", i))
	if err != nil {
		return nil, err
	}
	return node.NewNode(r.nodeType, id), nil
}

// newTriple creates new triple using the provided node IDs.
func (r *randomGraph) newTriple(i, j int) (*triple.Triple, error) {
	s, err := r.newNode(i)
	if err != nil {
		return nil, err
	}
	o, err := r.newNode(j)
	if err != nil {
		return nil, err
	}
	return triple.New(s, r.predicate, triple.NewNodeObject(o))
}

// Generate creates the required number of triples.
func (r *randomGraph) Generate(n int) ([]*triple.Triple, error) {
	maxEdges := r.nodes * r.nodes
	if n > maxEdges {
		return nil, fmt.Errorf("current configuration only allow a max of %d triples (%d requested)", maxEdges, n)
	}
	var trpls []*triple.Triple
	for _, idx := range rand.Perm(maxEdges)[:n] {
		i, j := idx/r.nodes, idx%r.nodes
		t, err := r.newTriple(i, j)
		if err != nil {
			return nil, err
		}
		trpls = append(trpls, t)
	}
	return trpls, nil
}
