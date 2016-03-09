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

// Package tree contains the data generator to build the tree bench mark data.
package tree

import (
	"fmt"
	"math"

	"github.com/google/badwolf/tools/benchmark/generator"
	"github.com/google/badwolf/triple"
	"github.com/google/badwolf/triple/node"
	"github.com/google/badwolf/triple/predicate"
)

// treeGenerator generates data modeled after a tree structure.
type treeGenerator struct {
	branch    int
	nodeType  *node.Type
	predicate *predicate.Predicate
}

// New creates a new tree generator. The triples are generated using breadth
// search first. All predicates are immutable and use the predicate
// `"parent_of"@[]`.`
func New(branch int) (generator.Generator, error) {
	if branch < 1 {
		return nil, fmt.Errorf("invalid branch factor %d", branch)
	}
	nt, err := node.NewType("/tn")
	if err != nil {
		return nil, err
	}
	p, err := predicate.NewImmutable("parent_of")
	if err != nil {
		return nil, err
	}
	return &treeGenerator{
		branch:    branch,
		nodeType:  nt,
		predicate: p,
	}, nil
}

// newNode returns a new node for the given identifier.
func (t *treeGenerator) newNode(branch int, parentID string) (*node.Node, error) {
	tid := fmt.Sprintf("%d/%s", branch, parentID)
	if parentID == "" {
		tid = fmt.Sprintf("%d", branch)
	}
	id, err := node.NewID(tid)
	if err != nil {
		return nil, err
	}
	return node.NewNode(t.nodeType, id), nil
}

// newTriple creates a new triple given the parent and the descendent as an object.
func (t *treeGenerator) newTriple(parent, descendent *node.Node) (*triple.Triple, error) {
	return triple.New(parent, t.predicate, triple.NewNodeObject(descendent))
}

// recurse generated the triple by recursing while there are still triples
// left to generate.
func (t *treeGenerator) recurse(parent *node.Node, left *int, currentDepth, maxDepth int, trpls []*triple.Triple) ([]*triple.Triple, error) {
	if *left < 1 {
		return trpls, nil
	}
	for i, last := 0, *left <= t.branch; i < t.branch; i++ {
		offspring, err := t.newNode(i, parent.ID().String())
		if err != nil {
			return trpls, err
		}
		trpl, err := t.newTriple(parent, offspring)
		if err != nil {
			return trpls, err
		}
		trpls = append(trpls, trpl)
		(*left)--
		if *left < 1 {
			break
		}
		if currentDepth < maxDepth && !last {
			ntrpls, err := t.recurse(offspring, left, currentDepth+1, maxDepth, trpls)
			if err != nil {
				return ntrpls, err
			}
			trpls = ntrpls
		}
		if *left < 1 {
			break
		}
	}
	return trpls, nil
}

// Generates the requested number of triples.
func (t *treeGenerator) Generate(n int) ([]*triple.Triple, error) {
	var trpls []*triple.Triple
	if n <= 0 {
		return trpls, nil
	}
	root, err := t.newNode(0, "")
	if err != nil {
		return nil, err
	}
	depth := int(math.Log(float64(n)) / math.Log(float64(t.branch)))
	ntrpls, err := t.recurse(root, &n, 0, depth, trpls)
	if err != nil {
		return nil, err
	}
	return ntrpls, nil
}
