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

// Package semantic contains the semantic analysis required to have a
// senantically valid parser. It includes the data conversion required to
// turn tokens into valid BadWolf structures. It also provides the hooks
// implementations required for buliding an actionable execution plan.
package semantic

import "github.com/google/badwolf/triple"

// StatementType describes the type of statement being represented.
type StatementType int8

const (
	// Query statement.
	Query StatementType = iota
	// Insert statemrnt.
	Insert
	// Delete statement.
	Delete
)

// String provides a readable version of the StatementType.
func (t StatementType) String() string {
	switch t {
	case Query:
		return "QUERY"
	case Insert:
		return "INSERT"
	case Delete:
		return "DELETE"
	default:
		return "UNKNOWN"
	}
}

// Statement contains all the semantic information extract from the parsing
type Statement struct {
	sType  StatementType
	graphs []string
	data   []*triple.Triple
}

// NewStatement returns a new empty statement.
func NewStatement(st StatementType) *Statement {
	return &Statement{
		sType: st,
	}
}

// Type returns the type of the statement.
func (s *Statement) Type() StatementType {
	return s.sType
}

// AddGraph adds a graph to a given statement.
func (s *Statement) AddGraph(g string) {
	s.graphs = append(s.graphs, g)
}

// Graphs returns the list of graphs listed on the statement.
func (s *Statement) Graphs() []string {
	return s.graphs
}

// AddData adds a triple to a given statement's data.
func (s *Statement) AddData(d *triple.Triple) {
	s.data = append(s.data, d)
}

// Data returns the data available for the given statement.
func (s *Statement) Data() []*triple.Triple {
	return s.data
}
