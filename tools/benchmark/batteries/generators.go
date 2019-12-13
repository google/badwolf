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

// Package batteries generates the benchmarks used for testing.
package batteries

import (
	"github.com/google/badwolf/tools/benchmark/generator"
	"github.com/google/badwolf/tools/benchmark/generator/graph"
	"github.com/google/badwolf/tools/benchmark/generator/tree"
)

// getTreeGenerators returns the set of tree generators to use while creating
// benchmarks.
func getTreeGenerators(bFactors []int) ([]generator.Generator, error) {
	var gens []generator.Generator
	for _, b := range bFactors {
		t, err := tree.New(b)
		if err != nil {
			return nil, err
		}
		gens = append(gens, t)
	}
	return gens, nil
}

// getGraphGenerators returns the set of tree generators to use while creating
// benchmarks.
func getGraphGenerators(nodes []int) ([]generator.Generator, error) {
	var gens []generator.Generator
	for _, b := range nodes {
		t, err := graph.NewRandomGraph(b)
		if err != nil {
			return nil, err
		}
		gens = append(gens, t)
	}
	return gens, nil
}
