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
	"context"
	"fmt"

	"github.com/google/badwolf/storage"
	"github.com/google/badwolf/tools/benchmark/runtime"
	"github.com/google/badwolf/triple"
)

// AddTreeTriplesBenchmark creates the benchmark.
func AddTreeTriplesBenchmark(ctx context.Context, st storage.Store, chanSize, bulkSize int) ([]*runtime.BenchEntry, error) {
	bFactors := []int{2, 200}
	sizes := []int{10, 1000, 100000}
	var trplSets [][]*triple.Triple
	var ids []string
	var gids []string
	var gSizes []int
	gs, err := getTreeGenerators(bFactors)
	if err != nil {
		return nil, err
	}
	for idx, g := range gs {
		for _, s := range sizes {
			ts, err := g.Generate(s)
			if err != nil {
				return nil, err
			}
			trplSets = append(trplSets, ts)
			ids = append(ids, fmt.Sprintf("tg branch_factor=%04d, size=%07d", bFactors[idx], s))
			gids = append(gids, fmt.Sprintf("b%d_s%d", bFactors[idx], s))
			gSizes = append(gSizes, s)
		}
	}
	var bes []*runtime.BenchEntry
	reps := []int{10}
	for i, max := 0, len(ids); i < max; i++ {
		for idxReps, r := range reps {
			var g storage.Graph
			gID := fmt.Sprintf("add_tree_%s_r%d_i%d", gids[i], i, idxReps)
			data := trplSets[i]
			bes = append(bes, &runtime.BenchEntry{
				BatteryID: "Add non existing triples",
				ID:        fmt.Sprintf("%s, reps=%02d", ids[i], r),
				Triples:   gSizes[i],
				Reps:      r,
				Setup: func() error {
					var err error
					g, err = st.NewGraph(ctx, gID)
					return err
				},
				F: func() error {
					return g.AddTriples(ctx, data)
				},
				TearDown: func() error {
					return st.DeleteGraph(ctx, gID)
				},
			})
		}
	}
	return bes, nil
}

// AddExistingTreeTriplesBenchmark creates the benchmark.
func AddExistingTreeTriplesBenchmark(ctx context.Context, st storage.Store, chanSize, bulkSize int) ([]*runtime.BenchEntry, error) {
	bFactors := []int{2, 200}
	sizes := []int{10, 1000, 100000}
	var trplSets [][]*triple.Triple
	var ids []string
	var gids []string
	var gSizes []int
	gs, err := getTreeGenerators(bFactors)
	if err != nil {
		return nil, err
	}
	for idx, g := range gs {
		for _, s := range sizes {
			ts, err := g.Generate(s)
			if err != nil {
				return nil, err
			}
			trplSets = append(trplSets, ts)
			ids = append(ids, fmt.Sprintf("tg branch_factor=%04d, size=%07d", bFactors[idx], s))
			gids = append(gids, fmt.Sprintf("b%d_s%d", bFactors[idx], s))
			gSizes = append(gSizes, s)
		}
	}
	var bes []*runtime.BenchEntry
	reps := []int{10}
	for i, max := 0, len(ids); i < max; i++ {
		for idxReps, r := range reps {
			var g storage.Graph
			gID := fmt.Sprintf("add_existing_tree_%s_r%d_i%d", gids[i], i, idxReps)
			data := trplSets[i]
			bes = append(bes, &runtime.BenchEntry{
				BatteryID: "Add existing triples",
				ID:        fmt.Sprintf("%s, reps=%02d", ids[i], r),
				Triples:   gSizes[i],
				Reps:      r,
				Setup: func() error {
					var err error
					g, err = st.NewGraph(ctx, gID)
					if err != nil {
						return err
					}
					return g.AddTriples(ctx, data)
				},
				F: func() error {
					return g.AddTriples(ctx, data)
				},
				TearDown: func() error {
					return st.DeleteGraph(ctx, gID)
				},
			})
		}
	}
	return bes, nil
}

// AddGraphTriplesBenchmark creates the benchmark.
func AddGraphTriplesBenchmark(ctx context.Context, st storage.Store, chanSize, bulkSize int) ([]*runtime.BenchEntry, error) {
	nodes := []int{317, 1000}
	sizes := []int{10, 1000, 100000}
	var trplSets [][]*triple.Triple
	var ids []string
	var gids []string
	var gSizes []int
	gs, err := getGraphGenerators(nodes)
	if err != nil {
		return nil, err
	}
	for idx, g := range gs {
		for _, s := range sizes {
			ts, err := g.Generate(s)
			if err != nil {
				return nil, err
			}
			trplSets = append(trplSets, ts)
			ids = append(ids, fmt.Sprintf("rg nodes=%04d, size=%07d", nodes[idx], s))
			gids = append(gids, fmt.Sprintf("n%d_s%d", nodes[idx], s))
			gSizes = append(gSizes, s)
		}
	}
	var bes []*runtime.BenchEntry
	reps := []int{10}
	for i, max := 0, len(ids); i < max; i++ {
		for idxReps, r := range reps {
			var g storage.Graph
			gID := fmt.Sprintf("add_graph_%s_r%d_i%d", gids[i], i, idxReps)
			data := trplSets[i]
			bes = append(bes, &runtime.BenchEntry{
				BatteryID: "Add non existing triples",
				ID:        fmt.Sprintf("%s, reps=%02d", ids[i], r),
				Triples:   gSizes[i],
				Reps:      r,
				Setup: func() error {
					var err error
					g, err = st.NewGraph(ctx, gID)
					return err
				},
				F: func() error {
					return g.AddTriples(ctx, data)
				},
				TearDown: func() error {
					return st.DeleteGraph(ctx, gID)
				},
			})
		}
	}
	return bes, nil
}

// AddExistingGraphTriplesBenchmark creates the benchmark.
func AddExistingGraphTriplesBenchmark(ctx context.Context, st storage.Store, chanSize, bulkSize int) ([]*runtime.BenchEntry, error) {
	nodes := []int{317, 1000}
	sizes := []int{10, 1000, 100000}
	var trplSets [][]*triple.Triple
	var ids []string
	var gids []string
	var gSizes []int
	gs, err := getGraphGenerators(nodes)
	if err != nil {
		return nil, err
	}
	for idx, g := range gs {
		for _, s := range sizes {
			ts, err := g.Generate(s)
			if err != nil {
				return nil, err
			}
			trplSets = append(trplSets, ts)
			ids = append(ids, fmt.Sprintf("erg nodes=%04d, size=%07d", nodes[idx], s))
			gids = append(gids, fmt.Sprintf("n%d_s%d", nodes[idx], s))
			gSizes = append(gSizes, s)
		}
	}
	var bes []*runtime.BenchEntry
	reps := []int{10}
	for i, max := 0, len(ids); i < max; i++ {
		for idxReps, r := range reps {
			var g storage.Graph
			gID := fmt.Sprintf("add_existing_graph_%s_r%d_i%d", gids[i], i, idxReps)
			data := trplSets[i]
			bes = append(bes, &runtime.BenchEntry{
				BatteryID: "Add existing triples",
				ID:        fmt.Sprintf("%s, reps=%02d", ids[i], r),
				Triples:   gSizes[i],
				Reps:      r,
				Setup: func() error {
					var err error
					g, err = st.NewGraph(ctx, gID)
					if err != nil {
						return err
					}
					return g.AddTriples(ctx, data)
				},
				F: func() error {
					return g.AddTriples(ctx, data)
				},
				TearDown: func() error {
					return st.DeleteGraph(ctx, gID)
				},
			})
		}
	}
	return bes, nil
}
