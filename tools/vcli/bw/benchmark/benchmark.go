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

// Package benchmark runs a set of canned benchmarks against the provided
// driver.
package benchmark

import (
	"fmt"
	"log"
	"sort"
	"time"

	"golang.org/x/net/context"

	"github.com/google/badwolf/storage"
	"github.com/google/badwolf/tools/benchmark/batteries"
	"github.com/google/badwolf/tools/benchmark/runtime"
	"github.com/google/badwolf/tools/vcli/bw/command"
)

// New create the version command.
func New(store storage.Store, chanSize int) *command.Command {
	return &command.Command{
		Run: func(ctx context.Context, args []string) int {
			return runAll(ctx, store, chanSize)
		},
		UsageLine: "benchmark",
		Short:     "runs a set of precan benchmarks.",
		Long: `Runs and prints the runtime statistics of running a set of precanned
benchmarks. They include thinngs like bulk addition and delition of triples to
arbitrary sets of BQL queries. The benchmark uses mostly data generated using
a tree or a random graph generator.`,
	}
}

// runAll executes all the canned benchmarks and prints out the stats.
func runAll(ctx context.Context, st storage.Store, chanSize int) int {
	//   - Add non existing triples.        (done)
	//   - Add triples that already exist.  (done)
	//   - Remove non existing triples.     (done)
	//   - Remove existing triples.         (done)
	//   - BQL tree walking from root.      (done)
	//   - BQL random graph hopping.        (done)
	//   - BQL sorting.                     (done)
	//   - BQL grouping.                    (done)
	//   - BQL counting.                    (bounded by sort and grouping)
	//   - BQL filter existent              (bounded by sort and grouping)
	//   - BQL filter non existent          (bounded by sort and grouping)
	fmt.Printf("DISCLAIMER: Running this benchmarks is expensive. Consider using a machine with at least 3G of RAM.\n\n")

	var out int
	// Add non existing triples.
	out += runBattery(ctx, st, "adding non existing tree triples", chanSize, batteries.AddTreeTriplesBenchmark)
	out += runBattery(ctx, st, "adding non existing graph triples", chanSize, batteries.AddGraphTriplesBenchmark)

	// Add existing triples.
	out += runBattery(ctx, st, "adding existing tree triples", chanSize, batteries.AddExistingTreeTriplesBenchmark)
	out += runBattery(ctx, st, "adding existing graph triples", chanSize, batteries.AddExistingGraphTriplesBenchmark)

	// Remove non existing triples.
	out += runBattery(ctx, st, "removing non existing tree triples", chanSize, batteries.RemoveTreeTriplesBenchmark)
	out += runBattery(ctx, st, "removing non existing graph triples", chanSize, batteries.RemoveGraphTriplesBenchmark)

	// Remove existing triples.
	out += runBattery(ctx, st, "removing existing tree triples", chanSize, batteries.RemoveExistingTreeTriplesBenchmark)
	out += runBattery(ctx, st, "removing existing graph triples", chanSize, batteries.RemoveExistingGraphTriplesBenchmark)

	// BQL graph walking.
	out += runBattery(ctx, st, "walking the tree graph with BQL", chanSize, batteries.BQLTreeGraphWalking)
	out += runBattery(ctx, st, "walking the random graph with BQL", chanSize, batteries.BQLRandomGraphWalking)
	return out
}

// runBattery executes all the canned benchmarks and prints out the stats.
func runBattery(ctx context.Context, st storage.Store, name string, chanSize int, f func(context.Context, storage.Store, int) ([]*runtime.BenchEntry, error)) int {
	// Add triples.
	fmt.Printf("Creating %s triples benchmark... ", name)
	bes, err := f(ctx, st, chanSize)
	if err != nil {
		log.Printf("[ERROR] %v\n", err)
		return 2
	}
	fmt.Printf("%d entries created\n", len(bes))

	fmt.Printf("Run %s benchmark sequentially... ", name)
	ts := time.Now()
	brs := runtime.RunBenchmarkBatterySequentially(bes)
	ds := time.Now().Sub(ts)
	fmt.Printf("(%v) done\n", ds)

	fmt.Printf("Run %s benchmark concurrently... ", name)
	tc := time.Now()
	brc := runtime.RunBenchmarkBatteryConcurrently(bes)
	dc := time.Now().Sub(tc)
	fmt.Printf("(%v) done\n\n", dc)

	format := func(br *runtime.BenchResult) string {
		if br.Err != nil {
			return fmt.Sprintf("%20s - %20s -[ERROR] %v", br.BatteryID, br.ID, br.Err)
		}
		tps := float64(br.Triples) / (float64(br.Mean) / float64(time.Second))
		return fmt.Sprintf("%20s - %20s - %05.2f triples/sec - %v/%v", br.BatteryID, br.ID, tps, br.Mean, br.StdDev)
	}

	sortAndPrint := func(ss []string) {
		sort.Strings(ss)
		for _, s := range ss {
			fmt.Println(s)
		}
	}

	fmt.Printf("Stats for sequentially run %s benchmark\n", name)
	var ress []string
	for _, br := range brs {
		ress = append(ress, format(br))
	}
	sortAndPrint(ress)
	fmt.Println()

	fmt.Printf("Stats for concurrently run %s benchmark\n", name)
	var resc []string
	for _, br := range brc {
		resc = append(resc, format(br))
	}
	sortAndPrint(resc)
	fmt.Println()

	return 0
}
