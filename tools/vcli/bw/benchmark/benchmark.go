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
	"os"
	"sort"
	"time"

	"golang.org/x/net/context"

	"github.com/google/badwolf/storage"
	"github.com/google/badwolf/tools/benchmark/batteries"
	"github.com/google/badwolf/tools/benchmark/runtime"
	"github.com/google/badwolf/tools/vcli/bw/command"
)

// New create the version command.
func New(store storage.Store) *command.Command {
	return &command.Command{
		Run: func(ctx context.Context, args []string) int {
			return runAll(ctx, store)
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
func runAll(ctx context.Context, st storage.Store) int {
	//   - Add non existing triples.        (done)
	//   - Add triples that already exist.  (todo)
	//   - Remove non existing triples.     (todo)
	//   - Remove existing triples.         (todo)
	//   - BQL tree walking from root.      (todo)
	//   - BQL random graph hopping.        (todo)
	//   - BQL sorting.                     (todo)
	//   - BQL grouping.                    (todo)
	//   - BQL counting.                    (todo)
	//   - BQL filter existent              (todo)
	//   - BQL filter non existent          (todo)
	return runAddTriples(ctx, st)
}

// runAddTriples executes all the canned benchmarks and prints out the stats.
func runAddTriples(ctx context.Context, st storage.Store) int {
	// Add triples.
	fmt.Print("Creating add tree triples benchmark... ")
	bes, err := batteries.AddTreeTriplesBenchmark(ctx, st)
	if err != nil {
		fmt.Fprintf(os.Stderr, "[ERROR] %v\n", err)
		return 2
	}
	fmt.Printf("%d entries created\n", len(bes))

	fmt.Print("Creating add graph triples benchmark... ")
	gbes, err := batteries.AddGraphTriplesBenchmark(ctx, st)
	if err != nil {
		fmt.Fprintf(os.Stderr, "[ERROR] %v\n", err)
		return 2
	}
	bes = append(bes, gbes...)
	fmt.Printf("%d entries created\n\n", len(gbes))

	fmt.Print("Run add triple benchmark sequentially... ")
	ts := time.Now()
	brs := runtime.RunBenchmarkBatterySequentially(bes)
	ds := time.Now().Sub(ts)
	fmt.Printf("(%v) done\n", ds)

	fmt.Print("Run add triple benchmark concurrently... ")
	tc := time.Now()
	brc := runtime.RunBenchmarkBatteryConcurrently(bes)
	dc := time.Now().Sub(tc)
	fmt.Printf("(%v) done\n\n", dc)

	format := func(br *runtime.BenchResult) string {
		if br.Err != nil {
			return fmt.Sprintf("%20s - %20s -[ERROR] %v", br.BatteryID, br.ID, br.Err)
		}
		return fmt.Sprintf("%20s - %20s - %v/%v", br.BatteryID, br.ID, br.Mean, br.StdDev)
	}

	sortAndPrint := func(ss []string) {
		sort.Strings(ss)
		for _, s := range ss {
			fmt.Println(s)
		}
	}

	fmt.Println("Stats for sequentially run add triple benchmark")
	var ress []string
	for _, br := range brs {
		ress = append(ress, format(br))
	}
	sortAndPrint(ress)
	fmt.Println()

	fmt.Println("Stats for concurrently run add triple benchmark")
	var resc []string
	for _, br := range brc {
		resc = append(resc, format(br))
	}
	sortAndPrint(resc)
	fmt.Println()

	return 0
}
