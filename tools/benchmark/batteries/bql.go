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
	"fmt"

	"github.com/google/badwolf/storage"
	"github.com/google/badwolf/tools/benchmark/runtime"
	"github.com/google/badwolf/tools/vcli/bw/run"
	"github.com/google/badwolf/triple"
	"golang.org/x/net/context"
)

var treeGraphWalkingBQL = []string{
	`SELECT ?c0
   FROM ?%s
   WHERE {
      /tn<0> "parent_of"@[] ?c0
   };`,
	`SELECT ?c0, ?c1
   FROM ?%s
   WHERE {
      /tn<0> "parent_of"@[] ?c0 .
      ?c0 "parent_of"@[] ?c1
   };`,
	`SELECT ?c0, ?c1, ?c2
   FROM ?%s
   WHERE {
      /tn<0> "parent_of"@[] ?c0 .
      ?c0 "parent_of"@[] ?c1 .
      ?c1 "parent_of"@[] ?c2
   };`,
	`SELECT ?c0, ?c1, ?c2, ?c3
   FROM ?%s
   WHERE {
      /tn<0> "parent_of"@[] ?c0 .
      ?c0 "parent_of"@[] ?c1 .
      ?c1 "parent_of"@[] ?c2 .
      ?c2 "parent_of"@[] ?c3
   };`,
	`SELECT ?c0, ?c1, ?c2, ?c3, ?c4
   FROM ?%s
   WHERE {
      /tn<0> "parent_of"@[] ?c0 .
      ?c0 "parent_of"@[] ?c1 .
      ?c1 "parent_of"@[] ?c2 .
      ?c2 "parent_of"@[] ?c3 .
      ?c3 "parent_of"@[] ?c4
   };`,
	`SELECT ?c0
   FROM ?%s
   WHERE {
      /tn<0> "parent_of"@[] ?c0
   }
   ORDER BY ?c0 DESC;`,
	`SELECT ?c0, ?c1
   FROM ?%s
   WHERE {
      /tn<0> "parent_of"@[] ?c0 .
      ?c0 "parent_of"@[] ?c1
   }
   ORDER BY ?c0 DESC;`,
	`SELECT ?c0, ?c1, ?c2
   FROM ?%s
   WHERE {
      /tn<0> "parent_of"@[] ?c0 .
      ?c0 "parent_of"@[] ?c1 .
      ?c1 "parent_of"@[] ?c2
   }
   ORDER BY ?c0 DESC;`,
	`SELECT ?c0, ?c1, ?c2, ?c3
   FROM ?%s
   WHERE {
      /tn<0> "parent_of"@[] ?c0 .
      ?c0 "parent_of"@[] ?c1 .
      ?c1 "parent_of"@[] ?c2 .
      ?c2 "parent_of"@[] ?c3
   }
   ORDER BY ?c0 DESC;`,
	`SELECT ?c0, ?c1, ?c2, ?c3, ?c4
   FROM ?%s
   WHERE {
      /tn<0> "parent_of"@[] ?c0 .
      ?c0 "parent_of"@[] ?c1 .
      ?c1 "parent_of"@[] ?c2 .
      ?c2 "parent_of"@[] ?c3 .
      ?c3 "parent_of"@[] ?c4
   }
   ORDER BY ?c0 DESC;`,
	`SELECT ?c0
   FROM ?%s
   WHERE {
      /tn<0> "parent_of"@[] ?c0
   }
   GROUP BY ?c0;`,
	`SELECT ?c0, ?c1
   FROM ?%s
   WHERE {
      /tn<0> "parent_of"@[] ?c0 .
      ?c0 "parent_of"@[] ?c1
   }
   GROUP BY ?c0, ?c1;`,
	`SELECT ?c0, ?c1, ?c2
   FROM ?%s
   WHERE {
      /tn<0> "parent_of"@[] ?c0 .
      ?c0 "parent_of"@[] ?c1 .
      ?c1 "parent_of"@[] ?c2
   }
   GROUP BY ?c0, ?c1, ?c2;`,
	`SELECT ?c0, ?c1, ?c2, ?c3
   FROM ?%s
   WHERE {
      /tn<0> "parent_of"@[] ?c0 .
      ?c0 "parent_of"@[] ?c1 .
      ?c1 "parent_of"@[] ?c2 .
      ?c2 "parent_of"@[] ?c3
   }
   GROUP BY ?c0, ?c1, ?c2, ?c3;`,
	`SELECT ?c0, ?c1, ?c2, ?c3, ?c4
   FROM ?%s
   WHERE {
      /tn<0> "parent_of"@[] ?c0 .
      ?c0 "parent_of"@[] ?c1 .
      ?c1 "parent_of"@[] ?c2 .
      ?c2 "parent_of"@[] ?c3 .
      ?c3 "parent_of"@[] ?c4
   }
   GROUP BY ?c0, ?c1, ?c2, ?c3, ?c4;`,
}

var randomGraphWalkingBQL = []string{
	`SELECT ?c0
   FROM ?%s
   WHERE {
      /tn<0> "follow"@[] ?c0
   };`,
	`SELECT ?c0, ?c1
   FROM ?%s
   WHERE {
      /tn<0> "follow"@[] ?c0 .
      ?c0 "follow"@[] ?c1
   };`,
	`SELECT ?c0, ?c1, ?c2
   FROM ?%s
   WHERE {
      /tn<0> "follow"@[] ?c0 .
      ?c0 "follow"@[] ?c1 .
      ?c1 "follow"@[] ?c2
   };`,
	`SELECT ?c0, ?c1, ?c2, ?c3
   FROM ?%s
   WHERE {
      /tn<0> "follow"@[] ?c0 .
      ?c0 "follow"@[] ?c1 .
      ?c1 "follow"@[] ?c2 .
      ?c2 "follow"@[] ?c3
   };`,
	`SELECT ?c0, ?c1, ?c2, ?c3, ?c4
   FROM ?%s
   WHERE {
      /tn<0> "follow"@[] ?c0 .
      ?c0 "follow"@[] ?c1 .
      ?c1 "follow"@[] ?c2 .
      ?c2 "follow"@[] ?c3 .
      ?c3 "follow"@[] ?c4
   };`,
	`SELECT ?c0
   FROM ?%s
   WHERE {
      /tn<0> "follow"@[] ?c0
   }
   ORDER BY ?c0 DESC;`,
	`SELECT ?c0, ?c1
   FROM ?%s
   WHERE {
      /tn<0> "follow"@[] ?c0 .
      ?c0 "follow"@[] ?c1
   }
   ORDER BY ?c0 DESC;`,
	`SELECT ?c0, ?c1, ?c2
   FROM ?%s
   WHERE {
      /tn<0> "follow"@[] ?c0 .
      ?c0 "follow"@[] ?c1 .
      ?c1 "follow"@[] ?c2
   }
   ORDER BY ?c0 DESC;`,
	`SELECT ?c0, ?c1, ?c2, ?c3
   FROM ?%s
   WHERE {
      /tn<0> "follow"@[] ?c0 .
      ?c0 "follow"@[] ?c1 .
      ?c1 "follow"@[] ?c2 .
      ?c2 "follow"@[] ?c3
   }
   ORDER BY ?c0 DESC;`,
	`SELECT ?c0, ?c1, ?c2, ?c3, ?c4
   FROM ?%s
   WHERE {
      /tn<0> "follow"@[] ?c0 .
      ?c0 "follow"@[] ?c1 .
      ?c1 "follow"@[] ?c2 .
      ?c2 "follow"@[] ?c3 .
      ?c3 "follow"@[] ?c4
   }
   ORDER BY ?c0 DESC;`,
	`SELECT ?c0
   FROM ?%s
   WHERE {
      /tn<0> "follow"@[] ?c0
   }
   GROUP BY ?c0;`,
	`SELECT ?c0, ?c1
   FROM ?%s
   WHERE {
      /tn<0> "follow"@[] ?c0 .
      ?c0 "follow"@[] ?c1
   }
   GROUP BY ?c0, ?c1;`,
	`SELECT ?c0, ?c1, ?c2
   FROM ?%s
   WHERE {
      /tn<0> "follow"@[] ?c0 .
      ?c0 "follow"@[] ?c1 .
      ?c1 "follow"@[] ?c2
   }
   GROUP BY ?c0, ?c1, ?c2;`,
	`SELECT ?c0, ?c1, ?c2, ?c3
   FROM ?%s
   WHERE {
      /tn<0> "follow"@[] ?c0 .
      ?c0 "follow"@[] ?c1 .
      ?c1 "follow"@[] ?c2 .
      ?c2 "follow"@[] ?c3
   }
   GROUP BY ?c0, ?c1, ?c2, ?c3;`,
	`SELECT ?c0, ?c1, ?c2, ?c3, ?c4
   FROM ?%s
   WHERE {
      /tn<0> "follow"@[] ?c0 .
      ?c0 "follow"@[] ?c1 .
      ?c1 "follow"@[] ?c2 .
      ?c2 "follow"@[] ?c3 .
      ?c3 "follow"@[] ?c4
   }
   GROUP BY ?c0, ?c1, ?c2, ?c3, ?c4;`,
}

// BQLTreeGraphWalking creates the benchmark.
func BQLTreeGraphWalking(ctx context.Context, st storage.Store, chanSize int) ([]*runtime.BenchEntry, error) {
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
			ids = append(ids, fmt.Sprintf("bql tg branch_factor=%04d, size=%07d", bFactors[idx], s))
			gids = append(gids, fmt.Sprintf("bql_b%d_s%d", bFactors[idx], s))
			gSizes = append(gSizes, s)
		}
	}
	var bes []*runtime.BenchEntry
	reps := []int{10}
	for bqlIdx, bqlQuery := range treeGraphWalkingBQL {
		bql := bqlQuery
		for i, max := 0, len(ids); i < max; i++ {
			for idxReps, r := range reps {
				var g storage.Graph
				gID := fmt.Sprintf("bql_tg_%d_%s_r%d_i%d", bqlIdx, gids[i], i, idxReps)
				data := trplSets[i]
				bes = append(bes, &runtime.BenchEntry{
					BatteryID: fmt.Sprintf("Run BQL tree graph walking query %d", bqlIdx),
					ID:        fmt.Sprintf("%s, reps=%02d", ids[i], r),
					Triples:   gSizes[i],
					Reps:      r,
					Setup: func() error {
						var err error
						g, err = st.NewGraph(ctx, "?"+gID)
						if err != nil {
							return err
						}
						return g.AddTriples(ctx, data)
					},
					F: func() error {
						query := fmt.Sprintf(bql, gID)
						_, err := run.RunBQL(ctx, query, st, chanSize)
						return err
					},
					TearDown: func() error {
						return st.DeleteGraph(ctx, "?"+gID)
					},
				})
			}
		}
	}
	return bes, nil
}

// BQLRandomGraphWalking creates the benchmark.
func BQLRandomGraphWalking(ctx context.Context, st storage.Store, chanSize int) ([]*runtime.BenchEntry, error) {
	rgSize := []int{1000, 10000}
	sizes := []int{10, 1000, 100000}
	var trplSets [][]*triple.Triple
	var ids []string
	var gids []string
	var gSizes []int
	gs, err := getGraphGenerators(rgSize)
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
			ids = append(ids, fmt.Sprintf("bql rg branch_factor=%04d, size=%07d", rgSize[idx], s))
			gids = append(gids, fmt.Sprintf("bql_b%d_s%d", rgSize[idx], s))
			gSizes = append(gSizes, s)
		}
	}
	var bes []*runtime.BenchEntry
	reps := []int{10}
	for bqlIdx, bqlQuery := range treeGraphWalkingBQL {
		bql := bqlQuery
		for i, max := 0, len(ids); i < max; i++ {
			for idxReps, r := range reps {
				var g storage.Graph
				gID := fmt.Sprintf("bql_rg_%d_%s_r%d_i%d", bqlIdx, gids[i], i, idxReps)
				data := trplSets[i]
				bes = append(bes, &runtime.BenchEntry{
					BatteryID: fmt.Sprintf("Run BQL random graph walking query %d", bqlIdx),
					ID:        fmt.Sprintf("%s, reps=%02d", ids[i], r),
					Triples:   gSizes[i],
					Reps:      r,
					Setup: func() error {
						var err error
						g, err = st.NewGraph(ctx, "?"+gID)
						if err != nil {
							return err
						}
						return g.AddTriples(ctx, data)
					},
					F: func() error {
						query := fmt.Sprintf(bql, gID)
						_, err := run.RunBQL(ctx, query, st, chanSize)
						return err
					},
					TearDown: func() error {
						return st.DeleteGraph(ctx, "?"+gID)
					},
				})
			}
		}
	}
	return bes, nil
}
