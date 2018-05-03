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

// Package export contains the command allowing to dump all triples of a graphs
// into a file.
package export

import (
	"context"
	"fmt"
	"log"
	"os"
	"strings"
	"sync"

	"github.com/google/badwolf/storage"
	"github.com/google/badwolf/tools/vcli/bw/command"
	"github.com/google/badwolf/triple"
)

// New creates the help command.
func New(store storage.Store, bulkSize int) *command.Command {
	cmd := &command.Command{
		UsageLine: "export <graph_names_separated_by_commas> <file_path>",
		Short:     "export triples in bulk from graphs into a file.",
		Long: `Export all the triples in the provided graphs into the provided
text file.`,
	}
	cmd.Run = func(ctx context.Context, args []string) int {
		return Eval(ctx, cmd.UsageLine+"\n\n"+cmd.Long, args, store, bulkSize)
	}
	return cmd
}

// Eval loads the triples in the file against as indicated by the command.
func Eval(ctx context.Context, usage string, args []string, store storage.Store, bulkSize int) int {
	if len(args) < 3 {
		log.Printf("[ERROR] Missing required file path and/or graph names.\n\n%s", usage)
		return 2
	}
	graphs, path := strings.Split(args[len(args)-2], ","), args[len(args)-1]
	f, err := os.Create(path)
	if err != nil {
		log.Printf("[ERROR] Failed to open target file %q with error %v.\n\n", path, err)
		return 2
	}
	defer f.Close()
	var sgs []storage.Graph
	for _, gr := range graphs {
		g, err := store.Graph(ctx, gr)
		if err != nil {
			log.Printf("[ERROR] Failed to retrieve graph %q with error %v.\n\n", gr, err)
			return 2
		}
		sgs = append(sgs, g)
	}

	cnt := 0
	var errs []error
	var mu sync.Mutex
	chn := make(chan *triple.Triple, bulkSize)
	for _, vg := range sgs {
		go func(g storage.Graph) {
			err := g.Triples(ctx, storage.DefaultLookup, chn)
			mu.Lock()
			errs = append(errs, err)
			mu.Unlock()
		}(vg)
	}

	for t := range chn {
		if _, err := f.WriteString(t.String() + "\n"); err != nil {
			log.Printf("[ERROR] Failed to write triple %s to file %q, %v.\n\n", t.String(), path, err)
			return 2
		}
		cnt++
	}
	for _, err := range errs {
		if err != nil {
			log.Printf("[ERROR] Failed to retrieve triples with error %v.\n\n", err)
			return 2
		}
	}

	fmt.Printf("Successfully written %d triples to file %q.\nTriples exported from graphs:\n\t- %s\n", cnt, path, strings.Join(graphs, "\n\t- "))
	return 0
}
