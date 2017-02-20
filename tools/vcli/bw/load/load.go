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

// Package load contains the command allowing to run a sequence of BQL commands
// from the provided file.
package load

import (
	"fmt"
	"log"
	"strings"

	"golang.org/x/net/context"

	"github.com/google/badwolf/storage"
	"github.com/google/badwolf/tools/vcli/bw/command"
	"github.com/google/badwolf/tools/vcli/bw/io"
	"github.com/google/badwolf/triple"
	"github.com/google/badwolf/triple/literal"
)

// New creates the help command.
func New(store storage.Store, bulkSize, builderSize int) *command.Command {
	cmd := &command.Command{
		UsageLine: "load <file_path> <graph_names_separated_by_commas>",
		Short:     "load triples in bulk stored in a file.",
		Long: `Loads all the triples stored in a file into the provided graphs.
Graph names need to be separated by commands with no whitespaces. Each triple
needs to placed in a single line. Each triple needs to be formated so it can be
parsed as indicated in the documetation (see https://github.com/google/badwolf).
All data in the file will be treated as triples. A line starting with # will
be treated as a commented line. If the load fails you may end up with partially
loaded data.
`,
	}
	cmd.Run = func(ctx context.Context, args []string) int {
		return Eval(ctx, cmd.UsageLine+"\n\n"+cmd.Long, args, store, bulkSize, builderSize)
	}
	return cmd
}

// Eval loads the triples in the file against as indicated by the command.
func Eval(ctx context.Context, usage string, args []string, store storage.Store, bulkSize, builderSize int) int {
	if len(args) <= 3 {
		log.Printf("[ERROR] Missing required file path and/or graph names.\n\n%s", usage)
		return 2
	}
	graphs, lb := strings.Split(args[len(args)-1], ","), literal.NewBoundedBuilder(builderSize)
	trplsChan, errChan, doneChan := make(chan *triple.Triple), make(chan error), make(chan bool)
	path := args[len(args)-2]
	go storeTriple(ctx, store, graphs, bulkSize, trplsChan, errChan, doneChan)
	cnt, err := io.ProcessLines(path, func(line string) error {
		t, err := triple.Parse(line, lb)
		if err != nil {
			return err
		}
		trplsChan <- t
		return <-errChan
	})
	if err != nil {
		log.Printf("[ERROR] Failed to process file %q. Ivalid triple on line %d. %v\n", path, cnt, err)
		return 2
	}
	doneChan <- true
	if err := <-errChan; err != nil {
		log.Printf("[ERROR] Failed to process file %q. Ivalid triple on line %d. %v\n", path, cnt, err)
		return 2
	}
	fmt.Printf("Successfully processed %d lines from file %q.\nTriples loaded into graphs:\n\t- %s\n", cnt, path, strings.Join(graphs, "\n\t- "))
	return 0
}

var workingTrpls []*triple.Triple

func flush(ctx context.Context, graphs []string, store storage.Store) error {
	defer func() {
		workingTrpls = nil
	}()
	if len(workingTrpls) > 0 {
		for _, graph := range graphs {
			g, err := store.Graph(ctx, graph)
			if err != nil {
				return err
			}
			if err := g.AddTriples(ctx, workingTrpls); err != nil {
				return err
			}
		}
	}
	return nil
}

func storeTriple(ctx context.Context, store storage.Store, graphs []string, bulkSize int, trplChan <-chan *triple.Triple, errChan chan<- error, doneChan <-chan bool) {
	for {
		select {
		case trpl := <-trplChan:
			if len(workingTrpls) < bulkSize {
				workingTrpls = append(workingTrpls, trpl)
				errChan <- nil
			} else {
				err := flush(ctx, graphs, store)
				workingTrpls = append(workingTrpls, trpl)
				errChan <- err
			}
		case <-doneChan:
			err := flush(ctx, graphs, store)
			errChan <- err
			return
		}
	}
}
