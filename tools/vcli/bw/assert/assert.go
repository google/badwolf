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

// Package assert implements the command allowing to run the stories in a folder
// and collect the outcome.
package assert

import (
	"fmt"
	"log"
	"os"
	"path"
	"strings"

	"golang.org/x/net/context"

	"github.com/google/badwolf/storage"
	"github.com/google/badwolf/tools/compliance"
	"github.com/google/badwolf/tools/vcli/bw/command"
	"github.com/google/badwolf/tools/vcli/bw/io"
	"github.com/google/badwolf/triple/literal"
)

// New creates the help command.
func New(store storage.Store, builder literal.Builder, chanSize, bulkSize int) *command.Command {
	cmd := &command.Command{
		UsageLine: "assert folder_path",
		Short:     "asserts all the stories in the indicated folder.",
		Long: `Asserts all the stories in the folder. Each story is stored in a JSON
file containing all the sources and all the assertions to run.
`,
	}
	cmd.Run = func(ctx context.Context, args []string) int {
		return assertCommand(ctx, cmd, args, store, builder, chanSize, bulkSize)
	}
	return cmd
}

// assertCommand runs all the BQL statements available in the file.
func assertCommand(ctx context.Context, cmd *command.Command, args []string, store storage.Store, builder literal.Builder, chanSize, bulkSize int) int {
	if len(args) < 3 {
		log.Printf("Missing required folder path. ")
		cmd.Usage()
		return 2
	}
	// Open the folder.
	folder := strings.TrimSpace(args[len(args)-1])
	f, err := os.Open(folder)
	if err != nil {
		log.Printf("[ERROR] Failed to open folder %s\n\n\t%v\n\n", folder, err)
		return 2
	}
	fis, err := f.Readdir(0)
	if err != nil {
		log.Printf("[ERROR] Failed to read folder %s\n\n\t%v\n\n", folder, err)
		return 2
	}
	fmt.Println("-------------------------------------------------------------")
	fmt.Printf("Processing folder %q...\n", folder)
	var stories []*compliance.Story
	empty := true
	for _, fi := range fis {
		if !strings.Contains(fi.Name(), "json") {
			continue
		}
		fmt.Printf("\tProcessing file %q... ", fi.Name())
		lns, err := io.ReadLines(path.Join(folder, fi.Name()))
		if err != nil {
			log.Printf("\n\n\tFailed to read file content with error %v\n\n", err)
			return 2
		}
		rawStory := strings.Join(lns, "\n")
		s := &compliance.Story{}
		if err := s.Unmarshal(rawStory); err != nil {
			log.Printf("\n\n\tFailed to unmarshal story with error %v\n\n", err)
			return 2
		}
		empty = false
		stories = append(stories, s)
		fmt.Println("done.")
	}
	if empty {
		fmt.Println("No stories found!")
		fmt.Println("-------------------------------------------------------------")
		return 2
	}
	fmt.Println("-------------------------------------------------------------")
	fmt.Printf("Evaluating %d stories... ", len(stories))
	results := compliance.RunStories(ctx, store, builder, stories, chanSize, bulkSize)
	fmt.Println("done.")
	fmt.Println("-------------------------------------------------------------")
	for i, entry := range results.Entries {
		fmt.Printf("(%d/%d) Story %q...\n", i+1, len(stories), entry.Story.Name)
		if entry.Err != nil {
			log.Printf("\tFailed to run story %q with error %v\n\n", entry.Story.Name, entry.Err)
			return 2
		}
		for aid, aido := range entry.Outcome {
			if aido.Equal {
				fmt.Printf("\t%s [Assertion=TRUE]\n", aid)
			} else {
				fmt.Printf("\t%s [Assertion=FALSE]\n\nGot:\n\n%s\nWant:\n\n%s\n", aid, aido.Got, aido.Want)
			}
		}
		fmt.Println()
	}
	fmt.Println("-------------------------------------------------------------")
	fmt.Println("\ndone")
	return 0
}
