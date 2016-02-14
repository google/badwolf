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

// The assert command allows to run the stories in a folder and collect the
// outcome.
package main

import (
	"fmt"
	"os"
	"path"
	"strings"

	"golang.org/x/net/context"

	"github.com/google/badwolf/storage/memory"
	"github.com/google/badwolf/tools/compliance"
	"github.com/google/badwolf/triple/literal"
)

// NewAssertCommand create the help command.
func NewAssertCommand() *Command {
	cmd := &Command{
		UsageLine: "assert folder_path",
		Short:     "asserts all the stories in the indicated folder.",
		Long: `Asserts all the stories in the folder. Each story is stored in a JSON
file containing all the sources and all the assertions to run.
`,
	}
	cmd.Run = func(ctx context.Context, args []string) int {
		return assertCommand(ctx, cmd, args)
	}
	return cmd
}

// assertCommand runs all the BQL statements available in the file.
func assertCommand(ctx context.Context, cmd *Command, args []string) int {
	if len(args) < 3 {
		fmt.Fprintf(os.Stderr, "Missing required folder path. ")
		cmd.Usage()
		return 2
	}
	// Open the folder.
	folder := strings.TrimSpace(args[2])
	f, err := os.Open(folder)
	if err != nil {
		fmt.Fprintf(os.Stderr, "Failed to open folder %s\n\n\t%v\n\n", folder, err)
		return 2
	}
	fis, err := f.Readdir(0)
	if err != nil {
		fmt.Fprintf(os.Stderr, "Failed to read folder %s\n\n\t%v\n\n", folder, err)
		return 2
	}
	fmt.Printf("Processing folder %q...\n\n", folder)
	for _, fi := range fis {
		if !strings.Contains(fi.Name(), "json") {
			continue
		}
		fmt.Println("-------------------------------------------------------------")
		fmt.Printf("Processing file %q...\n\n", fi.Name())
		lns, err := readLines(path.Join(folder, fi.Name()))
		if err != nil {
			fmt.Fprintf(os.Stderr, "\n\n\tFailed to read file content with error %v\n\n", err)
			return 2
		}
		rawStory := strings.Join(lns, "\n")
		s := &compliance.Story{}
		if err := s.Unmarshal(rawStory); err != nil {
			fmt.Fprintf(os.Stderr, "\n\n\tFailed to unmarshal story with error %v\n\n", err)
			return 2
		}
		m, err := s.Run(ctx, memory.NewStore(), literal.DefaultBuilder())
		if err != nil {
			fmt.Fprintf(os.Stderr, "\n\n\tFailed to run story %q with error %v\n\n", s.Name, err)
			return 2
		}
		for aid, aido := range m {
			if aido.Equal {
				fmt.Printf("%s [TRUE]\n", aid)
			} else {
				fmt.Printf("%s [FALSE]\n\nGot:\n\n%s\nWant:\n\n%s\n", aid, aido.Got, aido.Want)
			}
		}
	}
	fmt.Println("-------------------------------------------------------------")
	fmt.Println("\ndone")
	return 0
}
