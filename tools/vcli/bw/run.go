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

// The run command allows to run a sequence of BQL commands from the provided
// file.

package main

import (
	"bufio"
	"fmt"
	"os"
	"strings"

	"github.com/google/badwolf/bql/grammar"
	"github.com/google/badwolf/bql/planner"
	"github.com/google/badwolf/bql/semantic"
	"github.com/google/badwolf/bql/table"
	"github.com/google/badwolf/storage"
	"github.com/google/badwolf/storage/memory"
)

// NewRunCommand creates the help command.
func NewRunCommand() *Command {
	cmd := &Command{
		UsageLine: "run file_path",
		Short:     "runs BQL statements.",
		Long: `Runs all the commands listed in the provided file. Lines in the
the file starting with # will be ignored. All statements will be run
sequentially.
`,
	}
	cmd.Run = func(args []string) int {
		return runCommand(cmd, args)
	}
	return cmd
}

// runCommand runs all the BQL statements available in the file.
func runCommand(cmd *Command, args []string) int {
	if len(args) < 3 {
		fmt.Fprintf(os.Stderr, "Missing required file path. ")
		cmd.Usage()
		return 2
	}
	lines, err := getStatementsFromFile(args[2])
	if err != nil {
		fmt.Fprintf(os.Stderr, "Failed to read file %s\n\n\t%v\n\n", args[2], err)
		return 2
	}
	fmt.Printf("Processing file %s\n\n", args[2])
	s := memory.DefaultStore
	for idx, stm := range lines {
		fmt.Printf("Processing statement (%d/%d):\n%s\n\n", idx+1, len(lines), stm)
		tbl, err := runBQL(stm, s)
		if err != nil {
			fmt.Printf("[FAIL] %v\n\n", err)
			continue
		}
		fmt.Println("Result:")
		if tbl.NumRows() > 0 {
			fmt.Println(tbl)
		}
		fmt.Printf("OK\n\n")
	}
	return 0
}

// runBQL attemps to excecute the provided query against the given store.
func runBQL(bql string, s storage.Store) (*table.Table, error) {
	p, err := grammar.NewParser(grammar.SemanticBQL())
	if err != nil {
		return nil, fmt.Errorf("Failed to initilize a valid BQL parser")
	}
	stm := &semantic.Statement{}
	if err := p.Parse(grammar.NewLLk(bql, 1), stm); err != nil {
		return nil, fmt.Errorf("Failed to parse BQL statement with error %v", err)
	}
	pln, err := planner.New(s, stm)
	if err != nil {
		return nil, fmt.Errorf("Should have not failed to create a plan using memory.DefaultStorage for statement %v with error %v", stm, err)
	}
	res, err := pln.Excecute()
	if err != nil {
		return nil, fmt.Errorf("planner.Execute: failed to execute insert plan with error %v", err)
	}
	return res, nil
}

// getStatementsFromFile returns the statements found in the provided file.
func getStatementsFromFile(path string) ([]string, error) {
	stms, err := readLines(path)
	if err != nil {
		return nil, err
	}
	return stms, nil
}

// readLines from a file into a string array.
func readLines(path string) ([]string, error) {
	f, err := os.Open(path)
	if err != nil {
		return nil, err
	}
	defer f.Close()

	var lines []string
	scanner := bufio.NewScanner(f)
	line := ""
	for scanner.Scan() {
		l := strings.TrimSpace(scanner.Text())
		if len(l) == 0 || strings.Index(l, "#") == 0 {
			continue
		}
		line += " " + l
		if l[len(l)-1:] == ";" {
			lines = append(lines, strings.TrimSpace(line))
			line = ""
		}
	}
	if line != "" {
		lines = append(lines, strings.TrimSpace(line))
	}
	return lines, scanner.Err()
}
