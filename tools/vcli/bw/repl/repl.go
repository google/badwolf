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

// Package repl contains the implementation of the command that prints the
// BQL version.
package repl

import (
	"bufio"
	"fmt"
	"os"
	"strings"
	"time"

	"golang.org/x/net/context"

	"github.com/google/badwolf/bql/grammar"
	"github.com/google/badwolf/bql/planner"
	"github.com/google/badwolf/bql/semantic"
	"github.com/google/badwolf/bql/table"
	"github.com/google/badwolf/bql/version"
	"github.com/google/badwolf/storage"
	"github.com/google/badwolf/tools/vcli/bw/command"
	"github.com/google/badwolf/tools/vcli/bw/export"
	"github.com/google/badwolf/tools/vcli/bw/io"
	"github.com/google/badwolf/tools/vcli/bw/load"
)

const prompt = "bql> "

// New create the version command.
func New(driver storage.Store, chanSize, bulkSize, builderSize int, rl ReadLiner) *command.Command {
	return &command.Command{
		Run: func(ctx context.Context, args []string) int {
			REPL(driver, os.Stdin, rl, chanSize, bulkSize, builderSize)
			return 0
		},
		UsageLine: "bql",
		Short:     "starts a REPL to run BQL statements.",
		Long:      "Starts a REPL from the command line to accept BQL statements. Type quit; to leave the REPL.",
	}
}

// ReadLiner returns a channel with the imput to be used for the REPL.
type ReadLiner func(*os.File) <-chan string

// SimpleReadLine reads a line from the provided file. This does not support
// any advanced terminal functionalities.
//
// TODO(xllora): Replace simple reader for function that supports advanced
// terminal input.
func SimpleReadLine(f *os.File) <-chan string {
	c := make(chan string)
	go func() {
		defer close(c)
		scanner := bufio.NewScanner(os.Stdin)
		for scanner.Scan() {
			c <- strings.TrimSpace(scanner.Text())
		}
	}()
	return c
}

// REPL starts a read-evaluation-print-loop to run BQL commands.
func REPL(driver storage.Store, input *os.File, rl ReadLiner, chanSize, bulkSize, builderSize int) int {
	ctx := context.Background()
	fmt.Printf("Welcome to BadWolf vCli (%d.%d.%d-%s)\n", version.Major, version.Minor, version.Patch, version.Release)
	fmt.Printf("Using driver %q. Type quit; to exit\n", driver.Name(ctx))
	fmt.Printf("Session started at %v\n\n", time.Now())
	defer func() {
		fmt.Printf("\n\nThanks for all those BQL queries!\n\n")
	}()
	fmt.Print(prompt)
	l := ""
	for line := range rl(input) {
		nl := strings.TrimSpace(line)
		if nl == "" {
			fmt.Print(prompt)
			continue
		}
		if l != "" {
			l = l + " " + nl
		} else {
			l = nl
		}
		if !strings.HasSuffix(nl, ";") {
			// Not done with the statement.
			continue
		}
		if strings.HasPrefix(l, "quit") {
			break
		}
		if strings.HasPrefix(l, "help") {
			printHelp()
			fmt.Print(prompt)
			l = ""
			continue
		}
		if strings.HasPrefix(l, "export") {
			args := strings.Split("bw "+strings.TrimSpace(l[:len(l)-1]), " ")
			usage := "Wrong syntax\n\n\tload <graph_names_separated_by_commas> <file_path>\n"
			export.Eval(ctx, usage, args, driver, bulkSize)
			fmt.Print(prompt)
			l = ""
			continue
		}
		if strings.HasPrefix(l, "load") {
			args := strings.Split("bw "+strings.TrimSpace(l[:len(l)-1]), " ")
			usage := "Wrong syntax\n\n\tload <file_path> <graph_names_separated_by_commas>\n"
			load.Eval(ctx, usage, args, driver, bulkSize, builderSize)
			fmt.Print(prompt)
			l = ""
			continue
		}
		if strings.HasPrefix(l, "desc") {
			pln, err := planBQL(ctx, l[4:], driver, chanSize)
			if err != nil {
				fmt.Printf("[ERROR] %s\n\n", err)
			} else {
				fmt.Println(pln.String())
				fmt.Println("[OK]")
			}
			fmt.Print(prompt)
			l = ""
			continue
		}
		if strings.HasPrefix(l, "run") {
			path, cmds, err := runBQLFromFile(ctx, driver, chanSize, strings.TrimSpace(l[:len(l)-1]))
			if err != nil {
				fmt.Printf("[ERROR] %s\n\n", err)
			} else {
				fmt.Printf("Loaded %q and run %d BQL commands successfully\n\n", path, cmds)
			}
			fmt.Print(prompt)
			l = ""
			continue
		}

		table, err := runBQL(ctx, l, driver, chanSize)
		l = ""
		if err != nil {
			fmt.Printf("[ERROR] %s\n\n", err)
		} else {
			if len(table.Bindings()) > 0 {
				fmt.Println(table.String())
			}
			fmt.Println("[OK]")
		}
		fmt.Print(prompt)
	}
	return 0
}

// printHelp prints help for the console commands.
func printHelp() {
	fmt.Println("help                                                  - prints help for the bw console.")
	fmt.Println("export <graph_names_separated_by_commas> <file_path>  - dumps triples from graphs into a file path.")
	fmt.Println("desc <BQL>                                            - prints the execution plan for a BQL statement.")
	fmt.Println("load <file_path> <graph_names_separated_by_commas>    - load triples into the specified graphs.")
	fmt.Println("run <file_with_bql_statements>                        - runs all the BQL statements in the file.")
	fmt.Println("quit                                                  - quits the console.")
	fmt.Println()
}

// runBQLFromFile loads all the statements in the file and runs them.
func runBQLFromFile(ctx context.Context, driver storage.Store, chanSize int, line string) (string, int, error) {
	ss := strings.Split(strings.TrimSpace(line), " ")
	if len(ss) != 2 {
		return "", 0, fmt.Errorf("wrong syntax: run <file_with_bql_statements>")
	}
	path := ss[1]
	lines, err := io.GetStatementsFromFile(path)
	if err != nil {
		return "", 0, fmt.Errorf("failed to read file %q with error %v on\n", path, err)
	}
	for idx, stm := range lines {
		fmt.Printf("Processing statement (%d/%d)\n", idx+1, len(lines))
		_, err := runBQL(ctx, stm, driver, chanSize)
		if err != nil {
			return "", 0, fmt.Errorf("%v on\n%s\n", err, stm)
		}
	}
	fmt.Println()
	return path, len(lines), nil
}

// runBQL attempts to execute the provided query against the given store.
func runBQL(ctx context.Context, bql string, s storage.Store, chanSize int) (*table.Table, error) {
	pln, err := planBQL(ctx, bql, s, chanSize)
	if err != nil {
		return nil, err
	}
	res, err := pln.Execute(ctx)
	if err != nil {
		return nil, fmt.Errorf("planner.Execute: failed to execute insert plan with error %v", err)
	}
	return res, nil
}

// planBQL attempts to create the excecution plan for the provided query against the given store.
func planBQL(ctx context.Context, bql string, s storage.Store, chanSize int) (planner.Executor, error) {
	p, err := grammar.NewParser(grammar.SemanticBQL())
	if err != nil {
		return nil, fmt.Errorf("failed to initilize a valid BQL parser")
	}
	stm := &semantic.Statement{}
	if err := p.Parse(grammar.NewLLk(bql, 1), stm); err != nil {
		return nil, fmt.Errorf("failed to parse BQL statement with error %v", err)
	}
	pln, err := planner.New(ctx, s, stm, chanSize)
	if err != nil {
		return nil, fmt.Errorf("should have not failed to create a plan using memory.DefaultStorage for statement %v with error %v", stm, err)
	}
	return pln, nil
}
