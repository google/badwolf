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
	"fmt"
	"os"
)

// NewRunCommand create the help command.
func NewRunCommand() *Command {
	cmd := &Command{
		UsageLine: "run file_path",
		Short:     "runs BQL statements.",
		Long: `Runs all the commands listed in the provided file. Lines in the
the file starting with # will be ignored. All statements will be run
sequentially.`,
	}
	cmd.Run = func(args []string) int {
		return runCommand(cmd, args)
	}
	return cmd
}

// runCommand runs all the BQL statements available in the file.
func runCommand(cmd *Command, args []string) int {
	if len(args) <= 3 {
		fmt.Fprintf(os.Stderr, "Missing required file path. ")
		cmd.Usage()
		return 2
	}
	for _, stm := range getStatementsFromFile(args[2]) {
		fmt.Printf("Processing statement:\n%s\n", stm)
		// TODO(xllora): Implement the BQL execution.
	}
	return 0
}

// getStatementsFromFile returns the statements found in the provided file.
func getStatementsFromFile(path string) []string {
	var stms []string
	// TODO(xllora): Extract statements from file.
	return stms
}
