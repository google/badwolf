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

// BadWolf command line tools allows you to interact with graphs via BQL.

package main

import (
	"fmt"
	"os"

	"golang.org/x/net/context"
)

// Registration of the available commands. Please keep sorted.
var cmds = []*Command{
	NewAssertCommand(),
	NewRunCommand(),
	NewVersionCommand(),
}

func main() {
	ctx := context.Background()
	args := os.Args
	// Retrieve the provided command.
	cmd := ""
	if len(args) >= 2 {
		cmd = args[1]
	}
	// Check for help request.
	if cmd == "help" {
		os.Exit(help(args))
	}
	// Run the requested command.
	for _, c := range cmds {
		if c.Name() == cmd {
			os.Exit(c.Run(ctx, args))
		}
	}
	// The command was not found.
	if cmd == "" {
		fmt.Fprintf(os.Stderr, "missing command. Usage:\n\n\t$ badwolf [command]\n\nPlease run\n\n\t$ badwolf help\n\n")
	} else {
		fmt.Fprintf(os.Stderr, "command %q not recognized. Usage:\n\n\t$ badwolf [command]\n\nPlease run\n\n\t$ badwolf help\n\n", cmd)
	}
	os.Exit(1)
}

// help prints the requested hy
func help(args []string) int {
	var (
		cmd string
	)
	if len(args) >= 3 {
		cmd = args[2]
	}
	// Prints the help if hhe command exist.
	for _, c := range cmds {
		if c.Name() == cmd {
			return c.Usage()
		}
	}
	if cmd == "" {
		fmt.Fprintf(os.Stderr, "missing help command. Usage:\n\n\t$ badwolf help [command]\n\nAvailable help commands\n\n")
		for _, c := range cmds {
			fmt.Fprintf(os.Stderr, "\t%s\t- %s\n", c.Name(), c.Short)
		}
		fmt.Fprintln(os.Stderr, "")
		return 0
	}
	fmt.Fprintf(os.Stderr, "help command %q not recognized. Usage:\n\n\t$ badwolf help\n\n", cmd)
	return 2
}
