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

// Package common contains share functionality for the command line tool
// commands.
package common

import (
	"fmt"
	"os"
	"sort"
	"strconv"
	"strings"

	"golang.org/x/net/context"

	"github.com/google/badwolf/storage"
	"github.com/google/badwolf/tools/vcli/bw/assert"
	"github.com/google/badwolf/tools/vcli/bw/command"
	"github.com/google/badwolf/tools/vcli/bw/run"
	"github.com/google/badwolf/tools/vcli/bw/version"
	"github.com/google/badwolf/triple/literal"
)

// ParseChannelSizeFlag attempts to parse the "channel_size" flag.
func ParseChannelSizeFlag(flag string) (int, error) {
	ss := strings.Split(flag, "=")
	if len(ss) != 2 {
		return 0, fmt.Errorf("Failed split flag %s", flag)
	}
	if ss[0] != "--channel_size" {
		return 0, fmt.Errorf("Unknown flag %s", flag)
	}
	return strconv.Atoi(ss[1])
}

// Help prints the requested help
func Help(args []string, cmds []*command.Command) int {
	var (
		cmd string
	)
	if len(args) >= 3 {
		cmd = args[2]
	}
	// Prints the help if the command exist.
	for _, c := range cmds {
		if c.Name() == cmd {
			return c.Usage()
		}
	}
	if cmd == "" {
		fmt.Fprintf(os.Stderr, "missing help command. Usage:\n\n\t$ bw help [command]\n\nAvailable help commands\n\n")
		var usage []string
		for _, c := range cmds {
			usage = append(usage, fmt.Sprintf("\t%s\t- %s\n", c.Name(), c.Short))
		}
		sort.Strings(usage)
		for _, u := range usage {
			fmt.Fprint(os.Stderr, u)
		}
		fmt.Fprintln(os.Stderr, "")
		return 0
	}
	fmt.Fprintf(os.Stderr, "help command %q not recognized. Usage:\n\n\t$ bw help\n\n", cmd)
	return 2
}

// StoreGenerator is a function that generate a new valid storage.Store.
type StoreGenerator func() (storage.Store, error)

// InitializeDriver attemps to initalize the driver.
func InitializeDriver(driverName string, drivers map[string]StoreGenerator) (storage.Store, error) {
	f, ok := drivers[driverName]
	if !ok {
		var ds []string
		for k := range drivers {
			ds = append(ds, k)
		}
		return nil, fmt.Errorf("unkown driver name %q; valid drivers [%q]", driverName, strings.Join(ds, ", "))
	}
	return f()
}

// InitializeCommands intializes the avaialbe commands with the given storage
// instance.
func InitializeCommands(driver storage.Store, chanSize int) []*command.Command {
	return []*command.Command{
		assert.New(driver, literal.DefaultBuilder(), chanSize),
		run.New(driver, chanSize),
		version.New(),
	}
}

// Eval of the command line version tool. This allows injecting multiple
// drivers.
func Eval(ctx context.Context, args []string, cmds []*command.Command) int {
	// Retrieve the provided command.
	cmd := ""
	if len(args) >= 2 {
		cmd = args[1]
	}
	// Check for help request.
	if cmd == "help" {
		return Help(args, cmds)
	}
	// Run the requested command.
	for _, c := range cmds {
		if c.Name() == cmd {
			return c.Run(ctx, args)
		}
	}
	// The command was not found.
	if cmd == "" {
		fmt.Fprintf(os.Stderr, "missing command. Usage:\n\n\t$ bw [command]\n\nPlease run\n\n\t$ bw help\n\n")
	} else {
		fmt.Fprintf(os.Stderr, "command %q not recognized. Usage:\n\n\t$ bw [command]\n\nPlease run\n\n\t$ bw help\n\n", cmd)
	}
	return 1
}

// Run executes the main of the command line tool.
func Run(driverName string, drivers map[string]StoreGenerator, chanSize int) int {
	driver, err := InitializeDriver(driverName, drivers)
	if err != nil {
		fmt.Fprintln(os.Stderr, err)
		return 2
	}
	return Eval(context.Background(), os.Args, InitializeCommands(driver, chanSize))
}
