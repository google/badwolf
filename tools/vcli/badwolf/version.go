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

package main

import (
	"fmt"
	"os"
)

var stage = "alpha"
var major = 0
var minor = 1
var patch = "dev"

// NewVersionCommand create the help command.
func NewVersionCommand() *Command {
	return &Command{
		Run: func(args []string) int {
			fmt.Fprintf(os.Stderr, "badwolf vCli (%s-%d.%d.%s)\n", stage, major, minor, patch)
			return 0
		},
		UsageLine: "version",
		Short:     "prints the current version.",
		Long:      "Prints the current version of the BadWolf command line tool.",
	}
}
