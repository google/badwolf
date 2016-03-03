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

// Package version contains the implementation of the command that prints the
// BQL version.
package version

import (
	"fmt"
	"os"

	"golang.org/x/net/context"

	"github.com/google/badwolf/bql/version"
	"github.com/google/badwolf/tools/vcli/bw/command"
)

// New create the version command.
func New() *command.Command {
	return &command.Command{
		Run: func(ctx context.Context, args []string) int {
			fmt.Fprintf(os.Stderr, "badwolf vCli (%d.%d.%d-%s)\n", version.Major, version.Minor, version.Patch, version.Release)
			return 0
		},
		UsageLine: "version",
		Short:     "prints the current version.",
		Long:      "Prints the current version of the BadWolf command line tool.",
	}
}
