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
	"os"

	"github.com/google/badwolf/storage/memory"
	"github.com/google/badwolf/tools/vcli/bw/assert"
	"github.com/google/badwolf/tools/vcli/bw/command"
	"github.com/google/badwolf/tools/vcli/bw/common"
	"github.com/google/badwolf/tools/vcli/bw/run"
	"github.com/google/badwolf/tools/vcli/bw/version"
	"github.com/google/badwolf/triple/literal"

	"golang.org/x/net/context"
)

// Registration of the available commands. Please keep sorted.
var cmds = []*command.Command{
	assert.New(memory.NewStore(), literal.DefaultBuilder()),
	run.New(memory.NewStore()),
	version.New(),
}

func main() {
	ctx, args := context.Background(), os.Args
	os.Exit(common.Eval(ctx, args, cmds))
}
