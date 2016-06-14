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
// This file is a template for creating your own bw tool with custom backend
// storage drivers. Just *copy* this file to your project, add the required
// flags that you need to initialize your driver and register your diver on
// the registeredDriver map in the registerDrivers function.
package main

import (
	"flag"
	"os"

	"github.com/google/badwolf/storage"
	"github.com/google/badwolf/storage/memory"
	"github.com/google/badwolf/tools/vcli/bw/common"
)

var (
	// drivers contains the registered drivers available for this command line tool.
	registeredDrivers map[string]common.StoreGenerator
	// Available flags.
	driver               = flag.String("driver", "VOLATILE", "The storage driver to use {VOLATILE}.")
	bqlChannelSize       = flag.Int("bql_channel_size", 0, "Internal channel size to use on BQL queries.")
	bulkTripleOpSize     = flag.Int("bulk_triple_op_size", 1000, "Number of triples to use in bulk load operations.")
	bulkTripleBuilderSize = flag.Int("bulk_triple_builder_size_in_bytes", 1000, "Maximum size of literals when parsing a triple.")
	// Add your driver flags below.
)

// Registers the available drivers.
func registerDrivers() {
	registeredDrivers = map[string]common.StoreGenerator{
		// Memory only storage driver.
		"VOLATILE": func() (storage.Store, error) {
			return memory.NewStore(), nil
		},
	}
}

func main() {
	flag.Parse()
	registerDrivers()
	os.Exit(common.Run(*driver, registeredDrivers, *bqlChannelSize, *bulkTripleOpSize, *bulkTripleBuilderSize))
}
