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

// Package io contains helper functions for io operatoins on the command line
// bw tool.
package io

import (
	"bufio"
	"os"
	"strings"
)

// ReadLines from a file into a string array.
func ReadLines(path string) ([]string, error) {
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
