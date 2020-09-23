// Copyright 2020 Google Inc. All rights reserved.
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

// Package testutil implements utility functions used in testing.
package testutil

import (
	"strings"
	"testing"
	"time"

	"github.com/google/badwolf/triple/literal"
	"github.com/google/badwolf/triple/node"
	"github.com/google/badwolf/triple/predicate"
)

// MustBuildLiteral builds a Literal out of textLiteral or makes the given test to fail.
func MustBuildLiteral(t *testing.T, textLiteral string) *literal.Literal {
	t.Helper()
	lit, err := literal.DefaultBuilder().Parse(textLiteral)
	if err != nil {
		t.Fatalf("could not parse text literal %q, got error: %v", textLiteral, err)
	}
	return lit
}

// MustBuildNodeFromStrings builds a Node out of nodeType and nodeID or makes the given test to fail.
func MustBuildNodeFromStrings(t *testing.T, nodeType, nodeID string) *node.Node {
	t.Helper()
	n, err := node.NewNodeFromStrings(nodeType, nodeID)
	if err != nil {
		t.Fatalf("could not build node from type %q and ID %q, got error: %v", nodeType, nodeID, err)
	}
	return n
}

// MustBuildTime builds a Time out of timeLiteral or makes the given test to fail.
func MustBuildTime(t *testing.T, timeLiteral string) *time.Time {
	t.Helper()
	time, err := time.Parse(time.RFC3339Nano, strings.TrimSpace(timeLiteral))
	if err != nil {
		t.Fatalf("could not parse time literal %q, got error: %v", timeLiteral, err)
	}
	return &time
}

// MustBuildPredicate builds a Predicate out of predicateLiteral or makes the given test to fail.
func MustBuildPredicate(t *testing.T, predicateLiteral string) *predicate.Predicate {
	t.Helper()
	p, err := predicate.Parse(predicateLiteral)
	if err != nil {
		t.Fatalf("could not parse predicate literal %q, got error: %v", predicateLiteral, err)
	}
	return p
}
