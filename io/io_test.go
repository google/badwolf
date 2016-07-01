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

package io

import (
	"bytes"
	"fmt"
	"testing"

	"golang.org/x/net/context"

	"github.com/google/badwolf/storage/memory"
	"github.com/google/badwolf/triple"
	"github.com/google/badwolf/triple/literal"
)

func getTestTriples(t *testing.T) []*triple.Triple {
	ts := []*triple.Triple{}
	ss := []string{
		"/u<john>\t\"knows\"@[]\t/u<mary>",
		"/u<john>\t\"knows\"@[]\t/u<peter>",
		"/u<john>\t\"knows\"@[]\t/u<alice>",
		"/u<mary>\t\"knows\"@[]\t/u<andrew>",
		"/u<mary>\t\"knows\"@[]\t/u<kim>",
		"/u<mary>\t\"knows\"@[]\t/u<alice>",
	}
	for _, s := range ss {
		trpl, err := triple.Parse(s, literal.DefaultBuilder())
		if err != nil {
			t.Errorf("triple.Parse failed to parse valid triple %s with error %v", s, err)
			continue
		}
		ts = append(ts, trpl)
	}
	return ts
}

func TestReadIntoGraph(t *testing.T) {
	var buffer bytes.Buffer
	ts := getTestTriples(t)
	for _, trpl := range ts {
		buffer.WriteString(fmt.Sprintf("%s\n", trpl.String()))
	}
}

func TestWriteIntoGraph(t *testing.T) {
	var buffer bytes.Buffer
	ts, ctx := getTestTriples(t), context.Background()
	g, err := memory.NewStore().NewGraph(ctx, "test")
	if err != nil {
		t.Fatalf("memory.NewStore().NewGraph should have never failed to create a graph")
	}
	if err := g.AddTriples(ctx, ts); err != nil {
		t.Errorf("storage.AddTriples should have not fail to add triples %v with error %v", ts, err)
	}
	cnt, err := WriteGraph(ctx, &buffer, g)
	if err != nil {
		t.Errorf("io.WriteGraph failed to read %s with error %v", buffer.String(), err)
	}
	if cnt != 6 {
		t.Errorf("io.WriteGraph should have been able to write 6 triples not %d", cnt)
	}
}

func TestSerializationContents(t *testing.T) {
	var buffer bytes.Buffer
	ts, ctx := getTestTriples(t), context.Background()

	g, err := memory.NewStore().NewGraph(ctx, "test")
	if err != nil {
		t.Fatalf("memory.NewStore().NewGraph should have never failed to create a graph")
	}
	if err := g.AddTriples(ctx, ts); err != nil {
		t.Errorf("storage.AddTriples should have not fail to add triples %v with error %v", ts, err)
	}
	// Serialize to a buffer.
	cnt, err := WriteGraph(ctx, &buffer, g)
	if err != nil {
		t.Errorf("io.WriteGraph failed to read %s with error %v", buffer.String(), err)
	}
	if cnt != 6 {
		t.Errorf("io.WriteGraph should have been able to write 6 triples not %d", cnt)
	}
	// Deserialize from a buffer.
	g2, err := memory.DefaultStore.NewGraph(ctx, "test2")
	if err != nil {
		t.Fatalf("memory.DefaultStore.NewGraph should have never failed to create a graph")
	}
	cnt2, err := ReadIntoGraph(ctx, g2, &buffer, literal.DefaultBuilder())
	if err != nil {
		t.Errorf("io.readIntoGraph failed to read %s with error %v", buffer.String(), err)
	}
	if cnt2 != 6 {
		t.Errorf("io.readIntoGraph should have been able to read 6 triples not %d", cnt2)
	}
	// Check the graphs are equal
	m := make(map[string]bool)
	gs := 0
	gtrpls := make(chan *triple.Triple)
	go func() {
		if err := g.Triples(ctx, gtrpls); err != nil {
			t.Errorf("g.Triples failed to retrieve triples with error %v", err)
		}
	}()
	for trpl := range gtrpls {
		m[trpl.UUID().String()] = true
		gs++
	}
	gos := 0
	g2trpls := make(chan *triple.Triple)
	go func() {
		if err := g2.Triples(ctx, g2trpls); err != nil {
			t.Errorf("g2.Triples failed to retrieve triples with error %v", err)
		}
	}()
	for trpl := range g2trpls {
		if _, ok := m[trpl.UUID().String()]; !ok {
			t.Errorf("Failed to unmarshal marshaled triple; could not find triple %s", trpl.String())
		}
		gos++
	}
	if gs != gos || gs != 6 || gos != 6 {
		t.Errorf("Failed to unmarshal marshaled the right number of triples, %d != %d != 6", gs, gos)
	}
}
