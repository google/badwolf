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

// Package io provides basic tools to read and write graphs from and to files.
package io

import (
	"bufio"
	"context"
	"fmt"
	"io"
	"strings"
	"sync"

	"github.com/google/badwolf/storage"
	"github.com/google/badwolf/triple"
	"github.com/google/badwolf/triple/literal"
)

// ReadIntoGraph reads a graph out of the provided reader. The data on the
// reader is interpret as text. Each line represents one triple using the
// standard serialized format. ReadIntoGraph will stop if fails to Parse
// a triple on the stream. The triples read till then would have also been
// added to the graph. The int value returns the number of triples added.
func ReadIntoGraph(ctx context.Context, g storage.Graph, r io.Reader, b literal.Builder) (int, error) {
	cnt, scanner := 0, bufio.NewScanner(r)
	scanner.Split(bufio.ScanLines)
	for scanner.Scan() {
		text := strings.TrimSpace(scanner.Text())
		if text == "" {
			continue
		}
		t, err := triple.Parse(text, b)
		if err != nil {
			return cnt, err
		}
		cnt++
		g.AddTriples(ctx, []*triple.Triple{t})
	}
	return cnt, nil
}

// WriteGraph serializes the graph into the writer where each triple is
// marshaled into a separate line. If there is an error writing the
// serialization will stop. It returns the number of triples serialized
// regardless if it succeeded or failed partially.
func WriteGraph(ctx context.Context, w io.Writer, g storage.Graph) (int, error) {
	var (
		wg   sync.WaitGroup
		tErr error
		wErr error
	)
	cnt, ts := 0, make(chan *triple.Triple)
	wg.Add(1)
	go func() {
		defer wg.Done()
		tErr = g.Triples(ctx, storage.DefaultLookup, ts)
	}()
	for t := range ts {
		if wErr != nil {
			continue
		}
		if _, err := io.WriteString(w, fmt.Sprintf("%s\n", t.String())); err != nil {
			wErr = err
			continue
		}
		cnt++
	}
	wg.Wait()
	if tErr != nil {
		return 0, tErr
	}
	if wErr != nil {
		return 0, wErr
	}
	return cnt, nil
}
