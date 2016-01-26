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

package compliance

import (
	"reflect"
	"testing"
)

func TestJSONMarshaling(t *testing.T) {
	testStory := &Story{
		Name: "Story",
		Sources: []*Graph{
			{
				ID: "?g",
				Facts: []string{
					"A parseable triple",
				},
			},
		},
		Assertions: []*Assertion{
			{
				Requires:  "Assertion description",
				Statement: "some BQL statement;",
				WillFail:  false,
				MustReturn: []map[string]string{
					{"?foo": "foo"},
				},
			},
		},
	}
	ms, err := testStory.Marshal()
	if err != nil {
		t.Errorf("testStory.Marshal failed with error %v", err)
	}
	newStory := &Story{}
	if err := newStory.Unmarshal(ms); err != nil {
		t.Errorf("newStory.Unmarshal failed with error %v", err)
	}
	if got, want := newStory, testStory; !reflect.DeepEqual(got, want) {
		t.Errorf("failed to mashal and unmarshal; got %v, want %v", got, want)
	}
}

func TestOutputTableAssertion(t *testing.T) {
	testAssertion := &Assertion{
		Requires:  "Assertion description",
		Statement: "some BQL statement;",
		WillFail:  false,
		MustReturn: []map[string]string{
			{"?foo": "foo"},
		},
	}
	table, err := testAssertion.OutputTable()
	if err != nil {
		t.Errorf("testAssertion.OutputTable failed with error %v", err)
	}
	if got, want := table.NumRows(), 1; got != want {
		t.Errorf("failed to build a table with the right number of rows, got %d, want %d", got, want)
	}
	r, ok := table.Row(0)
	if !ok {
		t.Fatalf("failed to retrieve the first row with error")
	}
	if got, want := "foo", r["?foo"].S; got != want {
		t.Errorf("failed to provide the right value; got %v, want %v", got, want)
	}
}
