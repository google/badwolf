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
	"testing"

	"github.com/google/badwolf/storage/memory"
	"github.com/google/badwolf/triple/literal"
)

func TestRun(t *testing.T) {
	testStories := []*Story{
		{
			Name: "First Story",
			Sources: []*Graph{
				{
					ID: "?g",
					Facts: []string{
						"/t<id> \"predicate\"@[] /foo<bar>",
					},
				},
			},
			Assertions: []*Assertion{
				{
					Requires:  "retriving the types",
					Statement: "SELECT ?type FROM ?g WHERE {/t<id> \"predicate\"@[] /foo<bar> TYPE ?type};",
					WillFail:  false,
					MustReturn: []map[string]string{
						{"?type": "/foo"},
					},
				},
			},
		},
		{
			Name: "Second Story",
			Sources: []*Graph{
				{
					ID: "?g",
					Facts: []string{
						"/t<id> \"predicate\"@[] /foo<bar>",
					},
				},
			},
			Assertions: []*Assertion{
				{
					Requires:  "retrieving the object",
					Statement: "SELECT ?o FROM ?g WHERE {/t<id> \"predicate\"@[] ?o};",
					WillFail:  true,
					MustReturn: []map[string]string{
						{"?o": "/foo<bar>"},
					},
				},
			},
		},
		{
			Name: "Third Story",
			Sources: []*Graph{
				{
					ID: "?g",
					Facts: []string{
						"/t<id> \"predicate\"@[] /foo<bar>",
						"/t<id> \"predicate\"@[] /foo<bar2>",
					},
				},
			},
			Assertions: []*Assertion{
				{
					Requires:  "retrieving the objects",
					Statement: "SELECT ?o FROM ?g WHERE {/t<id> \"predicate\"@[] ?o} ORDER BY ?o DESC;",
					WillFail:  true,
					MustReturn: []map[string]string{
						{"?o": "/foo<bar>"},
						{"?o": "/foo<bar2>"},
					},
				},
			},
		},
	}
	for _, s := range testStories {
		m, err := s.Run(memory.NewStore(), literal.DefaultBuilder())
		if err != nil {
			t.Error(err)
		}
		for s, sao := range m {
			if !sao.Equal {
				t.Errorf("%q should have not returned false; got\n%s\nwant\n%s", s, sao.Got, sao.Want)
			}
		}
	}
}
