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

package semantic

import (
	"reflect"
	"testing"

	"github.com/google/badwolf/triple"
	"github.com/google/badwolf/triple/literal"
)

func TestStatementType(t *testing.T) {
	st := NewStatement(Query)
	if got, want := st.Type(), Query; got != want {
		t.Errorf("semantic.NewStatement returned wrong statement type; got %s, want %s", got, want)
	}
}

func TestStatementAddGraph(t *testing.T) {
	st := NewStatement(Query)
	st.AddGraph("?foo")
	if got, want := st.Graphs(), []string{"?foo"}; !reflect.DeepEqual(got, want) {
		t.Errorf("semantic.AddGraph returned the wrong graphs avaiable; got %v, want %v", got, want)
	}
}

func TestStatementAddData(t *testing.T) {
	tr, err := triple.ParseTriple(`/_<foo> "foo"@[] /_<bar>`, literal.DefaultBuilder())
	if err != nil {
		t.Fatalf("triple.ParseTriple failed to parse valid triple with error %v", err)
	}
	st := NewStatement(Query)
	st.AddData(tr)
	if got, want := st.Data(), []*triple.Triple{tr}; !reflect.DeepEqual(got, want) {
		t.Errorf("semantic.AddData returned the wrong data avaiable; got %v, want %v", got, want)
	}
}
