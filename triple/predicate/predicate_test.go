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

package predicate

import (
	"fmt"
	"reflect"
	"testing"
	"time"
)

var (
	immutFoo *Predicate
	tempBar  *Predicate
)

func init() {
	immutFoo, _ = NewImmutable("foo")
	tempBar, _ = NewTemporal("bar", time.Now())
}

func TestIDsAndTypes(t *testing.T) {
	table := []struct {
		gotID    ID
		wantID   ID
		gotType  Type
		wantType Type
	}{
		{immutFoo.ID(), ID("foo"), immutFoo.Type(), Immutable},
		{tempBar.ID(), ID("bar"), tempBar.Type(), Temporal},
	}
	for _, tc := range table {
		if tc.gotID != tc.wantID {
			t.Errorf("predicate.Type returned wrong predicate ID; got %s, want %s", tc.gotID, tc.wantID)
		}
		if tc.gotType != tc.wantType {
			t.Errorf("predicate.Type returned wrong predicate type; got %s, want %s", tc.gotType, tc.wantType)
		}
	}
}

func TestTimeAnchor(t *testing.T) {
	want := time.Now()
	temp, err := NewTemporal("bar", want)
	if err != nil {
		t.Error(err)
	}
	got, err := temp.TimeAnchor()
	if err != nil {
		t.Errorf("predicate.TimeAnchor failed to return time anchor in %v due to %v", got, err)
	}
	if !reflect.DeepEqual(*got, want) {
		t.Errorf("predicate.TimeAnchor failed to return the right time; got %v, want %v", got, want)
	}
}

func TestPrettyPrint(t *testing.T) {
	now := time.Now()
	format := now.Format(time.RFC3339Nano)
	temp, err := NewTemporal("bar", now)
	if err != nil {
		t.Error(err)
	}
	table := []struct {
		got  string
		want string
	}{
		{immutFoo.String(), "\"foo\"@[]"},
		{temp.String(), fmt.Sprintf("\"bar\"@[%s]", format)},
	}
	for _, tc := range table {
		if tc.got != tc.want {
			t.Errorf("predicate.String failed to pretty print the string; got %s, want %s", tc.got, tc.want)
		}
	}
}

func TestParse(t *testing.T) {
	if got, err := Parse(""); err == nil {
		t.Errorf("predicate.Parse should reject parsing \"\", but instead returned %v", got)
	}
	if got, err := Parse("id\"@[]"); err == nil {
		t.Errorf("predicate.Parse should reject parsing strings that do not start with \", but instead got %v", got)
	}
	if got, err := Parse("\"foo\""); err == nil {
		t.Errorf("predicate.Parse should reject parsing arbitrary strings, but instead got %v", got)
	}

	date := "2015-07-19T13:12:04.669618843-07:00"
	pd, err := time.Parse(time.RFC3339Nano, date)
	if err != nil {
		t.Fatalf("time.Parse failed to parse valid time %s with error %v", date, err)
	}
	pretty := fmt.Sprintf("\"bar\"@[%s]", date)
	got, err := Parse(pretty)
	if err != nil {
		t.Fatalf("predicate.Parse could not create a predicate for %s with error %v", pretty, err)
	}
	if got.Type() != Temporal {
		t.Errorf("predicate.Parse should have returned a temporal predicate, instead returned %s", got)
	}
	gotTA, err := got.TimeAnchor()
	if err != nil {
		t.Errorf("predicate.TimeAnchor failed to retrieve time anchor from %v with error %v", got, err)
	}
	if got, want := *gotTA, pd; !reflect.DeepEqual(got, want) {
		t.Errorf("predicate.Parse failed to parse the proper time anchor; got %s, want %s", got, want)
	}

	imm, err := Parse("\"foo\"@[]")
	if err != nil {
		t.Errorf("predicate.Parse failed to parse immutable predicate \"foo\"@[] with error %v", err)
	}
	if imm.Type() != Immutable || imm.ID() != "foo" {
		t.Errorf("predicate.Parse failed to parse immutable predicate \"foo\"@[]; got %v instead", imm)
	}
}

func TestQuotedID(t *testing.T) {
	const id = "ba\"r"
	const pretty = "\"ba\\\"r\"@[]"
	immut, err := NewImmutable(id)
	if err != nil {
		t.Fatal(err)
	}
	if got, want := immut.String(), pretty; got != want {
		t.Fatalf("predicate.String() = %v; want %v", got, want)
	}

	immut, err = Parse(pretty)
	if err != nil {
		t.Fatalf("predicate.Parse failed to parse immutable predicate \"foo\"@[] with error %v", err)
	}
	if immut.Type() != Immutable || immut.ID() != id {
		t.Errorf("predicate.Parse failed to parse immutable predicate %v; got %v instead", pretty, immut)
	}
}

func TestPartialUUID(t *testing.T) {
	p1, err := NewTemporal("foo", time.Now())
	if err != nil {
		t.Fatal(err)
	}
	p2, err := NewTemporal("foo", time.Now().Add(time.Nanosecond))
	if err != nil {
		t.Fatal(err)
	}
	if uuid1, uuid2 := p1.UUID(), p2.UUID(); reflect.DeepEqual(uuid1, uuid2) {
		t.Errorf("predicates %v and %v should have different UUID; got %q=%q", p1, p2, uuid1.String(), uuid2.String())
	}
	if uuid1, uuid2 := p1.PartialUUID(), p2.PartialUUID(); !reflect.DeepEqual(uuid1, uuid2) {
		t.Errorf("predicates %v and %v should have identical partial UUID; got %q=%q", p1, p2, uuid1.String(), uuid2.String())
	}
}
