package literal

import (
	"fmt"
	"reflect"
	"testing"
)

func TestDefaultBuilder(t *testing.T) {
	table := []struct {
		t    Type
		v    interface{}
		want *literal
	}{
		// Successful cases.
		{Bool, true, &literal{Bool, interface{}(true)}},
		{Bool, false, &literal{Bool, interface{}(false)}},
		{Int64, int64(-1), &literal{Int64, interface{}(int64(-1))}},
		{Int64, int64(0), &literal{Int64, interface{}(int64(0))}},
		{Int64, int64(1), &literal{Int64, interface{}(int64(1))}},
		{Float64, float64(-1), &literal{Float64, interface{}(float64(-1))}},
		{Float64, float64(0), &literal{Float64, interface{}(float64(0))}},
		{Float64, float64(1), &literal{Float64, interface{}(float64(1))}},
		{Text, "", &literal{Text, interface{}("")}},
		{Text, "some random string", &literal{Text, interface{}("some random string")}},
		{Blob, []byte{}, &literal{Blob, []byte{}}},
		{Blob, []byte("some random bytes"), &literal{Blob, interface{}([]byte("some random bytes"))}},
		// Invalid cases.
		{Bool, 1, nil},
		{Int64, 2, nil},
		{Float64, 3, nil},
		{Text, 4, nil},
		{Blob, 5, nil},
	}
	for _, tc := range table {
		got, err := DefaultBuilder().Build(tc.t, tc.v)
		if tc.want != nil && err != nil {
			t.Errorf("Failed to generate literal for case %v with error %v", tc, err)
		}
		if (got != nil || tc.want != nil) && !reflect.DeepEqual(got, tc.want) {
			t.Errorf("Failed to generate the expected triple; got %v want %v", got, tc.want)
		}
	}
}

func TestBoundedBuilder(t *testing.T) {
	max, table := 10, []struct {
		t    Type
		v    interface{}
		want *literal
	}{
		// Successful cases.
		{Text, "0123456789", &literal{Text, interface{}("0123456789")}},
		{Blob, []byte("0123456789"), &literal{Blob, interface{}([]byte("0123456789"))}},
		// Invalid cases.
		{Text, "01234567890", nil},
		{Blob, []byte("01234567890"), nil},
	}
	b := NewBoundedBuilder(max)
	for _, tc := range table {
		got, err := b.Build(tc.t, tc.v)
		if tc.want != nil && err != nil {
			t.Errorf("Failed to generate literal for case %v with error %v", tc, err)
		}
		if (got != nil || tc.want != nil) && !reflect.DeepEqual(got, tc.want) {
			t.Errorf("Failed to generate the expected triple; got %v want %v", got, tc.want)
		}
	}
}

func TestPrettyPrinting(t *testing.T) {
	table := []struct {
		t    Type
		v    interface{}
		want string
	}{
		// Successful cases.
		{Bool, true, `"true"^^type:bool`},
		{Bool, false, `"false"^^type:bool`},
		{Int64, int64(-1), `"-1"^^type:int64`},
		{Int64, int64(0), `"0"^^type:int64`},
		{Int64, int64(1), `"1"^^type:int64`},
		{Float64, float64(-1), `"-1"^^type:float64`},
		{Float64, float64(0), `"0"^^type:float64`},
		{Float64, float64(1), `"1"^^type:float64`},
		{Text, "", `""^^type:text`},
		{Text, "some random string", `"some random string"^^type:text`},
		{Blob, []byte{}, `"[]"^^type:blob`},
		{Blob, []byte("some random bytes"), `"[115 111 109 101 32 114 97 110 100 111 109 32 98 121 116 101 115]"^^type:blob`},
	}
	for _, tc := range table {
		lit, err := DefaultBuilder().Build(tc.t, tc.v)
		if err != nil {
			t.Errorf("Failed to generate literal for case %v with error %v", tc, err)
		}
		if got := fmt.Sprintf("%s", lit); got != tc.want {
			t.Errorf("Failed to pretty print a literal; got %s, want %s", got, tc.want)
		}
	}
}

func TestParse(t *testing.T) {
	table := []struct {
		t Type
		v interface{}
		s string
	}{
		// Successful cases.
		{Bool, true, `"true"^^type:bool`},
		{Bool, false, `"false"^^type:bool`},
		{Int64, int64(-1), `"-1"^^type:int64`},
		{Int64, int64(0), `"0"^^type:int64`},
		{Int64, int64(1), `"1"^^type:int64`},
		{Float64, float64(-1), `"-1"^^type:float64`},
		{Float64, float64(0), `"0"^^type:float64`},
		{Float64, float64(1), `"1"^^type:float64`},
		{Text, "", `""^^type:text`},
		{Text, "some random string", `"some random string"^^type:text`},
		{Blob, []byte{}, `"[]"^^type:blob`},
		{Blob, []byte("some random bytes"), `"[115 111 109 101 32 114 97 110 100 111 109 32 98 121 116 101 115]"^^type:blob`},
	}
	for _, tc := range table {
		want, err := DefaultBuilder().Build(tc.t, tc.v)
		if err != nil {
			t.Errorf("Failed to generate literal for case %v with error %v", tc, err)
		}
		got, err := DefaultBuilder().Parse(tc.s)
		if err != nil {
			t.Errorf("Failed to parse pretty printed literal %s with error %v", tc.s, err)
		}
		if !reflect.DeepEqual(got, want) {
			t.Errorf("Failed to parse correctly %s; got %v, want %s", tc.s, got, want)
		}
	}
}
