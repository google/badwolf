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

// Package filter isolates core FILTER related implementation.
package filter

import (
	"fmt"
)

// Operation represents a filter operation supported in BadWolf.
type Operation int

// List of supported filter operations.
const (
	Latest Operation = iota + 1
	IsImmutable
	IsTemporal
)

// Field represents the position of the semantic.GraphClause that will be operated by the filter at storage level.
type Field int

// List of filter fields.
const (
	SubjectField Field = iota + 1
	PredicateField
	ObjectField
)

// SupportedOperations maps suported filter operation strings to their correspondant Operation.
// Note that the string keys here must be in lowercase letters only (for compatibility with the WhereFilterClauseHook).
var SupportedOperations = map[string]Operation{
	"latest":      Latest,
	"isimmutable": IsImmutable,
	"istemporal":  IsTemporal,
}

// OperationRequiresValue keeps track of the filter Operations that require Value in the filter clause.
var OperationRequiresValue = map[Operation]bool{}

// StorageOptions represent the storage level specifications for the filtering to be executed.
// Operation below refers to the filter function being applied (eg: Latest), Field refers to the position of the graph clause it
// will be applied to (subject, predicate, or object) and Value, when specified, contains the second argument of the filter
// function (not applicable for all Operations - some like Latest do not use it while others like GreaterThan do, see Issue 129).
type StorageOptions struct {
	Operation Operation
	Field     Field
	Value     string
}

// String returns the string representation of Operation.
func (op Operation) String() string {
	switch op {
	case Latest:
		return "latest"
	case IsImmutable:
		return "isImmutable"
	case IsTemporal:
		return "isTemporal"
	default:
		return fmt.Sprintf(`not defined filter operation "%d"`, op)
	}
}

// IsEmpty returns true if the Operation was not set yet.
func (op Operation) IsEmpty() bool {
	return op == Operation(0)
}

// String returns the string representation of Field.
func (f Field) String() string {
	switch f {
	case SubjectField:
		return "subject field"
	case PredicateField:
		return "predicate field"
	case ObjectField:
		return "object field"
	default:
		return fmt.Sprintf(`not defined filter field "%d"`, f)
	}
}

// String returns the string representation of StorageOptions.
func (so *StorageOptions) String() string {
	return fmt.Sprintf("%+v", *so)
}
