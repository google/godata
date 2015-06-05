/*
Copyright 2014 Google Inc. All Rights Reserved.

Licensed under the Apache License, Version 2.0 (the "License");
you may not use this file except in compliance with the License.
You may obtain a copy of the License at

    http://www.apache.org/licenses/LICENSE-2.0

Unless required by applicable law or agreed to in writing, software
distributed under the License is distributed on an "AS IS" BASIS,
WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
See the License for the specific language governing permissions and
limitations under the License.
*/

package godata

import "fmt"

// Value represents general values that can be stored and indexed.
type Value interface {
	// V returns the encapsulated value.
	V() interface{}

	// Hash returns a unique string representation of the value. The function satisfies the
	// relation: v1.Hash() == v2.Hash() if and only if v1 == v2, and the type of v1 is the same as
	// the type of v2, for all values. The returned string must not contain the forward slash '/'.
	Hash() string

	// String returns a user-displayed value. The value does not have to satisfy uniqueness properties.
	String() string

	// Equals returns true if the value equals the given value.
	Equals(v Value) bool
}

// MultiValue represents a Value composed of multiple underlying values.
type MultiValue struct {
	Head Value
	Tail *MultiValue
}

// NewMultiValue returns a MultiValue containing the given values.
func NewMultiValue(val []Value) *MultiValue {
	r := &MultiValue{}
	current := r
	for _, v := range val {
		current.Head = v
		current.Tail = &MultiValue{}
		current = current.Tail
	}
	return r
}

// V returns a list containing the head and tail values.
func (m *MultiValue) V() interface{} {
	return []Value{m.Head, m.Tail}
}

// Hash returns a concatenated hash of the underlying values.
func (m *MultiValue) Hash() string {
	return fmt.Sprintf("MultiValue(%s/%s)", m.Head, m.Tail)
}

// String returns a user-displayed value for the head and tail.
func (m *MultiValue) String() string {
	return fmt.Sprintf("(%v, %v)", m.Head, m.Tail)
}

// Equals returns true if the given value is a MultiValue with the same head and tail.
func (m *MultiValue) Equals(v Value) bool {
	if multi, ok := v.(*MultiValue); ok {
		return m.Head.Equals(multi.Head) && m.Tail.Equals(multi.Tail)
	}
	return false
}

// ValuesAre returns true if the underlying value in each Value object equals the corresponding
// object in the comparison array.
func ValuesAre(v []Value, p []interface{}) bool {
	if len(v) != len(p) {
		return false
	}
	for i, val := range v {
		if val.V() != p[i] {
			return false
		}
	}
	return true
}

// ValuesEqual returns true if the underlying value in each Value object equals the corresponding
// value's object in the comparison array.
func ValuesEqual(v []Value, p []Value) bool {
	if len(v) != len(p) {
		return false
	}
	for i, val := range v {
		if !val.Equals(p[i]) {
			return false
		}
	}
	return true
}

// UniqueValues returns a list of unique values in the given list. Uniqueness is determined based on
// the value hash.
func UniqueValues(v []Value) []Value {
	uniqueHashes := make(map[string]struct{})
	uniqueValues := make([]Value, 0, len(v))
	for _, val := range v {
		if _, ok := uniqueHashes[val.Hash()]; !ok {
			uniqueHashes[val.Hash()] = struct{}{}
			uniqueValues = append(uniqueValues, val)
		}
	}
	return uniqueValues
}

// MatrixEqual returns true if the underlying value in each Value object equals the corresponding
// value's object int he comparison matrix, and if the dimensions of the matrices are equal.
func MatrixEqual(v [][]Value, p [][]Value) bool {
	if len(v) != len(p) {
		return false
	}
	for i, val := range v {
		if !ValuesEqual(val, p[i]) {
			return false
		}
	}
	return true
}
