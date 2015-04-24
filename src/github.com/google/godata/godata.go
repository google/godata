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

// Package godata defines common types and utilities for handling data.
package godata

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
}

// Predicate takes a given list of values and returns true if the values satisfy the predicate.
type Predicate func([]Value) bool

// Transform takes the given list of values and returns a list of values.
type Transform func([]Value) []Value

type View interface {
	// Cols returns a View containing the given columns.
	Cols([]Value) View

	// Rows returns a View containing the given rows.
	Rows([]Value) View

	// ColAt returns the column at the given column index.
	ColAt(int) []Value

	// RowAt returns the row at the given row index.
	RowAt(int) []Value

	// NumCols returns the number of columns in the View.
	NumCols() int

	// NumRows returns the number of rows in the View.
	NumRows() int

	// ColIndex returns the column index of this View.
	ColIndex() []Value

	// RowIndex returns the row index of this View.
	RowIndex() []Value

	// WhereColIndex returns a View containing all columns where the column index satisfies the
	// predicate.
	WhereColIndex(Predicate) View

	// WhereRowIndex returns a View containing all rows where the row index satisfies the predicate.
	WhereRowIndex(Predicate) View

	// RowsWhereCols returns a View containing the rows for which the given predicate is satisfied on the
	// given columns. If cols is nil, then use all columns in the predicate.
	RowsWhereCols(cols []Value, p Predicate) View

	// ColsWhereRows returns a View containing the columns for which the given predicate is satisfied on
	// the given rows. If rows is nil, then use all rows in the predicate.
	ColsWhereRows(rows []Value, p Predicate) View

	// ForEachRow applies the given transform to the given columns for each row, and returns a view on
	// the results, where the results have column indices given by out. If cols is nil, then operate
	// on all columns. If out is nil, then use the existing column indices in the transformed view.
	ForEachRow(cols []Value, out []Value, t Transform) View

	// ForEachCol applies the given transform to the given rows for each column, and return a view on
	// the results, where the results have row indices given by out. If rows is nil, then operate on
	// all rows. If out is nil, then use the existing row indices in the transformed view.
	ForEachCol(rows []Value, out []Value, t Transform) View

	// String returns a string representation of the View.
	String() string

	// Matrix returns the matrix representation of the DataFrame.
	Matrix() [][]Value
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
		if val.V() != p[i].V() {
			return false
		}
	}
	return true
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
