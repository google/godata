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

import (
	"errors"
	"fmt"
	"math"
	"testing"
)

var (
	rawIntValues = [][]int{{1, 2, 3}, {4, 5, 6}}
	intValues    = [][]Value{Ints([]int{1, 2, 3}), Ints([]int{4, 5, 6})}
	intRows      = Ints([]int{0, 1})
	intCols      = Ints([]int{0, 1, 2})
	strRows      = Strings([]string{"a", "b"})
	strCols      = Strings([]string{"a", "b", "c"})
)

func TestNewDataFrame(t *testing.T) {
	tests := []struct {
		rows     []Value
		cols     []Value
		values   [][]Value
		err      error
		rowIndex []Value
		colIndex []Value
	}{
		{nil, nil, intValues, nil, intRows, intCols},
		{strRows, nil, intValues, nil, strRows, intCols},
		{nil, strCols, intValues, nil, intRows, strCols},
		{strRows, strCols, intValues, nil, strRows, strCols},
		{Strings([]string{"a", "a"}), strCols, intValues, errors.New("some error"), nil, nil},
		{strRows, Strings([]string{"a", "b", "a"}), intValues, errors.New("some error"), nil, nil},
		{Strings([]string{"a"}), strCols, intValues, errors.New("some error"), nil, nil},
		{strRows, Strings([]string{"a"}), intValues, errors.New("some error"), nil, nil},
	}

	for _, tt := range tests {
		df, err := NewDataFrame(tt.rows, tt.cols, tt.values)
		if (err == nil && tt.err != nil) || (err != nil && tt.err == nil) {
			t.Errorf("NewDataFrame(%v, %v, %v) => error %v, expected %v", tt.rows, tt.cols, tt.values, err, tt.err)
			continue
		}
		if err != nil {
			continue
		}
		if len(df.RowIndex()) != len(tt.rowIndex) {
			t.Errorf("len(df.RowIndex()) => %d, expected %d", len(df.RowIndex()), len(tt.rowIndex))
			continue
		}
		if df.NumRows() != len(tt.rowIndex) {
			t.Errorf("df.NumRows() => %d, expected %d", df.NumRows(), len(tt.rowIndex))
			continue
		}
		if !ValuesEqual(df.RowIndex(), tt.rowIndex) {
			t.Errorf("df.RowIndex() => %v, expected %v", df.RowIndex(), tt.rowIndex)
		}
		if len(df.ColIndex()) != len(tt.colIndex) {
			t.Errorf("len(df.ColIndex()) => %d, expected %d", len(df.ColIndex()), len(tt.colIndex))
			continue
		}
		if df.NumCols() != len(tt.colIndex) {
			t.Errorf("df.NumCols() => %d, expected %d", df.NumCols(), len(tt.colIndex))
			continue
		}
		if !ValuesEqual(df.ColIndex(), tt.colIndex) {
			t.Errorf("df.ColIndex() => %v, expected %v", df.ColIndex(), tt.colIndex)
		}
	}
}

func TestNewDataFrameFromInts(t *testing.T) {
	tests := []struct {
		rows     []Value
		cols     []Value
		values   [][]int
		err      error
		rowIndex []Value
		colIndex []Value
	}{
		{nil, nil, rawIntValues, nil, intRows, intCols},
		{strRows, nil, rawIntValues, nil, strRows, intCols},
		{nil, strCols, rawIntValues, nil, intRows, strCols},
		{strRows, strCols, rawIntValues, nil, strRows, strCols},
	}

	for _, tt := range tests {
		df, err := NewDataFrameFromInts(tt.rows, tt.cols, tt.values)
		if (err == nil && tt.err != nil) || (err != nil && tt.err == nil) {
			t.Errorf("NewDataFrameFromInts(%v, %v, %v) => error %v, expected %v", tt.rows, tt.cols, tt.values, err, tt.err)
			continue
		}
		if err != nil {
			continue
		}
		if len(df.RowIndex()) != len(tt.rowIndex) {
			t.Errorf("len(df.RowIndex()) => %d, expected %d", len(df.RowIndex()), len(tt.rowIndex))
			continue
		}
		for i, r := range df.RowIndex() {
			if r.V() != tt.rowIndex[i].V() {
				t.Errorf("df.RowIndex()[%d] => %v, expected %v", i, r.V(), tt.rowIndex[i].V())
			}
		}
		if len(df.ColIndex()) != len(tt.colIndex) {
			t.Errorf("len(df.ColIndex()) => %d, expected %d", len(df.ColIndex()), len(tt.colIndex))
			continue
		}
		for i, c := range df.ColIndex() {
			if c.V() != tt.colIndex[i].V() {
				t.Errorf("df.ColIndex()[%d] => %v, expected %v", i, c.V(), tt.colIndex[i].V())
			}
		}
	}
}

func TestColsAndRead(t *testing.T) {
	tests := []struct {
		cols      []Value
		row       int
		rowValues []Value
		col       int
		colValues []Value
	}{
		{Ints([]int{0, 1}), 0, Ints([]int{1, 2}), 0, Ints([]int{1, 4})},
		{Ints([]int{0, 2}), 0, Ints([]int{1, 3}), 0, Ints([]int{1, 4})},
		{Ints([]int{1, 0}), 0, Ints([]int{2, 1}), 0, Ints([]int{2, 5})},
	}

	for _, tt := range tests {
		df, err := NewDataFrame(nil, nil, intValues)
		if err != nil {
			t.Errorf("NewDataFrame(nil, nil, %v) => error %v", intValues, err)
			continue
		}
		originalString := df.String()
		view := df.Cols(tt.cols)

		row := view.RowAt(tt.row)
		if len(row) != len(tt.rowValues) {
			t.Errorf("len(view.RowAt(%d)) => %d, expected %d", tt.row, row, len(tt.rowValues))
			continue
		}
		for i, v := range row {
			if v.V() != tt.rowValues[i].V() {
				t.Errorf("view.RowAt(%d)[%d] => %v, expected %v", tt.row, i, v.V(), tt.rowValues[i].V())
			}
		}

		col := view.ColAt(tt.col)
		if len(col) != len(tt.colValues) {
			t.Errorf("len(view.ColAt(%d)) => %d, expected %d", tt.col, col, len(tt.colValues))
			continue
		}
		for i, v := range col {
			if v.V() != tt.colValues[i].V() {
				t.Errorf("view.ColAt(%d)[%d] => %v, expected %v", tt.col, i, v.V(), tt.colValues[i].V())
			}
		}
		newString := df.String()
		if originalString != newString {
			t.Errorf("df.String() changed to %v from %v, expected no change", newString, originalString)
		}
	}
}

func TestRowsAndRead(t *testing.T) {
	tests := []struct {
		rows      []Value
		row       int
		rowValues []Value
		col       int
		colValues []Value
	}{
		{Ints([]int{0, 1}), 0, Ints([]int{1, 2, 3}), 0, Ints([]int{1, 4})},
		{Ints([]int{1, 0}), 0, Ints([]int{4, 5, 6}), 0, Ints([]int{4, 1})},
		{Ints([]int{0}), 0, Ints([]int{1, 2, 3}), 0, Ints([]int{1})},
	}

	for _, tt := range tests {
		df, err := NewDataFrame(nil, nil, intValues)
		if err != nil {
			t.Errorf("NewDataFrame(nil, nil, %v) => error %v", intValues, err)
			continue
		}
		originalString := df.String()
		view := df.Rows(tt.rows)

		row := view.RowAt(tt.row)
		if len(row) != len(tt.rowValues) {
			t.Errorf("len(view.RowAt(%d)) => %d, expected %d", tt.row, row, len(tt.rowValues))
			continue
		}
		for i, v := range row {
			if v.V() != tt.rowValues[i].V() {
				t.Errorf("view.RowAt(%d)[%d] => %v, expected %v", tt.row, i, v.V(), tt.rowValues[i].V())
			}
		}
		col := view.ColAt(tt.col)
		if len(col) != len(tt.colValues) {
			t.Errorf("len(view.ColAt(%d)) => %d, expected %d", tt.col, col, len(tt.colValues))
			continue
		}
		for i, v := range col {
			if v.V() != tt.colValues[i].V() {
				t.Errorf("view.ColAt(%d)[%d] => %v, expected %v", tt.col, i, v.V(), tt.colValues[i].V())
			}
		}
		newString := df.String()
		if originalString != newString {
			t.Errorf("df.String() changed to %v from %v, expected no change", newString, originalString)
		}
	}
}

func nothingPredicate(_ []Value) bool {
	return false
}

func evenPredicate(v []Value) bool {
	for _, val := range v {
		if intVal, ok := val.V().(int); ok && math.Mod(float64(intVal), 2) != 0 {
			return false
		}
	}
	return true
}

func oddPredicate(v []Value) bool {
	for _, val := range v {
		if intVal, ok := val.V().(int); ok && math.Mod(float64(intVal), 2) != 1 {
			return false
		}
	}
	return true
}

func TestWhereColIndex(t *testing.T) {
	tests := []struct {
		predicate Predicate
		columns   []interface{}
	}{
		{nothingPredicate, []interface{}{}},
		{evenPredicate, []interface{}{0, 2}},
		{oddPredicate, []interface{}{1}},
	}
	originalCols := []interface{}{0, 1, 2}
	for _, tt := range tests {
		df, err := NewDataFrame(nil, nil, intValues)
		if err != nil {
			t.Errorf("NewDataFrame(nil, nil, %v) => error %v", intValues, err)
			continue
		}
		view := df.WhereColIndex(tt.predicate)
		if !ValuesAre(view.ColIndex(), tt.columns) {
			t.Errorf("view.ColIndex() => %v, expected %v", view.ColIndex(), tt.columns)
		}
		if !ValuesAre(df.ColIndex(), originalCols) {
			t.Errorf("view.ColIndex() changed original cols from %v to %v", originalCols, df.ColIndex())
		}
	}

}

func TestWhereRowIndex(t *testing.T) {
	tests := []struct {
		predicate Predicate
		rows      []interface{}
	}{
		{nothingPredicate, []interface{}{}},
		{evenPredicate, []interface{}{0}},
		{oddPredicate, []interface{}{1}},
	}
	originalRows := []interface{}{0, 1}
	for _, tt := range tests {
		df, err := NewDataFrame(nil, nil, intValues)
		if err != nil {
			t.Errorf("NewDataFrame(nil, nil, %v) => error %v", intValues, err)
			continue
		}
		view := df.WhereRowIndex(tt.predicate)
		if !ValuesAre(view.RowIndex(), tt.rows) {
			t.Errorf("view.RowIndex() => %v, expected %v", view.RowIndex(), tt.rows)
		}
		if !ValuesAre(df.RowIndex(), originalRows) {
			t.Errorf("view.RowIndex() changed original cols from %v to %v", originalRows, df.RowIndex())
		}
	}
}

func TestRowsWhereCols(t *testing.T) {
	tests := []struct {
		predicate     Predicate
		predicateCols []Value
		rows          []Value
	}{
		{nothingPredicate, Ints([]int{0}), Ints([]int{})},
		{evenPredicate, Ints([]int{0}), Ints([]int{1})},
		{evenPredicate, Ints([]int{1}), Ints([]int{0})},
		{evenPredicate, Ints([]int{0, 1, 2}), Ints([]int{})},
	}
	originalRows := Ints([]int{0, 1})
	for _, tt := range tests {
		df, err := NewDataFrame(nil, nil, intValues)
		if err != nil {
			t.Errorf("NewDataFrame(nil, nil, %v) => error %v", intValues, err)
			continue
		}
		view := df.RowsWhereCols(tt.predicateCols, tt.predicate)
		if !ValuesEqual(view.RowIndex(), tt.rows) {
			t.Errorf("view.RowIndex() => %v, expected %v", view.RowIndex(), tt.rows)
		}
		if !ValuesEqual(df.RowIndex(), originalRows) {
			t.Errorf("view.RowIndex() changed original cols from %v to %v", originalRows, df.RowIndex())
		}
	}
}

func TestColsWhereRows(t *testing.T) {
	tests := []struct {
		predicate     Predicate
		predicateRows []Value
		cols          []Value
	}{
		{nothingPredicate, Ints([]int{0}), Ints([]int{})},
		{evenPredicate, Ints([]int{0}), Ints([]int{1})},
		{evenPredicate, Ints([]int{1}), Ints([]int{0, 2})},
		{evenPredicate, Ints([]int{0, 1}), Ints([]int{})},
	}
	originalCols := Ints([]int{0, 1, 2})
	for _, tt := range tests {
		df, err := NewDataFrame(nil, nil, intValues)
		if err != nil {
			t.Errorf("NewDataFrame(nil, nil, %v) => error %v", intValues, err)
			continue
		}
		view := df.ColsWhereRows(tt.predicateRows, tt.predicate)
		if !ValuesEqual(view.ColIndex(), tt.cols) {
			t.Errorf("view.ColIndex() => %v, expected %v", view.ColIndex(), tt.cols)
		}
		if !ValuesEqual(df.ColIndex(), originalCols) {
			t.Errorf("view.ColIndex() changed original cols from %v to %v", originalCols, df.ColIndex())
		}
	}
}

func addMultiplyTransform(v []Value) []Value {
	var sum, product int = 0, 1
	for _, val := range v {
		if intval, ok := val.V().(int); ok {
			sum += intval
			product *= intval
		}
	}
	return Ints([]int{sum, product})
}

func TestForEachRow(t *testing.T) {
	tests := []struct {
		transformCols []Value
		out           []Value
		values        [][]Value
	}{
		{Ints([]int{0, 1}), Strings([]string{"sum", "product"}), [][]Value{Ints([]int{3, 2}), Ints([]int{9, 20})}},
		{Ints([]int{2}), Ints([]int{100, 200}), [][]Value{Ints([]int{3, 3}), Ints([]int{6, 6})}},
	}
	originalCols := Ints([]int{0, 1, 2})
	for _, tt := range tests {
		df, err := NewDataFrame(nil, nil, intValues)
		if err != nil {
			t.Errorf("NewDataFrame(nil, nil, %v) => error %v", intValues, err)
			continue
		}
		view := df.ForEachRow(tt.transformCols, tt.out, addMultiplyTransform)
		if !ValuesEqual(view.ColIndex(), tt.out) {
			t.Errorf("view.ColIndex() => %v, expected %v", view.ColIndex(), tt.out)
		}
		if !ValuesEqual(df.ColIndex(), originalCols) {
			t.Errorf("view.ColIndex() changed original cols from %v to %v", originalCols, df.ColIndex())
		}
		if !MatrixEqual(view.Matrix(), tt.values) {
			t.Errorf("view.Matrix() => %v, expected %v", view.Matrix(), tt.values)
		}
	}
}

func TestForEachCol(t *testing.T) {
	tests := []struct {
		transformRows []Value
		out           []Value
		values        [][]Value
	}{
		{Ints([]int{0, 1}), Strings([]string{"sum", "product"}), [][]Value{Ints([]int{5, 7, 9}), Ints([]int{4, 10, 18})}},
		{Ints([]int{1}), Ints([]int{100, 200}), [][]Value{Ints([]int{4, 5, 6}), Ints([]int{4, 5, 6})}},
	}
	originalRows := Ints([]int{0, 1})
	for _, tt := range tests {
		df, err := NewDataFrame(nil, nil, intValues)
		if err != nil {
			t.Errorf("NewDataFrame(nil, nil, %v) => error %v", intValues, err)
			continue
		}
		view := df.ForEachCol(tt.transformRows, tt.out, addMultiplyTransform)
		if !ValuesEqual(view.RowIndex(), tt.out) {
			t.Errorf("view.RowIndex() => %v, expected %v", view.RowIndex(), tt.out)
		}
		if !ValuesEqual(df.RowIndex(), originalRows) {
			t.Errorf("view.RowIndex() changed original rows from %v to %v", originalRows, df.RowIndex())
		}
		if !MatrixEqual(view.Matrix(), tt.values) {
			t.Errorf("view.Matrix() => %v, expected %v", view.Matrix(), tt.values)
		}
	}
}

func TestString(t *testing.T) {
	df, err := NewDataFrame(nil, nil, intValues)
	if err != nil {
		t.Fatalf("NewDataFrame(nil, nil, %v) => error %v", intValues, err)
	}
	if expected := "[[1 2 3] [4 5 6]]"; fmt.Sprintf("%v", df) != expected {
		t.Errorf("fmt.Sprintf(\"%v\", df) => %s, expected %s", fmt.Sprintf("%v", df), expected)
	}
}
