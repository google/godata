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
	"bytes"
	"errors"
	"fmt"
	"log"
)

// DataFrame implements a View with an in memory two dimensional array. The DataFrame object is not
// threadsafe.
type DataFrame struct {
	// rows are the row labels.
	rows []Value

	// cols are the col labels.
	cols []Value

	// values contains the DataFrame values.
	values [][]Value

	// rowLabelIndex maps a value hash to an index.
	rowLabelIndex map[string]int

	// colLabelIndex maps a value hash to an index.
	colLabelIndex map[string]int

	// rowIndices contains the row indices that are visible in this view.
	rowIndices []int

	// colIndices returns the col indices that are visible in this view.
	colIndices []int
}

// NewDataFrame returns a View backed by a DataFrame. The view has the given row labels, column
// labels, and values. Values are specified by row and then by column. For example, values[0][1] is
// the first row, second column. If the rows are nil, use integer indexing for rows; same for cols.
func NewDataFrame(rows []Value, cols []Value, values [][]Value) (View, error) {
	if rows == nil {
		rows = Ints(intRange(len(values)))
	}
	if len(rows) != len(values) {
		return nil, errors.New("rows and values have inconsistent dimensions")
	}
	for _, row := range values {
		if cols == nil {
			cols = Ints(intRange(len(row)))
		}
		if len(row) != len(cols) {
			return nil, errors.New("cols and values have inconsistent dimensions")
		}
	}
	df := &DataFrame{
		rows:   rows,
		cols:   cols,
		values: values,
	}
	df.rowLabelIndex = make(map[string]int)
	df.colLabelIndex = make(map[string]int)
	df.rowIndices = intRange(len(rows))
	df.colIndices = intRange(len(cols))

	for i, r := range rows {
		if _, ok := df.rowLabelIndex[r.Hash()]; ok {
			return nil, fmt.Errorf("row labels are not unique: found value %v with hash %s", r.V(), r.Hash())
		}
		df.rowLabelIndex[r.Hash()] = i
	}
	for i, c := range cols {
		if _, ok := df.colLabelIndex[c.Hash()]; ok {
			return nil, fmt.Errorf("col labels are not unique: found value %v with hash %s", c.V(), c.Hash())
		}
		df.colLabelIndex[c.Hash()] = i
	}
	return df, nil
}

// NewDataFrameFromInts returns a View backed by a DataFrame that wraps around the given values.
// See NewDataFrame for more details.
func NewDataFrameFromInts(rows []Value, cols []Value, values [][]int) (View, error) {
	intValues := make([][]Value, len(values))
	for i, row := range values {
		intValues[i] = make([]Value, 0, len(row))
		for _, v := range row {
			intValues[i] = append(intValues[i], Int(v))
		}
	}
	return NewDataFrame(rows, cols, intValues)
}

// Cols returns a view containing the given columns.
func (d *DataFrame) Cols(v []Value) View {
	colIndices := valueIndicesInIndex(v, d.colLabelIndex)
	return d.copyWithColIndices(colIndices)
}

// Rows returns a view containing the given rows.
func (d *DataFrame) Rows(v []Value) View {
	rowIndices := valueIndicesInIndex(v, d.rowLabelIndex)
	return d.copyWithRowIndices(rowIndices)
}

// ColAt returns the values in the given column.
func (d *DataFrame) ColAt(i int) []Value {
	values := make([]Value, 0, len(d.rowIndices))
	for _, r := range d.rowIndices {
		values = append(values, d.values[r][d.colIndices[i]])
	}
	return values
}

// RowAt returns the values in the given row.
func (d *DataFrame) RowAt(i int) []Value {
	values := make([]Value, 0, len(d.colIndices))
	for _, c := range d.colIndices {
		values = append(values, d.values[d.rowIndices[i]][c])
	}
	return values
}

// NumRows returns the number of rows.
func (d *DataFrame) NumRows() int {
	return len(d.rowIndices)
}

// NumCols returns the number of columns.
func (d *DataFrame) NumCols() int {
	return len(d.colIndices)
}

// ColIndex returns the column index.
func (d *DataFrame) ColIndex() []Value {
	return valuesForIndices(d.cols, d.colIndices)
}

// RowIndex returns the row index.
func (d *DataFrame) RowIndex() []Value {
	return valuesForIndices(d.rows, d.rowIndices)
}

// WhereColIndex returns a view with the columns whose indices satisfy the predicate.
func (d *DataFrame) WhereColIndex(p Predicate) View {
	subset := indexSubsetWhere(d.cols, d.colIndices, p)
	return d.copyWithColIndices(subset)
}

// WhereRowIndex returns a view with the rows whose indices satisfy the predicate.
func (d *DataFrame) WhereRowIndex(p Predicate) View {
	subset := indexSubsetWhere(d.rows, d.rowIndices, p)
	return d.copyWithRowIndices(subset)
}

// RowsWhereCols returns a view containing the rows in which the given columns satisfy the predicate.
func (d *DataFrame) RowsWhereCols(cols []Value, p Predicate) View {
	columnIndices := valueIndicesInIndex(cols, d.colLabelIndex)
	rowIndices := make([]int, 0, len(d.rowIndices))
	for _, r := range d.rowIndices {
		columnValues := valuesForIndices(d.values[r], columnIndices)
		if p(columnValues) {
			rowIndices = append(rowIndices, r)
		}
	}
	return d.copyWithRowIndices(rowIndices)
}

// ColsWhereRows returns a view containing the columns in which the given rows satisfy the predicate.
func (d *DataFrame) ColsWhereRows(rows []Value, p Predicate) View {
	rowIndices := valueIndicesInIndex(rows, d.rowLabelIndex)
	colIndices := make([]int, 0, len(d.colIndices))
	for _, c := range d.colIndices {
		rowValues := make([]Value, 0, len(rowIndices))
		for i := range rowIndices {
			rowValues = append(rowValues, d.values[rowIndices[i]][c])
		}
		if p(rowValues) {
			colIndices = append(colIndices, c)
		}
	}
	return d.copyWithColIndices(colIndices)
}

// ForEachRow returns a view containing all rows, where the given columns are transformed into the
// given output columns.
func (d *DataFrame) ForEachRow(cols []Value, out []Value, t Transform) View {
	colView := d.Cols(cols)
	transformed := make([][]Value, 0, colView.NumRows())
	for i := 0; i < colView.NumRows(); i++ {
		output := t(colView.RowAt(i))
		if len(out) != len(output) {
			return nil
		}
		transformed = append(transformed, output)
	}
	df := *d
	df.colIndices = intRange(len(out))
	df.cols = out
	df.values = transformed
	return &df
}

// ForEachCol returns a view containing all cols, where the given rows are transformed into the
// given output rows.
func (d *DataFrame) ForEachCol(rows []Value, out []Value, t Transform) View {
	rowView := d.Rows(rows)
	transformed := make([][]Value, len(out))
	for i := range transformed {
		transformed[i] = make([]Value, rowView.NumCols())
	}
	for i := 0; i < rowView.NumCols(); i++ {
		output := t(rowView.ColAt(i))
		if len(out) != len(output) {
			return nil
		}
		for j, v := range output {
			transformed[j][i] = v
		}
	}
	df := *d
	df.rowIndices = intRange(len(out))
	df.rows = out
	df.values = transformed
	return &df
}

// String returns a string representation of the DataFrame. The string representation is not
// guaranteed to be stable across different versions of the code. Do not rely on the output format.
func (d *DataFrame) String() string {
	return fmt.Sprintf("%v", d.values)
}

// Matrix returns a copy of the internal value matrix.
func (d *DataFrame) Matrix() [][]Value {
	r := make([][]Value, 0, len(d.values))
	for _, s := range d.values {
		row := make([]Value, len(s))
		copy(row, s)
		r = append(r, row)
	}
	return r
}

// copyWithRowIndices returns a copy of the DataFrame with the row indices set to the given values.
func (d *DataFrame) copyWithRowIndices(i []int) *DataFrame {
	df := *d
	df.rowIndices = i
	return &df
}

// copyWithColIndices returns a copy of the DataFrame with the col indices set to the given values.
func (d *DataFrame) copyWithColIndices(i []int) *DataFrame {
	df := *d
	df.colIndices = i
	return &df
}

func writeStringOrDie(buf *bytes.Buffer, str string) {
	_, err := buf.WriteString(str)
	if err != nil {
		log.Fatal(err)
	}
}

func valuesForIndices(v []Value, indices []int) []Value {
	values := make([]Value, 0, len(indices))
	for _, i := range indices {
		values = append(values, v[i])
	}
	return values
}

func indexSubsetWhere(v []Value, indices []int, p Predicate) []int {
	subset := make([]int, 0, len(indices))
	for _, i := range indices {
		if p(v[i : i+1]) {
			subset = append(subset, i)
		}
	}
	return subset
}

func valueIndicesInIndex(v []Value, m map[string]int) []int {
	indices := make([]int, 0, len(v))
	for _, value := range v {
		if index, ok := m[value.Hash()]; ok {
			indices = append(indices, index)
		}
	}
	return indices
}

func intRange(max int) []int {
	r := make([]int, max)
	for i := range r {
		r[i] = i
	}
	return r
}
