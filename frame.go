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
	"fmt"

	"github.com/google/btree"
	"github.com/google/godata/group"
	"github.com/google/godata/row"
)

// Frame represents multiple rows and multiple columns of data.
type Frame struct {
	bt *btree.BTree

	indexer row.Indexer
}

// NewFrame returns a Frame for the given indexer.
func NewFrame(indexer row.Indexer) *Frame {
	return &Frame{
		bt:      btree.New(2),
		indexer: indexer,
	}
}

// Put inserts the data into the frame, replacing and returning the existing
// data if an entry already exists. Returns error if the data cannot be
// indexed.
func (f *Frame) Put(data row.Data) (row.Data, error) {
	index, err := f.indexer.Index(data)
	if err != nil {
		return nil, fmt.Errorf("AddRow: %v", err)
	}

	got := f.bt.ReplaceOrInsert(row.Row{
		Index: index,
		Data:  data,
	})

	if got == nil {
		return nil, nil
	}

	return got.(row.Row).Data, nil
}

// Get returns the data for the given key. Returns error if the given key is
// invalid. Returns nil if there is no data for the given key.
func (f *Frame) Get(key row.Data) (row.Data, error) {
	index, err := f.indexer.Index(key)
	if err != nil {
		return nil, err
	}

	got := f.bt.Get(index)
	if got == nil {
		return nil, nil
	}

	return got.(row.Row).Data, nil
}

// Pop returns the data for the given key and deletes it from the Frame.
// Returns error if the given key is invalid. Returns nil if there is no data
// for the given key.
func (f *Frame) Pop(key row.Data) (row.Data, error) {
	index, err := f.indexer.Index(key)
	if err != nil {
		return nil, err
	}

	got := f.bt.Delete(index)
	if got == nil {
		return nil, nil
	}
	return got.(row.Row).Data, nil
}

// rangeOptions represents a begin and end point for range functions.
type rangeOptions struct {
	greaterOrEqual row.Data
	lessThan       row.Data
}

// rangeArg mutate a rangeOptions based on a given argument.
type rangeArg func(*rangeOptions)

// rangeArgsToOptions converts the given rangeArgs into an options struct.
func rangeArgsToOptions(args []rangeArg) *rangeOptions {
	var opts rangeOptions
	for _, a := range args {
		a(&opts)
	}
	return &opts
}

// GreaterOrEqual returns a range option that filters on rows greater than or
// equal to the given value.
func GreaterOrEqual(val row.Data) rangeArg {
	return func(opts *rangeOptions) {
		opts.greaterOrEqual = val
	}
}

// LessThan returns a range option that filters on rows less than or equal to
// the given value.
func LessThan(val row.Data) rangeArg {
	return func(opts *rangeOptions) {
		opts.lessThan = val
	}
}

// RowAction performs an operation on the given row and optionally returns a
// value. RowAction must not mutate the Data.
type RowAction func(row.Data) (interface{}, error)

// rowAction performs an operation on the given Row. Actions may mutate the
// data, but must also re-insert the row into the btree to maintain
// synchronization of the index.
type rowAction func(row.Row) (interface{}, error)

// forRange performs an action for a given key range and returns the array of
// results, one for each row.
func (f *Frame) forRange(opts *rangeOptions, action rowAction) ([]interface{}, error) {
	var (
		returnError  error
		returnValues []interface{}
	)
	iterator := func(item btree.Item) bool {
		val, err := action(item.(row.Row))
		if err != nil {
			returnError = err
			return false
		}
		returnValues = append(returnValues, val)
		return true
	}

	if opts.lessThan == nil && opts.greaterOrEqual == nil {
		f.bt.Ascend(iterator)
	} else if opts.lessThan == nil {
		pivot, err := f.indexer.Index(opts.greaterOrEqual)
		if err != nil {
			return nil, err
		}
		f.bt.AscendGreaterOrEqual(pivot, iterator)
	} else if opts.greaterOrEqual == nil {
		pivot, err := f.indexer.Index(opts.lessThan)
		if err != nil {
			return nil, err
		}
		f.bt.AscendLessThan(pivot, iterator)
	} else {
		begin, err := f.indexer.Index(opts.greaterOrEqual)
		if err != nil {
			return nil, err
		}
		end, err := f.indexer.Index(opts.lessThan)
		if err != nil {
			return nil, err
		}
		f.bt.AscendRange(begin, end, iterator)
	}

	return returnValues, returnError
}

// String returns the string representation of the Frame.
//
// TODO: The output is ugly and potentially very long. Refactor the forRange to
// take a limit, and then print out a partial representation of the Frame if
// there are too many rows.
func (f *Frame) String() string {
	rows, err := f.GetRange()
	if err != nil {
		return fmt.Sprintf("Frame.String: %v", err)
	}
	var buf bytes.Buffer
	for i, r := range rows {
		buf.WriteString(fmt.Sprintf("%d: %v\n", i, r))
	}
	return buf.String()
}

// GetRange returns a list of all values in the given range. See GreaterOrEqual
// and LessThan. If no range is given, then this function returns all rows. If
// only a begin range is given, then this function returns all rows beginning
// with the given value. If only an end range is given, then this function
// returns all rows up to the given value.
func (f *Frame) GetRange(args ...rangeArg) ([]row.Data, error) {
	opts := rangeArgsToOptions(args)
	rows, err := f.forRange(opts, func(row row.Row) (interface{}, error) {
		return row.Data, nil
	})
	if err != nil {
		return nil, err
	}

	var castRows []row.Data
	for _, r := range rows {
		castRows = append(castRows, r.(row.Data))
	}
	return castRows, nil
}

// PopRange returns a list of all values in the given range and deletes them
// from the Frame. See GetRange for details on the arguments.
func (f *Frame) PopRange(args ...rangeArg) ([]row.Data, error) {
	opts := rangeArgsToOptions(args)
	rows, err := f.forRange(opts, func(row row.Row) (interface{}, error) {
		return row, nil
	})
	if err != nil {
		return nil, err
	}

	var (
		indices []row.Index
		data    []row.Data
	)
	for _, r := range rows {
		indices = append(indices, r.(row.Row).Index)
		data = append(data, r.(row.Row).Data)
	}

	for _, i := range indices {
		f.bt.Delete(i)
	}

	return data, nil
}

// Joined returns a new Frame object that contains the joined contents of the
// two frames. The indices of the frames must be compatible. The resulting
// Frame contains one row for each key in the union of keys for the left and
// right frames. The Data contains a JoinResult for each column of data,
// where Left is populated with the left side contents, and Right is populated
// with the right side contents. Left and Right are nil if they don't exist in
// the left and right sides.
func (f *Frame) Joined(frame *Frame) (*Frame, error) {
	fr := NewFrame(JoinResultIndexer{f.indexer})

	// Add all left rows.
	all, err := f.GetRange()
	if err != nil {
		return nil, err
	}
	for _, row := range all {
		// Convert each column to a JoinResult.
		projected := make(map[string]interface{})
		for col, val := range row {
			projected[col] = &JoinResult{Left: val}
		}

		_, err := fr.Put(projected)
		if err != nil {
			return nil, err
		}
	}

	// Add all right rows.
	all, err = frame.GetRange()
	if err != nil {
		return nil, err
	}
	for _, row := range all {
		projected, err := fr.Get(row)
		if err != nil {
			return nil, err
		}
		if projected == nil {
			projected = make(map[string]interface{})
		}

		// Add Right to each column JoinResult, or add a JoinResult if the column doesn't exist yet.
		for col, val := range row {
			joinResult, ok := projected[col]
			if ok {
				jd, ok := joinResult.(*JoinResult)
				if !ok {
					return nil, fmt.Errorf("Frame: column %q in %v is not a JoinResult", col, joinResult)
				}
				jd.Right = val
			} else {
				projected[col] = &JoinResult{Right: val}
			}
		}
		fr.Put(projected)
		if err != nil {
			return nil, err
		}
	}

	return fr, nil
}

// WithIndexer returns a new Frame object with the same underlying data indexed
// by a new indexer. Returns error if the data cannot be indexed by the new
// indexer. Note that mutating rows in the returned Frame will also mutate the
// rows in the existing Frame. However, adding to or deleting rows from the
// returned Frame will not add to and delete from the existing Frame.
// WithIndexer assumes that the given Indexer gives each existing row a unique
// Index. If rows share an index, then one of the rows will be dropped. The
// dropped row is not defined by the API, and is subject to change.
func (f *Frame) WithIndexer(indexer row.Indexer) (*Frame, error) {
	var returnErr error

	nf := NewFrame(indexer)
	iter := func(item btree.Item) bool {
		_, err := nf.Put(item.(row.Row).Data)
		if err != nil {
			returnErr = err
			return false
		}
		return true
	}

	f.bt.Ascend(iter)

	return nf, returnErr
}

// GroupBy returns a new Frame object containing rows grouped by the given
// indexer. Each index corresponds to a row containing a single column
// group.Column, which is a slice of rows with that index.
func (f *Frame) GroupBy(indexer row.Indexer) (*Frame, error) {
	var returnErr error
	nf := NewFrame(group.Indexer{indexer})
	iter := func(item btree.Item) bool {
		r := item.(row.Row)
		index, err := indexer.Index(r.Data)
		if err != nil {
			returnErr = err
			return false
		}
		existing := nf.bt.Get(index)
		var existingRow row.Row
		if existing != nil {
			existingRow = existing.(row.Row)
		} else {
			existingRow = row.Row{
				Index: index,
				Data:  map[string]interface{}{group.Column: group.Group(nil)},
			}
		}

		// Conversion errors imply the group is nil.
		existingGroup, _ := existingRow.Data[group.Column].(group.Group)

		existingRow.Data[group.Column] = append(existingGroup, r.Data)
		nf.bt.ReplaceOrInsert(existingRow)
		return true
	}

	f.bt.Ascend(iter)

	return nf, returnErr
}
