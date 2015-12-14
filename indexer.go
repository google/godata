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
	"fmt"
	"reflect"
)

// Indexer returns an Index for a given row of data.
type Indexer interface {
	Index(data RowData) (Index, error)
}

// ColumnIndexer indexes the given column names using the default indexing
// behavior of NewIndex. If a column does not exist for a given row, then the
// column indexer fails. The indexer keeps track of the types associated with
// each column, and fails if the underlying type changes for a given column.
//
// TODO: ColumnIndexer is currently not threadsafe.
type ColumnIndexer struct {
	columns []string
	types   map[string]reflect.Type
}

// NewColumnIndexer returns a ColumnIndexer for the given columns.
func NewColumnIndexer(columns ...string) *ColumnIndexer {
	return &ColumnIndexer{
		columns: columns,
		types:   make(map[string]reflect.Type),
	}
}

// Index returns the index value for the given row. Returns error if the row
// doesn't contain all necessary columns, or if the row contains values that
// cannot be automatically converted into indices.
func (c ColumnIndexer) Index(data RowData) (Index, error) {
	var vals []interface{}
	for _, col := range c.columns {
		val, ok := data[col]
		if !ok {
			return nil, fmt.Errorf("Index(%v) failed; missing %q", data, col)
		}
		if typ, ok := c.types[col]; ok {
			if newType := reflect.TypeOf(val); typ != newType {
				return nil, fmt.Errorf("Index(%v) failed; %q has type %v but saw %v of type %v", data, col, typ, val, newType)
			} else if newType != nil {
				c.types[col] = newType
			}
		}
		vals = append(vals, val)
	}
	return NewIndex(vals...)
}
