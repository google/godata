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

package row

import (
	"fmt"
	"log"

	"github.com/google/btree"
)

// Index compares rows.
type Index interface {
	// Less returns true if the index is less than the given Index or Row object.
	// If the argument is an Index, then it must be the same underlying type. If
	// the argument is a Row, then it must be indexed by an Index object with the
	// same underlying type.
	Less(item btree.Item) bool
}

// NullIndex represents a missing index. It is considered less than any other
// index.
type NullIndex struct{}

// Less returns true for all arguments.
func (n NullIndex) Less(btree.Item) bool {
	return true
}

// NewIndex returns an index for the given generic values. Returns error if the
// values cannot be automatically converted to an index.
func NewIndex(vals ...interface{}) (Index, error) {
	var indices []Index

	for _, v := range vals {
		switch v := v.(type) {
		default:
			return nil, fmt.Errorf("NewIndex given unsupported value %v", v)
		case int:
			indices = append(indices, IntIndex(v))
		case string:
			indices = append(indices, StringIndex(v))
		}
	}

	if len(indices) == 0 {
		return NullIndex{}, nil
	}
	if len(indices) == 1 {
		return indices[0], nil
	}
	return NewMultiIndex(indices...), nil
}

// MultiIndex compares rows via a dictionary comparison on multiple Index
// objects.
type MultiIndex struct {
	indices []Index
}

// NewMultiIndex returns a MultiIndex for the given indices.
func NewMultiIndex(indices ...Index) MultiIndex {
	return MultiIndex{indices}
}

// Less returns true if the index is less than the given MultiIndex object, or
// if it is less than the given Row object indexed by a MultiIndex. Comparision
// is performed in index order. For example, given MultiIndex objects
// ["a", "b"], ["a", "c"], the first index is less than the second index. A
// MultiIndex with fewer constituent indices is less than a MultiIndex that
// agrees on all existing values. For example, ["a"] is less than ["a", "b"].
func (m MultiIndex) Less(item btree.Item) bool {
	var mi MultiIndex
	switch item := item.(type) {
	default:
		log.Fatal("MultiIndex compared with object that isn't a MultiIndex or Row")
	case MultiIndex:
		mi = item
	case Row:
		return m.Less(item.Index)
	}

	for i, ind := range m.indices {
		// If no previous comparison was definitive, and the current index is
		// populated, then it is a greater MultiIndex value.
		if i >= len(mi.indices) {
			return false
		}
		otherInd := mi.indices[i]
		if ind.Less(otherInd) {
			return true
		}
		if otherInd.Less(ind) {
			return false
		}
	}

	// The two multiindices must be equal for all given elements.
	return len(m.indices) < len(mi.indices)
}

// String formats the MultiIndex as a string.
func (m MultiIndex) String() string {
	return fmt.Sprintf("%v", m.indices)
}

// StringIndex is a string.
type StringIndex string

// Less returns true if the string is less than the given StringIndex or Row
// object. Ordering follows the standard string comparison function.
func (s StringIndex) Less(item btree.Item) bool {
	switch item := item.(type) {
	default:
		log.Fatal("StringIndex compared with object that isn't a StringIndex or Row")
	case StringIndex:
		return string(s) < string(item)
	case Row:
		return s.Less(item.Index)
	}
	return false
}

// IntIndex is an int.
type IntIndex int

// Less returns true if the int is less than the given IntIndex or Row
// object. Ordering follows the standard int comparison function.
func (s IntIndex) Less(item btree.Item) bool {
	switch item := item.(type) {
	default:
		log.Fatal("IntIndex compared with object that isn't a IntIndex or Row")
	case IntIndex:
		return int(s) < int(item)
	case Row:
		return s.Less(item.Index)
	}
	return false
}
