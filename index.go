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

// Index represents an indexed, hierarchical collection of values. The indexed values do not have to
// be unique.
type Index struct {
	// values contains all values in the index.
	values []Value

	// index maps a hash to the position in values.
	index map[string][]int

	// levelIndex maps a given level and hash to the position in values.
	levelIndex []map[string][]int

	// pathIndex maps a chain of value hashes into indices.
	pathIndex *pathIndexNode
}

// NewIndex returns a computed Index on the given set of values. The index is optimized for
// returning indices on exact matches of values, exact matches of values at a given level (for
// MultiValue indices), and partial matches of a given path (for MultiValue indices).
func NewIndex(v []Value) *Index {
	index := &Index{
		values:     copyValues(v),
		index:      make(map[string][]int),
		levelIndex: make([]map[string][]int, 1),
		pathIndex:  &pathIndexNode{},
	}
	for i, val := range v {
		index.index[val.Hash()] = append(index.index[val.Hash()], i)
		if multival, ok := val.(*MultiValue); ok {
			initializeMultiValueIndex(index, multival, i)
		} else {
			initializeSingleValueIndex(index, val, i)
		}
	}
	return index
}

// Values returns the values held by the index.
func (i *Index) Values() []Value {
	return copyValues(i.values)
}

// Lookup returns the indices of the given value, if at least one exists, or nil otherwise.
func (i *Index) Lookup(v *MultiValue) []int {
	if index, ok := i.index[v.Hash()]; ok {
		return copyInts(index)
	}
	return nil
}

// LookupLevel returns the indices of the given value at the given level, if at least one exists, or
// nil otherwise. A level is defined as the nesting depth within a MultiValue. All Value objects
// have an implicitly defined level zero. For MultiValues, the level zero value is the root head, and
// for all other values, the level zero value is itself.
func (i *Index) LookupLevel(level int, v Value) []int {
	if level >= 0 && level < len(i.levelIndex) {
		if index, ok := i.levelIndex[level][v.Hash()]; ok {
			return copyInts(index)
		}
	}
	return nil
}

// LookupPath returns the indices starting with the given path, if at least one exists, or nil
// otherwise. For example, given the values {"foo", "bar", "baz"], this method will return the
// indices of all MultiValues whose head values are "foo", "bar", and "baz".
func (i *Index) LookupPath(path []Value) []int {
	currentPathIndex := i.pathIndex
	for _, p := range path {
		if next, ok := currentPathIndex.nextLevel[p.Hash()]; ok {
			currentPathIndex = next
		} else {
			return nil
		}
	}
	return copyInts(currentPathIndex.index)
}

// pathIndexNode represents one node in the internal path index. Each node includes all indices that
// match the path represented by that node. Each node also includes a reference to all subsequent
// nodes, indexed by the next node value.
type pathIndexNode struct {
	index     []int
	nextLevel map[string]*pathIndexNode
}

func initializeSingleValueIndex(index *Index, val Value, pos int) {
	if index.levelIndex[0] == nil {
		index.levelIndex[0] = make(map[string][]int)
	}
	if index.pathIndex.nextLevel == nil {
		index.pathIndex.nextLevel = make(map[string]*pathIndexNode)
	}
	index.levelIndex[0][val.Hash()] = append(index.levelIndex[0][val.Hash()], pos)
	index.pathIndex.nextLevel[val.Hash()].index = append(index.pathIndex.nextLevel[val.Hash()].index, pos)
}

func initializeMultiValueIndex(index *Index, multival *MultiValue, pos int) {
	for level, pi := 0, index.pathIndex; ; level++ {
		if multival.Head != nil {
			initializeLevelIndex(index, level, multival.Head, pos)
			initializePathIndex(index, pi, multival.Head, pos)
			if multival.Tail == nil {
				break
			} else {
				multival = multival.Tail
				pi = pi.nextLevel[multival.Head.Hash()]
			}
		}
	}
}

func initializeLevelIndex(index *Index, level int, value Value, pos int) {
	if len(index.levelIndex) <= level {
		grownLevelIndex := make([]map[string][]int, 2+2*level)
		copy(grownLevelIndex, index.levelIndex)
		index.levelIndex = grownLevelIndex
	}
	if index.levelIndex[level] == nil {
		index.levelIndex[level] = make(map[string][]int)
	}
	index.levelIndex[level][value.Hash()] = append(index.levelIndex[level][value.Hash()], pos)
}

func initializePathIndex(index *Index, parent *pathIndexNode, value Value, pos int) {
	if parent.nextLevel == nil {
		parent.nextLevel = make(map[string]*pathIndexNode)
	}
	if parent.nextLevel[value.Hash()] == nil {
		parent.nextLevel[value.Hash()] = &pathIndexNode{}
	}
	parent.nextLevel[value.Hash()].index = append(parent.nextLevel[value.Hash()].index, pos)
}

func copyValues(v []Value) []Value {
	r := make([]Value, len(v))
	copy(r, v)
	return r
}

func copyInts(v []int) []int {
	r := make([]int, len(v))
	copy(r, v)
	return r
}
