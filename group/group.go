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

// Package group defines utilities for aggregating and slicing Frames. A group
// is simply a Frame where each index corresponds to a single column that
// contains a list of rows, each with the same index.
package group

import row "github.com/google/godata/row"

// Column is the Frame column in which the group is stored.
const Column = "Group"

// Group is a slice of Data objects, stored in the column defined by Column.
type Group []row.Data

// New returns a Group containing the given Data objects.
func New(data ...row.Data) Group {
	var g Group
	for _, d := range data {
		g = append(g, d)
	}
	return g
}
