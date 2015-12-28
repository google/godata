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

import "testing"

func TestStringIndex(t *testing.T) {
	tt := []struct {
		s1 string
		s2 string
		lt bool
	}{
		{"abc", "def", true},
		{"def", "abc", false},
		{"abc", "abc", false},
	}

	for _, tt := range tt {
		if lt := StringIndex(tt.s1).Less(StringIndex(tt.s2)); lt != tt.lt {
			t.Errorf("%q < %q = %t; want %t", tt.s1, tt.s2, lt, tt.lt)
		}
	}
}

func TestIntIndex(t *testing.T) {
	tt := []struct {
		s1 int
		s2 int
		lt bool
	}{
		{1, 2, true},
		{2, 1, false},
		{1, 1, false},
	}

	for _, tt := range tt {
		if lt := IntIndex(tt.s1).Less(IntIndex(tt.s2)); lt != tt.lt {
			t.Errorf("%d < %d = %t; want %t", tt.s1, tt.s2, lt, tt.lt)
		}
	}
}

func TestMultiIndex(t *testing.T) {
	tt := []struct {
		s1 string
		s2 string
		i1 int
		i2 int
		lt bool
	}{
		{"a", "b", 1, 2, true},
		{"b", "a", 1, 2, false},
		{"a", "a", 1, 2, true},
		{"a", "a", 2, 1, false},
		{"a", "a", 1, 1, false},
	}

	for _, tt := range tt {
		m1 := NewMultiIndex(StringIndex(tt.s1), IntIndex(tt.i1))
		m2 := NewMultiIndex(StringIndex(tt.s2), IntIndex(tt.i2))
		if lt := m1.Less(m2); lt != tt.lt {
			t.Errorf("(%q, %d) < (%q, %d) = %t; want %t", tt.s1, tt.i1, tt.s2, tt.i2, lt, tt.lt)
		}
	}
}

func TestMultiIndexWithMoreElements(t *testing.T) {
	tt := []struct {
		s1 string
		s2 string
		i1 int
		lt bool
	}{
		{"a", "b", 1, true},
		{"a", "a", 1, false},
		{"b", "a", 1, false},
	}

	for _, tt := range tt {
		m1, err := NewIndex(tt.s1, tt.i1)
		if err != nil {
			t.Errorf("NewIndex(%q, %d): %v", tt.s1, tt.i1, err)
			continue
		}
		m2 := NewMultiIndex(StringIndex(tt.s2))

		if lt := m1.Less(m2); lt != tt.lt {
			t.Errorf("(%q, %d) < %q = %t; want %t", tt.s1, tt.i1, tt.s2, lt, tt.lt)
		}
	}
}

func TestMultiIndexWithFewerElements(t *testing.T) {
	tt := []struct {
		s1 string
		s2 string
		i2 int
		lt bool
	}{
		{"a", "b", 1, true},
		{"a", "a", 1, true},
		{"b", "a", 1, false},
	}

	for _, tt := range tt {
		m1 := NewMultiIndex(StringIndex(tt.s1))
		m2 := NewMultiIndex(StringIndex(tt.s2), IntIndex(tt.i2))

		if lt := m1.Less(m2); lt != tt.lt {
			t.Errorf("%q < (%q, %d) = %t; want %t", tt.s1, tt.s2, tt.i2, lt, tt.lt)
		}
	}
}
