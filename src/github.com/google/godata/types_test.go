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

import "testing"

func TestInts(t *testing.T) {
	tests := []struct {
		obj       []Value
		value     []int
		hashEqual bool
	}{
		{[]Value{Int(2), Int(3)}, []int{2, 3}, false},
		{[]Value{Int(4), Int(4)}, []int{4, 4}, true},
		{Ints([]int{4, 4}), []int{4, 4}, true},
		{Ints([]int{5, 6}), []int{5, 6}, false},
	}
	for _, tt := range tests {
		for i, v := range tt.obj {
			if v.V() != tt.value[i] {
				t.Errorf("v.V() for value %v => %v, expected %v", v, v.V(), tt.value[i])
			}
		}
		hashes := make([]string, 0)
		allEqual := true
		for i, v := range tt.obj {
			hashes = append(hashes, v.Hash())
			if len(hashes) >= 2 && hashes[i] != hashes[i-1] {
				allEqual = false
			}
		}
		if allEqual != tt.hashEqual {
			t.Errorf("Values %v produced v.Hash() output => %v with allEqual = %v, expected %v", tt.value, hashes, allEqual, tt.hashEqual)
		}
	}
}

func TestStrings(t *testing.T) {
	tests := []struct {
		obj       []Value
		value     []string
		hashEqual bool
	}{
		{[]Value{String("2"), String("3")}, []string{"2", "3"}, false},
		{[]Value{String("4"), String("4")}, []string{"4", "4"}, true},
		{Strings([]string{"4", "4"}), []string{"4", "4"}, true},
		{Strings([]string{"5", "6"}), []string{"5", "6"}, false},
	}
	for _, tt := range tests {
		for i, v := range tt.obj {
			if v.V() != tt.value[i] {
				t.Errorf("v.V() for value %v => %v, expected %v", v, v.V(), tt.value[i])
			}
		}
		hashes := make([]string, 0)
		allEqual := true
		for i, v := range tt.obj {
			hashes = append(hashes, v.Hash())
			if len(hashes) >= 2 && hashes[i] != hashes[i-1] {
				allEqual = false
			}
		}
		if allEqual != tt.hashEqual {
			t.Errorf("Values %v produced v.Hash() output => %v with allEqual = %v, expected %v", tt.value, hashes, allEqual, tt.hashEqual)
		}
	}
}

func TestFloat64s(t *testing.T) {
	tests := []struct {
		obj       []Value
		value     []float64
		hashEqual bool
	}{
		{[]Value{Float64(2), Float64(3)}, []float64{2, 3}, false},
		{[]Value{Float64(4), Float64(4)}, []float64{4, 4}, true},
		{Float64s([]float64{4, 4}), []float64{4, 4}, true},
		{Float64s([]float64{5, 6}), []float64{5, 6}, false},
	}
	for _, tt := range tests {
		for i, v := range tt.obj {
			if v.V() != tt.value[i] {
				t.Errorf("v.V() for value %v => %v, expected %v", v, v.V(), tt.value[i])
			}
		}
		hashes := make([]string, 0)
		allEqual := true
		for i, v := range tt.obj {
			hashes = append(hashes, v.Hash())
			if len(hashes) >= 2 && hashes[i] != hashes[i-1] {
				allEqual = false
			}
		}
		if allEqual != tt.hashEqual {
			t.Errorf("Values %v produced v.Hash() output => %v with allEqual = %v, expected %v", tt.value, hashes, allEqual, tt.hashEqual)
		}
	}
}
