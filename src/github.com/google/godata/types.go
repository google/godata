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
	"encoding/base64"
	"encoding/gob"
	"fmt"
	"log"
)

// IntValue represents an int value.
type IntValue struct {
	value int
	hash  string
}

// Int returns a Value for the given integer.
func Int(i int) Value {
	return &IntValue{
		value: i,
	}
}

// Ints returns Values for the given integers.
func Ints(input []int) []Value {
	values := make([]Value, 0, len(input))
	for _, v := range input {
		values = append(values, Int(v))
	}
	return values
}

// Hash returns the hash for the int value.
func (i *IntValue) Hash() string {
	return cachedHash(&i.hash, i.value)
}

// V returns the int value.
func (i IntValue) V() interface{} {
	return i.value
}

// String returns the string value.
func (i IntValue) String() string {
	return fmt.Sprintf("%d", i.value)
}

// StringValue represents a string value.
type StringValue struct {
	Value
	value string
	hash  string
}

// String returns a Value for the given string.
func String(str string) Value {
	return &StringValue{
		value: str,
	}
}

// Strings returns Values for the given strings.
func Strings(input []string) []Value {
	values := make([]Value, 0, len(input))
	for _, v := range input {
		values = append(values, String(v))
	}
	return values
}

// Hash returns the hash for the string value.
func (s *StringValue) Hash() string {
	return cachedHash(&s.hash, s.value)
}

// V returns the string value.
func (s StringValue) V() interface{} {
	return s.value
}

// String returns the string value.
func (s StringValue) String() string {
	return s.value
}

// Float64Value represents a float64 value.
type Float64Value struct {
	value float64
	hash  string
}

// Float64 returns a Value for the given float64.
func Float64(f float64) Value {
	return &Float64Value{
		value: f,
	}
}

// Float64s returns Values for the given float64s.
func Float64s(input []float64) []Value {
	values := make([]Value, 0, len(input))
	for _, v := range input {
		values = append(values, Float64(v))
	}
	return values
}

// Hash returns the hash for the float64 value.
func (f *Float64Value) Hash() string {
	return cachedHash(&f.hash, f.value)
}

// V returns the float64 value.
func (f Float64Value) V() interface{} {
	return f.value
}

// String returns the string value.
func (f Float64Value) String() string {
	return fmt.Sprintf("%f", f.value)
}

func cachedHash(hash *string, v interface{}) string {
	if *hash == "" {
		val, err := encodeWithoutSlash(v)
		if err != nil {
			log.Fatalf("Unable to compute hash of value %v: %v", v, err)
		}
		*hash = fmt.Sprintf("%T(%s)", v, *val)
	}
	return *hash
}

func encodeWithoutSlash(val interface{}) (*string, error) {
	var buf bytes.Buffer
	enc := gob.NewEncoder(&buf)
	err := enc.Encode(val)
	if err != nil {
		return nil, err
	}
	encoded := base64.URLEncoding.EncodeToString(buf.Bytes())
	return &encoded, nil
}
