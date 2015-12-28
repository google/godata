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
	"log"

	"github.com/google/godata/row"
)

func Example_putAndGet() {
	frame := NewFrame(row.NewColumnIndexer("index1", "index2"))
	frame.Put(row.Of("index1", 123, "index2", "foo", "data1", "hello", "data2", "world"))
	frame.Put(row.Of("index1", 456, "index2", "bar", "data3", "something"))

	row, err := frame.Get(row.Of("index1", 123, "index2", "foo"))
	if err != nil {
		log.Fatalf("Get: %v", err)
	}
	fmt.Print(row["data1"])
	// Output: hello
}

func Example_joined() {
	f1 := NewFrame(row.NewColumnIndexer("index"))
	f2 := NewFrame(row.NewColumnIndexer("index"))

	// f1 and f2 share an index, so the values will be joined together.
	f1.Put(row.Of("index", 0, "value", "foo"))
	f2.Put(row.Of("index", 0, "value", "bar"))

	// f1 and f2 have values at different indices, so index 1 will only contain a
	// value from f1, and index 2 will only contain a value from f2.
	f1.Put(row.Of("index", 1, "value", "something"))
	f2.Put(row.Of("index", 2, "value", "else"))

	joined, err := f1.Joined(f2)
	if err != nil {
		log.Fatalf("Joined: %v", err)
	}
	common, err := joined.Get(row.Of("index", 0))
	if err != nil {
		log.Fatalf("Get: %v", err)
	}
	fmt.Println("index:", common["index"], "value:", common["value"])

	leftOnly, err := joined.Get(row.Of("index", 1))
	if err != nil {
		log.Fatalf("Get: %v", err)
	}
	fmt.Println("index:", leftOnly["index"], "value:", leftOnly["value"])

	rightOnly, err := joined.Get(row.Of("index", 2))
	if err != nil {
		log.Fatalf("Get: %v", err)
	}
	fmt.Println("index:", rightOnly["index"], "value:", rightOnly["value"])

	// Output:
	// index: JoinResult{Left: 0, Right: 0} value: JoinResult{Left: foo, Right: bar}
	// index: JoinResult{Left: 1} value: JoinResult{Left: something}
	// index: JoinResult{Right: 2} value: JoinResult{Right: else}
}
