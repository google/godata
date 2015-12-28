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
	"reflect"

	"github.com/google/godata/group"
	"github.com/google/godata/row"

	"testing"
)

func TestPutAndGetFrame(t *testing.T) {
	f := NewFrame(row.NewColumnIndexer("i1", "i2"))
	r1, err := f.Put(row.Of("i1", 1, "i2", 1, "data", "foo"))
	if err != nil {
		t.Fatalf("Put: %v", err)
	}
	if r1 != nil {
		t.Fatalf("Put = %v; want nil", r1)
	}
	r2, err := f.Put(row.Of("i1", 1, "i2", 2, "data", "bar"))
	if err != nil {
		t.Fatalf("Put: %v", err)
	}
	if r2 != nil {
		t.Fatalf("Put = %v; want nil", r2)
	}

	r1, err = f.Get(row.Of("i1", 1, "i2", 1))

	if got, want := r1["data"], "foo"; got != want {
		t.Errorf("r1[%q] = %q; want %q", "data", got, want)
	}

	r2, err = f.Get(row.Of("i1", 1, "i2", 2))
	if got, want := r2["data"], "bar"; got != want {
		t.Errorf("r2[%q] = %q; want %q", "data", got, want)
	}
}

func TestJoined(t *testing.T) {
	f1 := NewFrame(row.NewColumnIndexer("i1", "i2"))
	f2 := NewFrame(row.NewColumnIndexer("i1", "i2"))

	f1.Put(row.Of("i1", 1, "i2", 1, "data", "foo"))
	f2.Put(row.Of("i1", 1, "i2", 1, "data", "bar"))

	f1.Put(row.Of("i1", 1, "i2", 2, "data", "hello"))
	f2.Put(row.Of("i1", 1, "i2", 2, "data", "world"))

	f1.Put(row.Of("i1", 2, "i2", 1, "data", "pikachu"))
	f2.Put(row.Of("i1", 2, "i2", 1, "data", "raichu"))

	f1.Put(row.Of("i1", 0, "i2", 1, "data", "nyc"))
	f2.Put(row.Of("i1", 1, "i2", 0, "data", "sfo"))

	f3, err := f1.Joined(f2)
	if err != nil {
		t.Fatalf("Joined: %v", err)
	}
	tt := []struct {
		i1    int
		i2    int
		left  string
		right string
	}{
		{1, 1, "foo", "bar"},
		{1, 2, "hello", "world"},
		{2, 1, "pikachu", "raichu"},
		{0, 1, "nyc", ""},
		{1, 0, "", "sfo"},
	}

	for _, tt := range tt {
		got, err := f3.Get(row.Of("i1", tt.i1, "i2", tt.i2))
		if err != nil {
			t.Errorf("Get: %v", err)
			continue
		}
		jr, ok := got["data"].(*JoinResult)
		if !ok {
			t.Errorf("Get = %v; want JoinResult", got)
			continue
		}
		if (jr.Left == nil) != (tt.left == "") && jr.Left != tt.left {
			t.Errorf("Get = %v; want left = %q", jr, tt.left)
		}
		if (jr.Right == nil) != (tt.right == "") && jr.Right != tt.right {
			t.Errorf("Get = %v; want right = %q", jr, tt.right)
		}
	}
}

func TestGroupBy(t *testing.T) {
	f := NewFrame(row.NewColumnIndexer("i1", "i2"))
	f.Put(row.Of("i1", 0, "i2", 0))
	f.Put(row.Of("i1", 0, "i2", 1))
	f.Put(row.Of("i1", 1, "i2", 0))
	f.Put(row.Of("i1", 1, "i2", 1))

	grouped, err := f.GroupBy(row.NewColumnIndexer("i1"))
	got, err := grouped.Get(row.Of(group.Column, group.New(row.Of("i1", 0))))
	if err != nil {
		t.Fatalf("Get: %v", err)
	}
	col, ok := got[group.Column]
	if !ok {
		t.Fatalf("Get: %v; need column %q", got, group.Column)
	}
	grp, ok := col.(group.Group)
	if !ok {
		t.Fatalf("Get: %v; column %q is not a Group", got, group.Column)
	}
	if len(grp) != 2 {
		t.Fatalf("Group = %v; want 2 elements", grp)
	}
	if want := row.Of("i1", 0, "i2", 0); !reflect.DeepEqual(grp[0], want) {
		t.Fatalf("Group = %v; want %v", grp[0], want)
	}
	if want := row.Of("i1", 0, "i2", 1); !reflect.DeepEqual(grp[1], want) {
		t.Fatalf("Group = %v; want %v", grp[1], want)
	}

}
