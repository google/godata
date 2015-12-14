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

func TestPutAndGetFrame(t *testing.T) {
	f := NewFrame(NewColumnIndexer("i1", "i2"))
	r1, err := f.Put(map[string]interface{}{"i1": 1, "i2": 1, "data": "foo"})
	if err != nil {
		t.Fatalf("Put: %v", err)
	}
	if r1 != nil {
		t.Fatalf("Put = %v; want nil", r1)
	}
	r2, err := f.Put((map[string]interface{}{"i1": 1, "i2": 2, "data": "bar"}))
	if err != nil {
		t.Fatalf("Put: %v", err)
	}
	if r2 != nil {
		t.Fatalf("Put = %v; want nil", r2)
	}

	r1, err = f.Get(map[string]interface{}{"i1": 1, "i2": 1})

	if got, want := r1["data"], "foo"; got != want {
		t.Errorf("r1[%q] = %q; want %q", "data", got, want)
	}

	r2, err = f.Get(map[string]interface{}{"i1": 1, "i2": 2})
	if got, want := r2["data"], "bar"; got != want {
		t.Errorf("r2[%q] = %q; want %q", "data", got, want)
	}

}
