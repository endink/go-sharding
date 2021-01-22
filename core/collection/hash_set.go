/*
 *
 *  * Copyright 2021. Go-Sharding Author All Rights Reserved.
 *  *
 *  *  Licensed under the Apache License, Version 2.0 (the "License");
 *  *  you may not use this file except in compliance with the License.
 *  *  You may obtain a copy of the License at
 *  *
 *  *      http://www.apache.org/licenses/LICENSE-2.0
 *  *
 *  *  Unless required by applicable law or agreed to in writing, software
 *  *  distributed under the License is distributed on an "AS IS" BASIS,
 *  *  WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 *  *  See the License for the specific language governing permissions and
 *  *  limitations under the License.
 *  *
 *  *  File author: Anders Xiao
 *
 */

package collection

import (
	"fmt"
	"strings"
)

// HashSet holds elements in go's native map
type HashSet struct {
	items map[interface{}]struct{}
}

var justNothing = struct{}{}

// New instantiates a new empty set and adds the passed values, if any, to the set
func NewHashSet(values ...interface{}) *HashSet {
	set := &HashSet{items: make(map[interface{}]struct{}, len(values))}
	if len(values) > 0 {
		set.Add(values...)
	}
	return set
}

// Add adds the items (one or more) to the set.
func (set *HashSet) Add(items ...interface{}) {
	for _, item := range items {
		set.items[item] = justNothing
	}
}

// Remove removes the items (one or more) from the set.
func (set *HashSet) Remove(items ...interface{}) {
	for _, item := range items {
		delete(set.items, item)
	}
}

// Contains check if items (one or more) are present in the set.
// All items have to be present in the set for the method to return true.
// Returns true if no arguments are passed at all, i.e. set is always superset of empty set.
func (set *HashSet) Contains(items ...interface{}) bool {
	for _, item := range items {
		if _, contains := set.items[item]; !contains {
			return false
		}
	}
	return true
}

// Empty returns true if set does not contain any elements.
func (set *HashSet) Empty() bool {
	return set.Size() == 0
}

// Size returns number of elements within the set.
func (set *HashSet) Size() int {
	return len(set.items)
}

// Clear clears all values in the set.
func (set *HashSet) Clear() {
	set.items = make(map[interface{}]struct{})
}

// Values returns all items in the set.
func (set *HashSet) Values() []interface{} {
	values := make([]interface{}, set.Size())
	count := 0
	for item := range set.items {
		values[count] = item
		count++
	}
	return values
}

func (set *HashSet) All(action func(item interface{}) (bool, error)) error {
	return set.AllIndex(func(_ int, item interface{}) (bool, error) {
		return action(item)
	})
}

func (set *HashSet) AllIndex(action func(index int, item interface{}) (bool, error)) error {
	var index int
	for key, _ := range set.items {
		next, err := action(index, key)
		if err != nil {
			return err
		}
		if !next {
			break
		}
	}
	return nil
}

func (set *HashSet) Select(action func(item interface{}) (bool, error)) ([]interface{}, error) {
	return set.SelectIndex(func(index int, item interface{}) (bool, error) {
		return action(item)
	})
}

func (set *HashSet) SelectIndex(action func(index int, item interface{}) (bool, error)) ([]interface{}, error) {
	var result []interface{}
	var index int
	for key, _ := range set.items {
		if ok, err := action(index, key); err == nil {
			if ok {
				result = append(result, key)
			}
		} else {
			return nil, err
		}
	}
	return result, nil
}

// String returns a string representation of container
func (set *HashSet) String() string {
	str := "HashSet\n"
	var items []string
	for k := range set.items {
		items = append(items, fmt.Sprintf("%v", k))
	}
	str += strings.Join(items, ", ")
	return str
}
