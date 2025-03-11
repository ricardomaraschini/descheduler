/*
Copyright 2025 The Kubernetes Authors.
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

package classifier

import (
	"golang.org/x/exp/constraints"
)

// Normalizer is a function that receives two values of the same type and
// return an object of a different type. An usage case can be a function
// that converts a memory usage from mb to % (the first argument would be
// the memory usage in mb and the second argument would be the total memory
// available in mb).
type Normalizer[V, N any] func(V, V) N

// Number is an interface that represents a number. Represents things we
// can do math operations on.
type Number interface {
	constraints.Integer | constraints.Float
}

// Normalize uses a Normalizer function to normalize a set of values. For
// example one may want to convert a set of memory usages from mb to %.
// This function receives a set of usages, a set of totals, and a Normalizer
// function. The function will return a map with the normalized values.
func Normalize[K comparable, V, N any](usages, totals Values[K, V], fn Normalizer[V, N]) map[K]N {
	result := Values[K, N]{}
	for key, value := range usages {
		total, ok := totals[key]
		if !ok {
			continue
		}

		result[key] = fn(value, total)
	}
	return result
}

// Average calculates the average of a set of values. This function receives
// a map of values and returns the average of all the values. Average expects
// the values to represent the same unit of measure. You can use this function
// after Normalizing the values.
func Average[J, K comparable, N Number, V ~map[J]N](values map[K]V) V {
	result := V{}
	for _, imap := range values {
		for name, value := range imap {
			result[name] += value
		}
	}

	for name := range result {
		result[name] /= N(len(values))
	}

	return result
}
