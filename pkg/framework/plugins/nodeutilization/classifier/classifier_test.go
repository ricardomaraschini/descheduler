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
	"reflect"
	"testing"

	v1 "k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/api/resource"
	"k8s.io/utils/ptr"
	"sigs.k8s.io/descheduler/pkg/api"
)

func TestClassifySimple(t *testing.T) {
	for _, tt := range []struct {
		name        string
		usage       map[string]int
		limits      map[string][]int
		classifiers []Classifier[int]
		expected    []map[string]int
	}{
		{
			name:     "empty",
			usage:    map[string]int{},
			limits:   map[string][]int{},
			expected: []map[string]int{},
		},
		{
			name: "one under one over",
			usage: map[string]int{
				"node1": 2,
				"node2": 8,
			},
			limits: map[string][]int{
				"node1": {4, 6},
				"node2": {4, 6},
			},
			expected: []map[string]int{
				{"node1": 2},
				{"node2": 8},
			},
			classifiers: []Classifier[int]{
				func(usage, limit int) bool {
					return usage < limit
				},
				func(usage, limit int) bool {
					return usage > limit
				},
			},
		},
		{
			name: "randomly positioned over utilized",
			usage: map[string]int{
				"node1": 2,
				"node2": 8,
				"node3": 2,
				"node4": 8,
				"node5": 8,
				"node6": 2,
				"node7": 2,
				"node8": 8,
				"node9": 8,
			},
			limits: map[string][]int{
				"node1": {4, 6},
				"node2": {4, 6},
				"node3": {4, 6},
				"node4": {4, 6},
				"node5": {4, 6},
				"node6": {4, 6},
				"node7": {4, 6},
				"node8": {4, 6},
				"node9": {4, 6},
			},
			expected: []map[string]int{
				{
					"node1": 2,
					"node3": 2,
					"node6": 2,
					"node7": 2,
				},
				{
					"node2": 8,
					"node4": 8,
					"node5": 8,
					"node8": 8,
					"node9": 8,
				},
			},
			classifiers: []Classifier[int]{
				func(usage, limit int) bool {
					return usage < limit
				},
				func(usage, limit int) bool {
					return usage > limit
				},
			},
		},
	} {
		t.Run(tt.name, func(t *testing.T) {
			result := Classify(tt.usage, tt.limits, tt.classifiers...)
			if !reflect.DeepEqual(result, tt.expected) {
				t.Fatalf("unexpected result: %v", result)
			}
		})
	}
}

func TestClassify_pointers(t *testing.T) {
	for _, tt := range []struct {
		name        string
		usage       map[string]map[v1.ResourceName]*resource.Quantity
		limits      map[string][]map[v1.ResourceName]*resource.Quantity
		classifiers []Classifier[map[v1.ResourceName]*resource.Quantity]
		expected    []map[string]map[v1.ResourceName]*resource.Quantity
	}{
		{
			name:     "empty",
			usage:    map[string]map[v1.ResourceName]*resource.Quantity{},
			limits:   map[string][]map[v1.ResourceName]*resource.Quantity{},
			expected: []map[string]map[v1.ResourceName]*resource.Quantity{},
		},
		{
			name: "single underutilized",
			usage: map[string]map[v1.ResourceName]*resource.Quantity{
				"node1": {
					v1.ResourceCPU:    ptr.To(resource.MustParse("2")),
					v1.ResourceMemory: ptr.To(resource.MustParse("2Gi")),
				},
			},
			limits: map[string][]map[v1.ResourceName]*resource.Quantity{
				"node1": {
					{
						v1.ResourceCPU:    ptr.To(resource.MustParse("4")),
						v1.ResourceMemory: ptr.To(resource.MustParse("4Gi")),
					},
				},
			},
			expected: []map[string]map[v1.ResourceName]*resource.Quantity{
				{
					"node1": {
						v1.ResourceCPU:    ptr.To(resource.MustParse("2")),
						v1.ResourceMemory: ptr.To(resource.MustParse("2Gi")),
					},
				},
			},
			classifiers: []Classifier[map[v1.ResourceName]*resource.Quantity]{
				ForMap[v1.ResourceName, *resource.Quantity, map[v1.ResourceName]*resource.Quantity](
					func(usage, limit *resource.Quantity) int {
						return usage.Cmp(*limit)
					},
				),
			},
		},
		{
			name: "single underutilized and properly utilized",
			usage: map[string]map[v1.ResourceName]*resource.Quantity{
				"node1": {
					v1.ResourceCPU:    ptr.To(resource.MustParse("2")),
					v1.ResourceMemory: ptr.To(resource.MustParse("2Gi")),
				},
				"node2": {
					v1.ResourceCPU:    ptr.To(resource.MustParse("5")),
					v1.ResourceMemory: ptr.To(resource.MustParse("5Gi")),
				},
				"node3": {
					v1.ResourceCPU:    ptr.To(resource.MustParse("8")),
					v1.ResourceMemory: ptr.To(resource.MustParse("8Gi")),
				},
			},
			limits: map[string][]map[v1.ResourceName]*resource.Quantity{
				"node1": {
					{
						v1.ResourceCPU:    ptr.To(resource.MustParse("4")),
						v1.ResourceMemory: ptr.To(resource.MustParse("4Gi")),
					},
					{
						v1.ResourceCPU:    ptr.To(resource.MustParse("16")),
						v1.ResourceMemory: ptr.To(resource.MustParse("16Gi")),
					},
				},
				"node2": {
					{
						v1.ResourceCPU:    ptr.To(resource.MustParse("4")),
						v1.ResourceMemory: ptr.To(resource.MustParse("4Gi")),
					},
					{
						v1.ResourceCPU:    ptr.To(resource.MustParse("16")),
						v1.ResourceMemory: ptr.To(resource.MustParse("16Gi")),
					},
				},
				"node3": {
					{
						v1.ResourceCPU:    ptr.To(resource.MustParse("4")),
						v1.ResourceMemory: ptr.To(resource.MustParse("4Gi")),
					},
					{
						v1.ResourceCPU:    ptr.To(resource.MustParse("16")),
						v1.ResourceMemory: ptr.To(resource.MustParse("16Gi")),
					},
				},
			},
			expected: []map[string]map[v1.ResourceName]*resource.Quantity{
				{
					"node1": {
						v1.ResourceCPU:    ptr.To(resource.MustParse("2")),
						v1.ResourceMemory: ptr.To(resource.MustParse("2Gi")),
					},
				},
				{},
			},
			classifiers: []Classifier[map[v1.ResourceName]*resource.Quantity]{
				ForMap[v1.ResourceName, *resource.Quantity, map[v1.ResourceName]*resource.Quantity](
					func(usage, limit *resource.Quantity) int {
						return usage.Cmp(*limit)
					},
				),
				ForMap[v1.ResourceName, *resource.Quantity, map[v1.ResourceName]*resource.Quantity](
					func(usage, limit *resource.Quantity) int {
						return limit.Cmp(*usage)
					},
				),
			},
		},
	} {
		t.Run(tt.name, func(t *testing.T) {
			result := Classify(tt.usage, tt.limits, tt.classifiers...)
			if !reflect.DeepEqual(result, tt.expected) {
				t.Fatalf("unexpected result: %v", result)
			}
		})
	}
}

func TestClassify(t *testing.T) {
	for _, tt := range []struct {
		name        string
		usage       map[string]v1.ResourceList
		limits      map[string][]v1.ResourceList
		classifiers []Classifier[v1.ResourceList]
		expected    []map[string]v1.ResourceList
	}{
		{
			name:     "empty",
			usage:    map[string]v1.ResourceList{},
			limits:   map[string][]v1.ResourceList{},
			expected: []map[string]v1.ResourceList{},
		},
		{
			name: "single underutilized",
			usage: map[string]v1.ResourceList{
				"node1": {
					v1.ResourceCPU:    resource.MustParse("2"),
					v1.ResourceMemory: resource.MustParse("2Gi"),
				},
			},
			limits: map[string][]v1.ResourceList{
				"node1": {
					{
						v1.ResourceCPU:    resource.MustParse("4"),
						v1.ResourceMemory: resource.MustParse("4Gi"),
					},
				},
			},
			expected: []map[string]v1.ResourceList{
				{
					"node1": {
						v1.ResourceCPU:    resource.MustParse("2"),
						v1.ResourceMemory: resource.MustParse("2Gi"),
					},
				},
			},
			classifiers: []Classifier[v1.ResourceList]{
				ForMap[v1.ResourceName, resource.Quantity, v1.ResourceList](
					func(usage, limit resource.Quantity) int {
						return usage.Cmp(limit)
					},
				),
			},
		},
		{
			name: "less classifiers than limits",
			usage: map[string]v1.ResourceList{
				"node1": {
					v1.ResourceCPU:    resource.MustParse("2"),
					v1.ResourceMemory: resource.MustParse("2Gi"),
				},
				"node2": {
					v1.ResourceCPU:    resource.MustParse("5"),
					v1.ResourceMemory: resource.MustParse("5Gi"),
				},
				"node3": {
					v1.ResourceCPU:    resource.MustParse("8"),
					v1.ResourceMemory: resource.MustParse("8Gi"),
				},
			},
			limits: map[string][]v1.ResourceList{
				"node1": {
					{
						v1.ResourceCPU:    resource.MustParse("4"),
						v1.ResourceMemory: resource.MustParse("4Gi"),
					},
					{
						v1.ResourceCPU:    resource.MustParse("16"),
						v1.ResourceMemory: resource.MustParse("16Gi"),
					},
				},
				"node2": {
					{
						v1.ResourceCPU:    resource.MustParse("4"),
						v1.ResourceMemory: resource.MustParse("4Gi"),
					},
					{
						v1.ResourceCPU:    resource.MustParse("16"),
						v1.ResourceMemory: resource.MustParse("16Gi"),
					},
				},
				"node3": {
					{
						v1.ResourceCPU:    resource.MustParse("4"),
						v1.ResourceMemory: resource.MustParse("4Gi"),
					},
					{
						v1.ResourceCPU:    resource.MustParse("16"),
						v1.ResourceMemory: resource.MustParse("16Gi"),
					},
				},
			},
			expected: []map[string]v1.ResourceList{
				{
					"node1": {
						v1.ResourceCPU:    resource.MustParse("2"),
						v1.ResourceMemory: resource.MustParse("2Gi"),
					},
				},
			},
			classifiers: []Classifier[v1.ResourceList]{
				ForMap[v1.ResourceName, resource.Quantity, v1.ResourceList](
					func(usage, limit resource.Quantity) int {
						return usage.Cmp(limit)
					},
				),
			},
		},
		{
			name: "more classifiers than limits",
			usage: map[string]v1.ResourceList{
				"node1": {
					v1.ResourceCPU:    resource.MustParse("20"),
					v1.ResourceMemory: resource.MustParse("20"),
				},
				"node2": {
					v1.ResourceCPU:    resource.MustParse("50"),
					v1.ResourceMemory: resource.MustParse("50"),
				},
				"node3": {
					v1.ResourceCPU:    resource.MustParse("80"),
					v1.ResourceMemory: resource.MustParse("80"),
				},
			},
			limits: map[string][]v1.ResourceList{
				"node1": {
					{
						v1.ResourceCPU:    resource.MustParse("30"),
						v1.ResourceMemory: resource.MustParse("30"),
					},
				},
				"node2": {
					{
						v1.ResourceCPU:    resource.MustParse("30"),
						v1.ResourceMemory: resource.MustParse("30"),
					},
				},
				"node3": {
					{
						v1.ResourceCPU:    resource.MustParse("30"),
						v1.ResourceMemory: resource.MustParse("30"),
					},
				},
			},
			expected: []map[string]v1.ResourceList{
				{
					"node1": {
						v1.ResourceCPU:    resource.MustParse("20"),
						v1.ResourceMemory: resource.MustParse("20"),
					},
				},
				{},
			},
			classifiers: []Classifier[v1.ResourceList]{
				ForMap[v1.ResourceName, resource.Quantity, v1.ResourceList](
					func(usage, limit resource.Quantity) int {
						return usage.Cmp(limit)
					},
				),
				ForMap[v1.ResourceName, resource.Quantity, v1.ResourceList](
					func(usage, limit resource.Quantity) int {
						return limit.Cmp(usage)
					},
				),
			},
		},
		{
			name: "single underutilized and properly utilized",
			usage: map[string]v1.ResourceList{
				"node1": {
					v1.ResourceCPU:    resource.MustParse("2"),
					v1.ResourceMemory: resource.MustParse("2Gi"),
				},
				"node2": {
					v1.ResourceCPU:    resource.MustParse("5"),
					v1.ResourceMemory: resource.MustParse("5Gi"),
				},
				"node3": {
					v1.ResourceCPU:    resource.MustParse("8"),
					v1.ResourceMemory: resource.MustParse("8Gi"),
				},
			},
			limits: map[string][]v1.ResourceList{
				"node1": {
					{
						v1.ResourceCPU:    resource.MustParse("4"),
						v1.ResourceMemory: resource.MustParse("4Gi"),
					},
					{
						v1.ResourceCPU:    resource.MustParse("16"),
						v1.ResourceMemory: resource.MustParse("16Gi"),
					},
				},
				"node2": {
					{
						v1.ResourceCPU:    resource.MustParse("4"),
						v1.ResourceMemory: resource.MustParse("4Gi"),
					},
					{
						v1.ResourceCPU:    resource.MustParse("16"),
						v1.ResourceMemory: resource.MustParse("16Gi"),
					},
				},
				"node3": {
					{
						v1.ResourceCPU:    resource.MustParse("4"),
						v1.ResourceMemory: resource.MustParse("4Gi"),
					},
					{
						v1.ResourceCPU:    resource.MustParse("16"),
						v1.ResourceMemory: resource.MustParse("16Gi"),
					},
				},
			},
			expected: []map[string]v1.ResourceList{
				{
					"node1": {
						v1.ResourceCPU:    resource.MustParse("2"),
						v1.ResourceMemory: resource.MustParse("2Gi"),
					},
				},
				{},
			},
			classifiers: []Classifier[v1.ResourceList]{
				ForMap[v1.ResourceName, resource.Quantity, v1.ResourceList](
					func(usage, limit resource.Quantity) int {
						return usage.Cmp(limit)
					},
				),
				ForMap[v1.ResourceName, resource.Quantity, v1.ResourceList](
					func(usage, limit resource.Quantity) int {
						return limit.Cmp(usage)
					},
				),
			},
		},
		{
			name: "single underutilized and multiple over utilized nodes",
			usage: map[string]v1.ResourceList{
				"node1": {
					v1.ResourceCPU:    resource.MustParse("2"),
					v1.ResourceMemory: resource.MustParse("2Gi"),
				},
				"node2": {
					v1.ResourceCPU:    resource.MustParse("8"),
					v1.ResourceMemory: resource.MustParse("8Gi"),
				},
				"node3": {
					v1.ResourceCPU:    resource.MustParse("8"),
					v1.ResourceMemory: resource.MustParse("8Gi"),
				},
			},
			limits: map[string][]v1.ResourceList{
				"node1": {
					{
						v1.ResourceCPU:    resource.MustParse("4"),
						v1.ResourceMemory: resource.MustParse("4Gi"),
					},
					{
						v1.ResourceCPU:    resource.MustParse("6"),
						v1.ResourceMemory: resource.MustParse("6Gi"),
					},
				},
				"node2": {
					{
						v1.ResourceCPU:    resource.MustParse("4"),
						v1.ResourceMemory: resource.MustParse("4Gi"),
					},
					{
						v1.ResourceCPU:    resource.MustParse("6"),
						v1.ResourceMemory: resource.MustParse("6Gi"),
					},
				},
				"node3": {
					{
						v1.ResourceCPU:    resource.MustParse("4"),
						v1.ResourceMemory: resource.MustParse("4Gi"),
					},
					{
						v1.ResourceCPU:    resource.MustParse("6"),
						v1.ResourceMemory: resource.MustParse("6Gi"),
					},
				},
			},
			expected: []map[string]v1.ResourceList{
				{
					"node1": {
						v1.ResourceCPU:    resource.MustParse("2"),
						v1.ResourceMemory: resource.MustParse("2Gi"),
					},
				},
				{
					"node2": {
						v1.ResourceCPU:    resource.MustParse("8"),
						v1.ResourceMemory: resource.MustParse("8Gi"),
					},
					"node3": {
						v1.ResourceCPU:    resource.MustParse("8"),
						v1.ResourceMemory: resource.MustParse("8Gi"),
					},
				},
			},
			classifiers: []Classifier[v1.ResourceList]{
				ForMap[v1.ResourceName, resource.Quantity, v1.ResourceList](
					func(usage, limit resource.Quantity) int {
						return usage.Cmp(limit)
					},
				),
				ForMap[v1.ResourceName, resource.Quantity, v1.ResourceList](
					func(usage, limit resource.Quantity) int {
						return limit.Cmp(usage)
					},
				),
			},
		},
		{
			name: "over and under at the same time",
			usage: map[string]v1.ResourceList{
				"node1": {
					v1.ResourceCPU:    resource.MustParse("1"),
					v1.ResourceMemory: resource.MustParse("8Gi"),
				},
				"node2": {
					v1.ResourceCPU:    resource.MustParse("1"),
					v1.ResourceMemory: resource.MustParse("8Gi"),
				},
			},
			limits: map[string][]v1.ResourceList{
				"node1": {
					{
						v1.ResourceCPU:    resource.MustParse("4"),
						v1.ResourceMemory: resource.MustParse("4Gi"),
					},
					{
						v1.ResourceCPU:    resource.MustParse("6"),
						v1.ResourceMemory: resource.MustParse("6Gi"),
					},
				},
				"node2": {
					{
						v1.ResourceCPU:    resource.MustParse("4"),
						v1.ResourceMemory: resource.MustParse("4Gi"),
					},
					{
						v1.ResourceCPU:    resource.MustParse("6"),
						v1.ResourceMemory: resource.MustParse("6Gi"),
					},
				},
			},
			expected: []map[string]v1.ResourceList{
				{},
				{},
			},
			classifiers: []Classifier[v1.ResourceList]{
				ForMap[v1.ResourceName, resource.Quantity, v1.ResourceList](
					func(usage, limit resource.Quantity) int {
						return usage.Cmp(limit)
					},
				),
				ForMap[v1.ResourceName, resource.Quantity, v1.ResourceList](
					func(usage, limit resource.Quantity) int {
						return limit.Cmp(usage)
					},
				),
			},
		},
		{
			name: "only memory over utilized",
			usage: map[string]v1.ResourceList{
				"node1": {
					v1.ResourceCPU:    resource.MustParse("5"),
					v1.ResourceMemory: resource.MustParse("8Gi"),
				},
			},
			limits: map[string][]v1.ResourceList{
				"node1": {
					{
						v1.ResourceCPU:    resource.MustParse("4"),
						v1.ResourceMemory: resource.MustParse("4Gi"),
					},
					{
						v1.ResourceCPU:    resource.MustParse("6"),
						v1.ResourceMemory: resource.MustParse("6Gi"),
					},
				},
			},
			expected: []map[string]v1.ResourceList{
				{},
				{},
			},
			classifiers: []Classifier[v1.ResourceList]{
				ForMap[v1.ResourceName, resource.Quantity, v1.ResourceList](
					func(usage, limit resource.Quantity) int {
						return usage.Cmp(limit)
					},
				),
				ForMap[v1.ResourceName, resource.Quantity, v1.ResourceList](
					func(usage, limit resource.Quantity) int {
						return limit.Cmp(usage)
					},
				),
			},
		},
		{
			name: "randomly positioned over utilized",
			usage: map[string]v1.ResourceList{
				"node1": {v1.ResourceCPU: resource.MustParse("8")},
				"node2": {v1.ResourceCPU: resource.MustParse("2")},
				"node3": {v1.ResourceCPU: resource.MustParse("8")},
				"node4": {v1.ResourceCPU: resource.MustParse("2")},
				"node5": {v1.ResourceCPU: resource.MustParse("8")},
				"node6": {v1.ResourceCPU: resource.MustParse("8")},
				"node7": {v1.ResourceCPU: resource.MustParse("8")},
				"node8": {v1.ResourceCPU: resource.MustParse("2")},
				"node9": {v1.ResourceCPU: resource.MustParse("5")},
			},
			limits: map[string][]v1.ResourceList{
				"node1": {
					{v1.ResourceCPU: resource.MustParse("4")},
					{v1.ResourceCPU: resource.MustParse("6")},
				},
				"node2": {
					{v1.ResourceCPU: resource.MustParse("4")},
					{v1.ResourceCPU: resource.MustParse("6")},
				},
				"node3": {
					{v1.ResourceCPU: resource.MustParse("4")},
					{v1.ResourceCPU: resource.MustParse("6")},
				},
				"node4": {
					{v1.ResourceCPU: resource.MustParse("4")},
					{v1.ResourceCPU: resource.MustParse("6")},
				},
				"node5": {
					{v1.ResourceCPU: resource.MustParse("4")},
					{v1.ResourceCPU: resource.MustParse("6")},
				},
				"node6": {
					{v1.ResourceCPU: resource.MustParse("4")},
					{v1.ResourceCPU: resource.MustParse("6")},
				},
				"node7": {
					{v1.ResourceCPU: resource.MustParse("4")},
					{v1.ResourceCPU: resource.MustParse("6")},
				},
				"node8": {
					{v1.ResourceCPU: resource.MustParse("4")},
					{v1.ResourceCPU: resource.MustParse("6")},
				},
				"node9": {
					{v1.ResourceCPU: resource.MustParse("4")},
					{v1.ResourceCPU: resource.MustParse("6")},
				},
			},
			expected: []map[string]v1.ResourceList{
				{
					"node2": {v1.ResourceCPU: resource.MustParse("2")},
					"node4": {v1.ResourceCPU: resource.MustParse("2")},
					"node8": {v1.ResourceCPU: resource.MustParse("2")},
				},
				{
					"node1": {v1.ResourceCPU: resource.MustParse("8")},
					"node3": {v1.ResourceCPU: resource.MustParse("8")},
					"node5": {v1.ResourceCPU: resource.MustParse("8")},
					"node6": {v1.ResourceCPU: resource.MustParse("8")},
					"node7": {v1.ResourceCPU: resource.MustParse("8")},
				},
			},
			classifiers: []Classifier[v1.ResourceList]{
				ForMap[v1.ResourceName, resource.Quantity, v1.ResourceList](
					func(usage, limit resource.Quantity) int {
						return usage.Cmp(limit)
					},
				),
				ForMap[v1.ResourceName, resource.Quantity, v1.ResourceList](
					func(usage, limit resource.Quantity) int {
						return limit.Cmp(usage)
					},
				),
			},
		},
	} {
		t.Run(tt.name, func(t *testing.T) {
			result := Classify(tt.usage, tt.limits, tt.classifiers...)
			if !reflect.DeepEqual(result, tt.expected) {
				t.Fatalf("unexpected result: %v", result)
			}
		})
	}
}

func TestNormalizeAndClassify(t *testing.T) {
	for _, tt := range []struct {
		name        string
		usage       map[string]v1.ResourceList
		totals      map[string]v1.ResourceList
		thresholds  map[string][]api.ResourceThresholds
		expected    []map[string]api.ResourceThresholds
		classifiers []Classifier[api.ResourceThresholds]
	}{
		{
			name: "happy path test",
			usage: map[string]v1.ResourceList{
				"node1": {
					// underutilized on cpu and memory.
					v1.ResourceCPU:    resource.MustParse("10"),
					v1.ResourceMemory: resource.MustParse("10"),
				},
				"node2": {
					// overutilized on cpu and memory.
					v1.ResourceCPU:    resource.MustParse("90"),
					v1.ResourceMemory: resource.MustParse("90"),
				},
				"node3": {
					// properly utilized on cpu and memory.
					v1.ResourceCPU:    resource.MustParse("50"),
					v1.ResourceMemory: resource.MustParse("50"),
				},
				"node4": {
					// underutilized on cpu and overutilized on memory.
					v1.ResourceCPU:    resource.MustParse("10"),
					v1.ResourceMemory: resource.MustParse("90"),
				},
			},
			totals: map[string]v1.ResourceList{
				"node1": {
					v1.ResourceCPU:    resource.MustParse("100"),
					v1.ResourceMemory: resource.MustParse("100"),
				},
				"node2": {
					v1.ResourceCPU:    resource.MustParse("100"),
					v1.ResourceMemory: resource.MustParse("100"),
				},
				"node3": {
					v1.ResourceCPU:    resource.MustParse("100"),
					v1.ResourceMemory: resource.MustParse("100"),
				},
				"node4": {
					v1.ResourceCPU:    resource.MustParse("100"),
					v1.ResourceMemory: resource.MustParse("100"),
				},
			},
			thresholds: map[string][]api.ResourceThresholds{
				"node1": {
					{v1.ResourceCPU: 20, v1.ResourceMemory: 20},
					{v1.ResourceCPU: 80, v1.ResourceMemory: 80},
				},
				"node2": {
					{v1.ResourceCPU: 20, v1.ResourceMemory: 20},
					{v1.ResourceCPU: 80, v1.ResourceMemory: 80},
				},
				"node3": {
					{v1.ResourceCPU: 20, v1.ResourceMemory: 20},
					{v1.ResourceCPU: 80, v1.ResourceMemory: 80},
				},
				"node4": {
					{v1.ResourceCPU: 20, v1.ResourceMemory: 20},
					{v1.ResourceCPU: 80, v1.ResourceMemory: 80},
				},
			},
			expected: []map[string]api.ResourceThresholds{
				{
					"node1": {v1.ResourceCPU: 10, v1.ResourceMemory: 10},
				},
				{
					"node2": {v1.ResourceCPU: 90, v1.ResourceMemory: 90},
				},
			},
			classifiers: []Classifier[api.ResourceThresholds]{
				ForMap[v1.ResourceName, api.Percentage, api.ResourceThresholds](
					func(usage, limit api.Percentage) int {
						return int(usage - limit)
					},
				),
				ForMap[v1.ResourceName, api.Percentage, api.ResourceThresholds](
					func(usage, limit api.Percentage) int {
						return int(limit - usage)
					},
				),
			},
		},
		{
			name: "three thresholds",
			usage: map[string]v1.ResourceList{
				"node1": {
					// match for the first classifier.
					v1.ResourceCPU:    resource.MustParse("10"),
					v1.ResourceMemory: resource.MustParse("10"),
				},
				"node2": {
					// match for the third classifier.
					v1.ResourceCPU:    resource.MustParse("90"),
					v1.ResourceMemory: resource.MustParse("90"),
				},
				"node3": {
					// match fo the second classifier.
					v1.ResourceCPU:    resource.MustParse("40"),
					v1.ResourceMemory: resource.MustParse("40"),
				},
				"node4": {
					// matches no classifier.
					v1.ResourceCPU:    resource.MustParse("10"),
					v1.ResourceMemory: resource.MustParse("90"),
				},
				"node5": {
					// match for the first classifier.
					v1.ResourceCPU:    resource.MustParse("11"),
					v1.ResourceMemory: resource.MustParse("18"),
				},
			},
			totals: map[string]v1.ResourceList{
				"node1": {
					v1.ResourceCPU:    resource.MustParse("100"),
					v1.ResourceMemory: resource.MustParse("100"),
				},
				"node2": {
					v1.ResourceCPU:    resource.MustParse("100"),
					v1.ResourceMemory: resource.MustParse("100"),
				},
				"node3": {
					v1.ResourceCPU:    resource.MustParse("100"),
					v1.ResourceMemory: resource.MustParse("100"),
				},
				"node4": {
					v1.ResourceCPU:    resource.MustParse("100"),
					v1.ResourceMemory: resource.MustParse("100"),
				},
				"node5": {
					v1.ResourceCPU:    resource.MustParse("100"),
					v1.ResourceMemory: resource.MustParse("100"),
				},
			},
			thresholds: map[string][]api.ResourceThresholds{
				"node1": {
					{v1.ResourceCPU: 20, v1.ResourceMemory: 20},
					{v1.ResourceCPU: 50, v1.ResourceMemory: 50},
					{v1.ResourceCPU: 80, v1.ResourceMemory: 80},
				},
				"node2": {
					{v1.ResourceCPU: 20, v1.ResourceMemory: 20},
					{v1.ResourceCPU: 50, v1.ResourceMemory: 50},
					{v1.ResourceCPU: 80, v1.ResourceMemory: 80},
				},
				"node3": {
					{v1.ResourceCPU: 20, v1.ResourceMemory: 20},
					{v1.ResourceCPU: 50, v1.ResourceMemory: 50},
					{v1.ResourceCPU: 80, v1.ResourceMemory: 80},
				},
				"node4": {
					{v1.ResourceCPU: 20, v1.ResourceMemory: 20},
					{v1.ResourceCPU: 50, v1.ResourceMemory: 50},
					{v1.ResourceCPU: 80, v1.ResourceMemory: 80},
				},
				"node5": {
					{v1.ResourceCPU: 20, v1.ResourceMemory: 20},
					{v1.ResourceCPU: 50, v1.ResourceMemory: 50},
					{v1.ResourceCPU: 80, v1.ResourceMemory: 80},
				},
			},
			expected: []map[string]api.ResourceThresholds{
				{
					"node1": {v1.ResourceCPU: 10, v1.ResourceMemory: 10},
					"node5": {v1.ResourceCPU: 11, v1.ResourceMemory: 18},
				},
				{
					"node3": {v1.ResourceCPU: 40, v1.ResourceMemory: 40},
				},
				{
					"node2": {v1.ResourceCPU: 90, v1.ResourceMemory: 90},
				},
			},
			classifiers: []Classifier[api.ResourceThresholds]{
				ForMap[v1.ResourceName, api.Percentage, api.ResourceThresholds](
					func(usage, limit api.Percentage) int {
						return int(usage - limit)
					},
				),
				ForMap[v1.ResourceName, api.Percentage, api.ResourceThresholds](
					func(usage, limit api.Percentage) int {
						return int(usage - limit)
					},
				),
				ForMap[v1.ResourceName, api.Percentage, api.ResourceThresholds](
					func(usage, limit api.Percentage) int {
						return int(limit - usage)
					},
				),
			},
		},
	} {
		t.Run(tt.name, func(t *testing.T) {
			res := Classify(
				Normalize(tt.usage, tt.totals, ResourceUsageNormalizer),
				tt.thresholds,
				tt.classifiers...,
			)
			if !reflect.DeepEqual(res, tt.expected) {
				t.Fatalf("unexpected result: %v, expecting: %v", res, tt.expected)
			}
		})
	}
}
