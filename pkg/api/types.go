/*
Copyright 2017 The Kubernetes Authors.

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

package api

import (
	"math"

	v1 "k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/api/resource"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/utils/ptr"
)

// +k8s:deepcopy-gen:interfaces=k8s.io/apimachinery/pkg/runtime.Object

type DeschedulerPolicy struct {
	metav1.TypeMeta

	// Profiles
	Profiles []DeschedulerProfile

	// NodeSelector for a set of nodes to operate over
	NodeSelector *string

	// MaxNoOfPodsToEvictPerNode restricts maximum of pods to be evicted per node.
	MaxNoOfPodsToEvictPerNode *uint

	// MaxNoOfPodsToEvictPerNamespace restricts maximum of pods to be evicted per namespace.
	MaxNoOfPodsToEvictPerNamespace *uint

	// MaxNoOfPodsToTotal restricts maximum of pods to be evicted total.
	MaxNoOfPodsToEvictTotal *uint

	// EvictionFailureEventNotification should be set to true to enable eviction failure event notification.
	// Default is false.
	EvictionFailureEventNotification *bool

	// MetricsCollector configures collection of metrics about actual resource utilization
	MetricsCollector MetricsCollector

	// GracePeriodSeconds The duration in seconds before the object should be deleted. Value must be non-negative integer.
	// The value zero indicates delete immediately. If this value is nil, the default grace period for the
	// specified type will be used.
	// Defaults to a per object value if not specified. zero means delete immediately.
	GracePeriodSeconds *int64
}

// Namespaces carries a list of included/excluded namespaces
// for which a given strategy is applicable
type Namespaces struct {
	Include []string `json:"include,omitempty"`
	Exclude []string `json:"exclude,omitempty"`
}

// EvictionLimits limits the number of evictions per domain. E.g. node, namespace, total.
type EvictionLimits struct {
	// node restricts the maximum number of evictions per node
	Node *uint `json:"node,omitempty"`
}

type (
	Percentage         float64
	ResourceThresholds map[v1.ResourceName]Percentage
)

// Negative returns a new ResourceThresholds with all its values negated.
func (r ResourceThresholds) Negative() ResourceThresholds {
	reversed := make(ResourceThresholds, len(r))
	for k, v := range r {
		reversed[k] = -v
	}
	return reversed
}

// Round returns a new ResourceThresholds with all its values rounded according
// to math.Round.
func (r ResourceThresholds) Round() ResourceThresholds {
	rounded := make(ResourceThresholds, len(r))
	for k, v := range r {
		rounded[k] = Percentage(math.Round(float64(v)))
	}
	return rounded
}

type PriorityThreshold struct {
	Value *int32 `json:"value"`
	Name  string `json:"name"`
}

type DeschedulerProfile struct {
	Name          string
	PluginConfigs []PluginConfig
	Plugins       Plugins
}

type PluginConfig struct {
	Name string
	Args runtime.Object
}

type Plugins struct {
	PreSort           PluginSet
	Sort              PluginSet
	Deschedule        PluginSet
	Balance           PluginSet
	Filter            PluginSet
	PreEvictionFilter PluginSet
}

type PluginSet struct {
	Enabled  []string
	Disabled []string
}

// MetricsCollector configures collection of metrics about actual resource utilization
type MetricsCollector struct {
	// Enabled metrics collection from kubernetes metrics.
	// Later, the collection can be extended to other providers.
	Enabled bool
}

// ReferencedResourceList is an adaption of v1.ResourceList with resources as references
type ReferencedResourceList = map[v1.ResourceName]*resource.Quantity

// ReferencedResourceListForNodesCapacity returns a ReferencedResourceList for
// the capacity of a list of nodes. If allocatable resources are present, they
// are used instead of capacity.
func ReferencedResourceListForNodesCapacity(node []*v1.Node) map[string]ReferencedResourceList {
	capacities := map[string]ReferencedResourceList{}
	for _, n := range node {
		capacities[n.Name] = ReferencedResourceListForNodeCapacity(n)
	}
	return capacities
}

// ReferencedResourceListForNodeCapacity returns a ReferencedResourceList for
// the capacity of a node. If allocatable resources are present, they are used
// instead of capacity.
func ReferencedResourceListForNodeCapacity(node *v1.Node) ReferencedResourceList {
	capacity := node.Status.Capacity
	if len(node.Status.Allocatable) > 0 {
		capacity = node.Status.Allocatable
	}

	referenced := ReferencedResourceList{}
	for name, quantity := range capacity {
		referenced[name] = ptr.To(quantity)
	}

	return referenced
}
