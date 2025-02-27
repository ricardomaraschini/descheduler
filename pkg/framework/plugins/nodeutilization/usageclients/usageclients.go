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

package usageclients

import (
	v1 "k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/api/resource"
	"sigs.k8s.io/descheduler/pkg/api"
)

// UsageClientType is used to distinguish between different types of usage clients.
type UsageClientType int

const (
	RequestedUsageClientType UsageClientType = iota
	ActualUsageClientType
	PrometheusUsageClientType
)

// Interface is the interface implemented by all types of usage clients. Usage
// clients are responsible for assessing node usage.
type Interface interface {
	// Sync method fetches data and assess the usage of the given nodes.
	// This function must be called before any other method.
	Sync(nodes []*v1.Node) error

	// NodeUtilization returns the utilization of the given node.
	NodeUtilization(node string) map[v1.ResourceName]*resource.Quantity

	// NodesUsage returns the utilization for all the provided nodes. This
	// function is meant to return all the pods assigned to the given nodes
	// as well, this should give the called more information than a simple
	// call to NodeUtilization.
	NodesUsage(nodes []*v1.Node) []NodeUsage

	// NodesAverageUsage returns the resource average usage for all
	// resources for all the provided nodes.
	NodesAverageUsage(nodes []*v1.Node) api.ResourceThresholds

	// NodeCapacity returns the capacity of the given node. This returns an
	// individual capacity for each single resource.
	NodeCapacity(node *v1.Node) v1.ResourceList

	// Pods list all pods assigned to the given node.
	Pods(node string) []*v1.Pod

	// PodUsage returns the usage of the given pod.
	PodUsage(pod *v1.Pod) (map[v1.ResourceName]*resource.Quantity, error)
}
