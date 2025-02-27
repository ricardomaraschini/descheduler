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
	"fmt"

	v1 "k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/api/resource"
	utilptr "k8s.io/utils/ptr"

	"sigs.k8s.io/descheduler/pkg/api"
	nodeutil "sigs.k8s.io/descheduler/pkg/descheduler/node"
	podutil "sigs.k8s.io/descheduler/pkg/descheduler/pod"
	"sigs.k8s.io/descheduler/pkg/utils"
)

// Makes sure that the RequestedUsageClient struct satisfies the Interface
// interface.
var _ Interface = &RequestedUsageClient{}

// RequestedUsageClient assesses the node usage using pod requests.
type RequestedUsageClient struct {
	resourceNames         []v1.ResourceName
	getPodsAssignedToNode podutil.GetPodsAssignedToNodeFunc
	pods                  map[string][]*v1.Pod
	nodeUtilization       map[string]map[v1.ResourceName]*resource.Quantity
}

// NewRequestedUsageClient creates a new RequestedUsageClient.
func NewRequestedUsageClient(
	resourceNames []v1.ResourceName,
	getPodsAssignedToNode podutil.GetPodsAssignedToNodeFunc,
) *RequestedUsageClient {
	return &RequestedUsageClient{
		resourceNames:         resourceNames,
		getPodsAssignedToNode: getPodsAssignedToNode,
	}
}

// NodeUtilization returns the node utilization as calculated by summing up pod
// requests.
func (s *RequestedUsageClient) NodeUtilization(node string) map[v1.ResourceName]*resource.Quantity {
	return s.nodeUtilization[node]
}

// Pods returns the list of pods used to calculate the node utilization.
func (s *RequestedUsageClient) Pods(node string) []*v1.Pod {
	return s.pods[node]
}

// PodUsage returns the pod usage using its requests.
func (s *RequestedUsageClient) PodUsage(pod *v1.Pod) (map[v1.ResourceName]*resource.Quantity, error) {
	usage := make(map[v1.ResourceName]*resource.Quantity)
	for _, resourceName := range s.resourceNames {
		qtcopy := utils.GetResourceRequestQuantity(pod, resourceName).DeepCopy()
		usage[resourceName] = utilptr.To(qtcopy)
	}
	return usage, nil
}

// Sync fetches utilization for all provided nodes and populates the client
// with the resulting data.
func (s *RequestedUsageClient) Sync(nodes []*v1.Node) error {
	s.nodeUtilization = map[string]map[v1.ResourceName]*resource.Quantity{}
	s.pods = map[string][]*v1.Pod{}

	for _, node := range nodes {
		pods, err := podutil.ListPodsOnANode(node.Name, s.getPodsAssignedToNode, nil)
		if err != nil {
			return fmt.Errorf("error accessing %q node's pods: %v", node.Name, err)
		}

		nodeUsage, err := nodeutil.NodeUtilization(
			pods,
			s.resourceNames,
			func(pod *v1.Pod) (v1.ResourceList, error) {
				req, _ := utils.PodRequestsAndLimits(pod)
				return req, nil
			},
		)
		if err != nil {
			return err
		}

		// store the snapshot of pods from the same (or the closest)
		// node utilization computation
		s.pods[node.Name] = pods
		s.nodeUtilization[node.Name] = nodeUsage
	}

	return nil
}

// NodesUsage gather node utilization and pods into a slice of NodeUsage.
func (client *RequestedUsageClient) NodesUsage(nodes []*v1.Node) []NodeUsage {
	return getNodeUsage(client, nodes)
}

// NodesAverageUsage calculates the average of the resources usage of all the
// provided nodes.
func (client *RequestedUsageClient) NodesAverageUsage(nodes []*v1.Node) api.ResourceThresholds {
	return averageNodeBasicResources(client, nodes)
}

// NodeCapacity returns the node capacity as reported by its status or, if
// present, according to is allocatable resources.
func (client *RequestedUsageClient) NodeCapacity(node *v1.Node) v1.ResourceList {
	capacity := node.Status.Capacity
	if len(node.Status.Allocatable) > 0 {
		capacity = node.Status.Allocatable
	}
	return capacity
}
