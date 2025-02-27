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
	"context"
	"fmt"
	"time"

	v1 "k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/api/resource"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/klog/v2"
	utilptr "k8s.io/utils/ptr"

	"sigs.k8s.io/descheduler/pkg/api"
	"sigs.k8s.io/descheduler/pkg/descheduler/metricscollector"
	podutil "sigs.k8s.io/descheduler/pkg/descheduler/pod"
)

// Makes sure that the ActualUsageClient struct satisfies the Interface.
var _ Interface = &ActualUsageClient{}

// ActualUsageClient assesses the node usage using a metrics server. This is
// slightly different from the RequestedUsageClient, which uses pod requests.
type ActualUsageClient struct {
	resourceNames         []v1.ResourceName
	getPodsAssignedToNode podutil.GetPodsAssignedToNodeFunc
	metricsCollector      *metricscollector.MetricsCollector
	pods                  map[string][]*v1.Pod
	nodeUtilization       map[string]map[v1.ResourceName]*resource.Quantity
}

// NewActualUsageClient creates a new ActualUsageClient.
func NewActualUsageClient(
	resourceNames []v1.ResourceName,
	getPodsAssignedToNode podutil.GetPodsAssignedToNodeFunc,
	metricsCollector *metricscollector.MetricsCollector,
) *ActualUsageClient {
	return &ActualUsageClient{
		resourceNames:         resourceNames,
		getPodsAssignedToNode: getPodsAssignedToNode,
		metricsCollector:      metricsCollector,
	}
}

// NodeUtilization returns the node utilization as calculated by the metrics
// server.
func (client *ActualUsageClient) NodeUtilization(node string) map[v1.ResourceName]*resource.Quantity {
	return client.nodeUtilization[node]
}

// Pods returns the list of pods present on the node.
func (client *ActualUsageClient) Pods(node string) []*v1.Pod {
	return client.pods[node]
}

// PodUsage returns the pod usage using the metrics server.
func (client *ActualUsageClient) PodUsage(pod *v1.Pod) (map[v1.ResourceName]*resource.Quantity, error) {
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()

	// It's not efficient to keep track of all pods in a cluster when only
	// their fractions is evicted. Thus, take the current pod metrics
	// without computing any softening (like e.g. EWMA).
	metricsClient := client.metricsCollector.MetricsClient().MetricsV1beta1()
	podMetrics, err := metricsClient.PodMetricses(pod.Namespace).Get(ctx, pod.Name, metav1.GetOptions{})
	if err != nil {
		return nil, fmt.Errorf("unable to get podmetrics for %q/%q: %v", pod.Namespace, pod.Name, err)
	}

	totalUsage := make(map[v1.ResourceName]*resource.Quantity)
	for _, container := range podMetrics.Containers {
		for _, resourceName := range client.resourceNames {
			if _, exists := container.Usage[resourceName]; !exists {
				continue
			}

			if totalUsage[resourceName] == nil {
				totalUsage[resourceName] = utilptr.To(container.Usage[resourceName].DeepCopy())
				continue
			}
			totalUsage[resourceName].Add(container.Usage[resourceName])
		}
	}

	return totalUsage, nil
}

// Sync fetches utilization for all provided nodes and populates the client.
func (client *ActualUsageClient) Sync(nodes []*v1.Node) error {
	client.nodeUtilization = map[string]map[v1.ResourceName]*resource.Quantity{}
	client.pods = map[string][]*v1.Pod{}

	nodesUsage, err := client.metricsCollector.AllNodesUsage()
	if err != nil {
		return err
	}

	for _, node := range nodes {
		pods, err := podutil.ListPodsOnANode(node.Name, client.getPodsAssignedToNode, nil)
		if err != nil {
			klog.V(2).InfoS("Node will not be processed, error accessing its pods", "node", klog.KObj(node), "err", err)
			return fmt.Errorf("error accessing %q node's pods: %v", node.Name, err)
		}

		nodeUsage, ok := nodesUsage[node.Name]
		if !ok {
			return fmt.Errorf("unable to find node %q in the collected metrics", node.Name)
		}
		nodeUsage[v1.ResourcePods] = resource.NewQuantity(int64(len(pods)), resource.DecimalSI)

		for _, resourceName := range client.resourceNames {
			if _, exists := nodeUsage[resourceName]; !exists {
				return fmt.Errorf("unable to find %q resource for collected %q node metric", resourceName, node.Name)
			}
		}

		// store the snapshot of pods from the same (or the closest)
		// node utilization computation
		client.pods[node.Name] = pods
		client.nodeUtilization[node.Name] = nodeUsage
	}

	return nil
}

// NodesUsage gather node utilization and pods into a slice of NodeUsage.
func (client *ActualUsageClient) NodesUsage(nodes []*v1.Node) []NodeUsage {
	return getNodeUsage(client, nodes)
}

// NodesAverageUsage calculates the average of the resources usage of all the
// provided nodes.
func (client *ActualUsageClient) NodesAverageUsage(nodes []*v1.Node) api.ResourceThresholds {
	return averageNodeBasicResources(client, nodes)
}

// NodeCapacity returns the node capacity as reported by its status or, if
// present, according to is allocatable resources.
func (client *ActualUsageClient) NodeCapacity(node *v1.Node) v1.ResourceList {
	capacity := node.Status.Capacity
	if len(node.Status.Allocatable) > 0 {
		capacity = node.Status.Allocatable
	}
	return capacity
}
