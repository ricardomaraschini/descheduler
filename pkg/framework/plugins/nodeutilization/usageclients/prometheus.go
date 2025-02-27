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

	promapi "github.com/prometheus/client_golang/api"
	promv1 "github.com/prometheus/client_golang/api/prometheus/v1"
	"github.com/prometheus/common/model"
	v1 "k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/api/resource"
	"k8s.io/klog/v2"

	"sigs.k8s.io/descheduler/pkg/api"
	podutil "sigs.k8s.io/descheduler/pkg/descheduler/pod"
)

// Opposite from other metrics we don't know what we are measuring with
// Prometheus. We essentially run an user provided query and expect to find a
// value between 0 and 1 as a result. As we don't know what we are measuring
// (it could be cpu, memory, etc) we create a ResourceName to represent it.
const MetricResource = v1.ResourceName("MetricResource")

// Makes sure that the PrometheusUsageClient struct satisfies the Interface
var _ Interface = &PrometheusUsageClient{}

// PrometheusUsageClient assesses the node usage using a prometheus query. We
// don't know what we are measuring but we expect the returning value to be
// between 0 and 1. We also expect the query to return a value for each node.
type PrometheusUsageClient struct {
	getPodsAssignedToNode podutil.GetPodsAssignedToNodeFunc
	promClient            promapi.Client
	promQuery             string
	pods                  map[string][]*v1.Pod
	nodeUtilization       map[string]map[v1.ResourceName]*resource.Quantity
}

// NewPrometheusUsageClient creates a new PrometheusUsageClient.
func NewPrometheusUsageClient(
	getPodsAssignedToNode podutil.GetPodsAssignedToNodeFunc,
	promClient promapi.Client,
	promQuery string,
) *PrometheusUsageClient {
	return &PrometheusUsageClient{
		getPodsAssignedToNode: getPodsAssignedToNode,
		promClient:            promClient,
		promQuery:             promQuery,
	}
}

// NodeUtilization returns the node utilization as calculated by the prometheus.
func (client *PrometheusUsageClient) NodeUtilization(node string) map[v1.ResourceName]*resource.Quantity {
	return client.nodeUtilization[node]
}

// Pods returns the list of pods present on the node.
func (client *PrometheusUsageClient) Pods(node string) []*v1.Pod {
	return client.pods[node]
}

// PodUsage returns the pod usage using the prometheus query. This is not
// supported by this client.
func (client *PrometheusUsageClient) PodUsage(pod *v1.Pod) (map[v1.ResourceName]*resource.Quantity, error) {
	return nil, NewNotSupportedError(
		PrometheusUsageClientType,
		"fetching pod usage is not supported by PrometheusUsageClient",
	)
}

// Sync runs the prometheus query remotely and stores the results in the
// client. This function must be called before any other method.
func (client *PrometheusUsageClient) Sync(nodes []*v1.Node) error {
	client.nodeUtilization = map[string]map[v1.ResourceName]*resource.Quantity{}
	client.pods = map[string][]*v1.Pod{}

	// Let's make the prometheus query bounded by a timeout.
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()

	results, warnings, err := promv1.NewAPI(client.promClient).Query(ctx, client.promQuery, time.Now())
	if err != nil {
		return fmt.Errorf("unable to capture prometheus metrics: %v", err)
	}

	if len(warnings) > 0 {
		klog.Infof("prometheus metrics warnings: %v", warnings)
	}

	if results.Type() != model.ValVector {
		return fmt.Errorf("expected query results to be of type %q, got %q instead", model.ValVector, results.Type())
	}

	nodeUsages := make(map[string]map[v1.ResourceName]*resource.Quantity)
	for _, sample := range results.(model.Vector) {
		nodeName, exists := sample.Metric["instance"]
		if !exists {
			return fmt.Errorf("The collected metrics sample is missing 'instance' key")
		}

		if sample.Value < 0 || sample.Value > 1 {
			return fmt.Errorf("The collected metrics sample for %q has value %v outside of <0; 1> interval", string(nodeName), sample.Value)
		}

		nodeUsages[string(nodeName)] = map[v1.ResourceName]*resource.Quantity{
			MetricResource: resource.NewQuantity(int64(sample.Value*100), resource.DecimalSI),
		}
	}

	for _, node := range nodes {
		if _, exists := nodeUsages[node.Name]; !exists {
			return fmt.Errorf("unable to find metric entry for %v", node.Name)
		}

		pods, err := podutil.ListPodsOnANode(node.Name, client.getPodsAssignedToNode, nil)
		if err != nil {
			return fmt.Errorf("error accessing %q node's pods: %v", node.Name, err)
		}

		// store the snapshot of pods from the same (or the closest)
		// node utilization computation
		client.pods[node.Name] = pods
		client.nodeUtilization[node.Name] = nodeUsages[node.Name]
	}

	return nil
}

// NodesUsage gather node utilization and pods into a slice of NodeUsage.
func (client *PrometheusUsageClient) NodesUsage(nodes []*v1.Node) []NodeUsage {
	return getNodeUsage(client, nodes)
}

// NodesAverageUsage calculates the average of the resources usage of all the
// provided nodes.
func (client *PrometheusUsageClient) NodesAverageUsage(nodes []*v1.Node) api.ResourceThresholds {
	return averageNodeBasicResources(client, nodes)
}

// NodeCapacity for a prometheus query is always 100%.
func (client *PrometheusUsageClient) NodeCapacity(node *v1.Node) v1.ResourceList {
	return map[v1.ResourceName]resource.Quantity{
		MetricResource: *resource.NewQuantity(int64(100), resource.DecimalSI),
	}
}
