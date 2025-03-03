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
package thresholds

import (
	v1 "k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/api/resource"

	"sigs.k8s.io/descheduler/pkg/api"
	"sigs.k8s.io/descheduler/pkg/framework/plugins/nodeutilization/usageclients"
)

const (
	// MinResourcePercentage is the minimum value of a resource's
	// percentage.
	MinResourcePercentage = 0
	// MaxResourcePercentage is the maximum value of a resource's
	// percentage.
	MaxResourcePercentage = 100
)

// ClassifyNodeFn is a function that is capable of classifying a node based on
// its usage. This function is opaque from this package point of view, whoever
// does the classification does it outside of the context of this package.
type ClassifyNodeFn func(usageclients.NodeUsage, NodeThresholds)

// NodeThresholds struct represents the usage thresholds for a node. It has
// both a low and high limits. The idea is that if the node is over the high
// limit, it is overutilized while it it is below the lower limit then it is
// underutilized.
type NodeThresholds struct {
	Low  map[v1.ResourceName]*resource.Quantity
	High map[v1.ResourceName]*resource.Quantity
}

// NodeProcessor is an entity capable of assessing the node thresholds.
// Everything starts with the thresholds provided by the user. These thresholds
// are specified for a certain list of resources. They can be either absolute
// values or relative (to the average) values.
type NodeProcessor struct {
	nodes                  []*v1.Node
	lowThreshold           api.ResourceThresholds
	highThreshold          api.ResourceThresholds
	resourceNames          []v1.ResourceName
	useDeviationThresholds bool
	usageClient            usageclients.Interface
}

// NewNodeProcessor creates a new node threshold processor.
func NewNodeProcessor(
	nodes []*v1.Node,
	lowThreshold api.ResourceThresholds,
	highThreshold api.ResourceThresholds,
	resourceNames []v1.ResourceName,
	useDeviationThresholds bool,
	usageClient usageclients.Interface,
) *NodeProcessor {
	return &NodeProcessor{
		nodes:                  nodes,
		lowThreshold:           lowThreshold,
		highThreshold:          highThreshold,
		resourceNames:          resourceNames,
		useDeviationThresholds: useDeviationThresholds,
		usageClient:            usageClient,
	}
}

// resourceThreshold calculates the resource threshold for the given resource
// name and threshold. The threshold is calculated as a percentage of the
// node's capacity.
func (n *NodeProcessor) resourceThreshold(
	nodeCapacity v1.ResourceList,
	resourceName v1.ResourceName,
	threshold api.Percentage,
) *resource.Quantity {
	defaultFormat := resource.DecimalSI
	if resourceName == v1.ResourceMemory {
		defaultFormat = resource.BinarySI
	}

	// A threshold is in percentages but in <0;100> interval. Performing
	// `threshold * 0.01` will convert <0;100> interval into <0;1>.
	// Multiplying it with capacity will give fraction of the capacity
	// corresponding to the given resource threshold in Quantity units.
	fraction := func(resourceCapacity int64) int64 {
		return int64(float64(threshold) * 0.01 * float64(resourceCapacity))
	}

	resourceCapacityQuantity := nodeCapacity.Name(resourceName, defaultFormat)

	if resourceName == v1.ResourceCPU {
		return resource.NewMilliQuantity(
			fraction(resourceCapacityQuantity.MilliValue()),
			defaultFormat,
		)
	}

	return resource.NewQuantity(
		fraction(resourceCapacityQuantity.Value()),
		defaultFormat,
	)
}

// normalizePercentage makes sure we never return a value outside of the
// <0;100> interval.
func (n *NodeProcessor) normalizePercentage(percent api.Percentage) api.Percentage {
	if percent > MaxResourcePercentage {
		return MaxResourcePercentage
	}
	if percent < MinResourcePercentage {
		return MinResourcePercentage
	}
	return percent
}

// thresholdsForNode assess the thresholds for the given node. If deviation
// thresholds are used, the average is used to calculate the thresholds. If
// not, the thresholds are copied directly from the provided values.
func (n NodeProcessor) thresholdsForNode(node *v1.Node, average api.ResourceThresholds) NodeThresholds {
	capacity := n.usageClient.NodeCapacity(node)

	thresholds := NodeThresholds{
		Low:  map[v1.ResourceName]*resource.Quantity{},
		High: map[v1.ResourceName]*resource.Quantity{},
	}

	for _, resourceName := range n.resourceNames {
		// if we aren't using the deviation thresholds, things are
		// simpler. we just need to guarantee that we copy the values
		// directly.
		if !n.useDeviationThresholds {
			thresholds.Low[resourceName] = n.resourceThreshold(
				capacity, resourceName, n.lowThreshold[resourceName],
			)

			thresholds.High[resourceName] = n.resourceThreshold(
				capacity, resourceName, n.highThreshold[resourceName],
			)
			continue
		}

		resCapacity := capacity[resourceName]
		if n.lowThreshold[resourceName] == MinResourcePercentage {
			thresholds.Low[resourceName] = &resCapacity
			thresholds.High[resourceName] = &resCapacity
			continue
		}

		pct := average[resourceName] - n.lowThreshold[resourceName]
		thresholds.Low[resourceName] = n.resourceThreshold(
			capacity, resourceName, n.normalizePercentage(pct),
		)

		pct = average[resourceName] + n.highThreshold[resourceName]
		thresholds.High[resourceName] = n.resourceThreshold(
			capacity, resourceName, n.normalizePercentage(pct),
		)
	}

	return thresholds
}

// process calculates the node thresholds using the provided usage client.
func (n NodeProcessor) process() map[string]NodeThresholds {
	result := map[string]NodeThresholds{}

	// if we are dealing with deviation thresholds, we need to calculate
	// the average resource usage for all the nodes first as the thresholds
	// now indicate the deviation from the mean.
	average := api.ResourceThresholds{}
	if n.useDeviationThresholds {
		average = n.usageClient.NodesAverageUsage(n.nodes)
	}

	for _, node := range n.nodes {
		result[node.Name] = n.thresholdsForNode(node, average)
	}

	return result
}

// Classify runs the provided FilterNodeFn on each node based. The function
// receives a pointer to a v1.Node object, the node usage (according to usage
// client assessment) and the node thresholds. Functions are ran in the order
// they are provided.
func (n NodeProcessor) Classify(classifiers ...ClassifyNodeFn) {
	thresholds := n.process()
	for _, usage := range n.usageClient.NodesUsage(n.nodes) {
		for _, classifier := range classifiers {
			classifier(usage, thresholds[usage.Node.Name])
		}
	}
}

// IsNodeWithHighUtilization returns true if the node is overutilized. This
// functions take all metrics into account.
func (n *NodeProcessor) IsNodeWithHighUtilization(
	usage usageclients.NodeUsage,
	threshold map[v1.ResourceName]*resource.Quantity,
) bool {
	for name, nodeValue := range usage.Usage {
		// usage.highResourceThreshold[name] < nodeValue
		if threshold[name].Cmp(*nodeValue) == -1 {
			return true
		}
	}
	return false
}

// IsNodeWithLowUtilization returns true if the node is underutilized. This
// functions take all metrics into account.
func (n *NodeProcessor) IsNodeWithLowUtilization(
	usage usageclients.NodeUsage,
	threshold map[v1.ResourceName]*resource.Quantity,
) bool {
	for name, nodeValue := range usage.Usage {
		// usage.lowResourceThreshold[name] < nodeValue
		if threshold[name].Cmp(*nodeValue) == -1 {
			return false
		}
	}
	return true
}
