package usageclients

import (
	v1 "k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/api/resource"
	"sigs.k8s.io/descheduler/pkg/api"
)

// NodeUsage stores a node's info, pods on it, thresholds and its resource
// usage.
type NodeUsage struct {
	Node    *v1.Node
	Usage   map[v1.ResourceName]*resource.Quantity
	AllPods []*v1.Pod
}

// getNodeUsage is a helper function to gather node utilization information and
// its pods into a single struct. This function returns a slice with the
// utilization of all the nodes.
func getNodeUsage(cli Interface, nodes []*v1.Node) []NodeUsage {
	var nodeUsageList []NodeUsage

	for _, node := range nodes {
		nodeUsageList = append(nodeUsageList, NodeUsage{
			Node:    node,
			Usage:   cli.NodeUtilization(node.Name),
			AllPods: cli.Pods(node.Name),
		})
	}

	return nodeUsageList
}

// averageNodeBasicResources calculates the average of the basic resources
// of all the provided nodes.
func averageNodeBasicResources(cli Interface, nodes []*v1.Node) api.ResourceThresholds {
	total := api.ResourceThresholds{}
	average := api.ResourceThresholds{}
	numberOfNodes := len(nodes)
	for _, node := range nodes {
		usage := cli.NodeUtilization(node.Name)

		nodeCapacity := node.Status.Capacity
		if len(node.Status.Allocatable) > 0 {
			nodeCapacity = node.Status.Allocatable
		}

		for resource, value := range usage {
			nodeCapacityValue := nodeCapacity[resource]
			if resource == v1.ResourceCPU {
				usage := api.Percentage(value.MilliValue())
				capacity := api.Percentage(nodeCapacityValue.MilliValue())
				total[resource] += usage / capacity * 100.0
				continue
			}

			usage := api.Percentage(value.Value())
			capacity := api.Percentage(nodeCapacityValue.Value())
			total[resource] += usage / capacity * 100.0
		}
	}

	for resource, value := range total {
		average[resource] = value / api.Percentage(numberOfNodes)
	}

	return average
}
