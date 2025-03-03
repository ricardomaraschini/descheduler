/*
Copyright 2022 The Kubernetes Authors.

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

package nodeutilization

import (
	"context"
	"fmt"

	v1 "k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/api/resource"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/klog/v2"
	"sigs.k8s.io/descheduler/pkg/api"
	"sigs.k8s.io/descheduler/pkg/descheduler/evictions"
	nodeutil "sigs.k8s.io/descheduler/pkg/descheduler/node"

	podutil "sigs.k8s.io/descheduler/pkg/descheduler/pod"
	"sigs.k8s.io/descheduler/pkg/framework/plugins/nodeutilization/thresholds"
	"sigs.k8s.io/descheduler/pkg/framework/plugins/nodeutilization/usageclients"
	frameworktypes "sigs.k8s.io/descheduler/pkg/framework/types"
)

const HighNodeUtilizationPluginName = "HighNodeUtilization"

// HighNodeUtilization evicts pods from under utilized nodes so that scheduler can schedule according to its plugin.
// Note that CPU/Memory requests are used to calculate nodes' utilization and not the actual resource usage.

type HighNodeUtilization struct {
	handle                   frameworktypes.Handle
	args                     *HighNodeUtilizationArgs
	podFilter                func(pod *v1.Pod) bool
	underutilizationCriteria []interface{}
	resourceNames            []v1.ResourceName
	targetThresholds         api.ResourceThresholds
	usageClient              usageclients.Interface
}

var _ frameworktypes.BalancePlugin = &HighNodeUtilization{}

// NewHighNodeUtilization builds plugin from its arguments while passing a handle
func NewHighNodeUtilization(args runtime.Object, handle frameworktypes.Handle) (frameworktypes.Plugin, error) {
	highNodeUtilizatioArgs, ok := args.(*HighNodeUtilizationArgs)
	if !ok {
		return nil, fmt.Errorf("want args to be of type HighNodeUtilizationArgs, got %T", args)
	}

	targetThresholds := make(api.ResourceThresholds)
	setDefaultForThresholds(highNodeUtilizatioArgs.Thresholds, targetThresholds)
	resourceNames := getResourceNames(targetThresholds)

	underutilizationCriteria := []interface{}{
		"CPU", highNodeUtilizatioArgs.Thresholds[v1.ResourceCPU],
		"Mem", highNodeUtilizatioArgs.Thresholds[v1.ResourceMemory],
		"Pods", highNodeUtilizatioArgs.Thresholds[v1.ResourcePods],
	}
	for name := range highNodeUtilizatioArgs.Thresholds {
		if !nodeutil.IsBasicResource(name) {
			underutilizationCriteria = append(underutilizationCriteria, string(name), int64(highNodeUtilizatioArgs.Thresholds[name]))
		}
	}

	podFilter, err := podutil.NewOptions().
		WithFilter(handle.Evictor().Filter).
		BuildFilterFunc()
	if err != nil {
		return nil, fmt.Errorf("error initializing pod filter function: %v", err)
	}

	return &HighNodeUtilization{
		handle:                   handle,
		args:                     highNodeUtilizatioArgs,
		resourceNames:            resourceNames,
		targetThresholds:         targetThresholds,
		underutilizationCriteria: underutilizationCriteria,
		podFilter:                podFilter,
		usageClient:              usageclients.NewRequestedUsageClient(resourceNames, handle.GetPodsAssignedToNodeFunc()),
	}, nil
}

// Name retrieves the plugin name
func (h *HighNodeUtilization) Name() string {
	return HighNodeUtilizationPluginName
}

// Balance extension point implementation for the plugin
func (h *HighNodeUtilization) Balance(ctx context.Context, nodes []*v1.Node) *frameworktypes.Status {
	if err := h.usageClient.Sync(nodes); err != nil {
		return &frameworktypes.Status{
			Err: fmt.Errorf("error getting node usage: %v", err),
		}
	}

	thresholdsProcessor := thresholds.NewNodeProcessor(
		nodes,
		h.args.Thresholds,
		h.targetThresholds,
		h.resourceNames,
		false,
		h.usageClient,
	)

	lowNodes, highNodes := []NodeInfo{}, []NodeInfo{}
	thresholdsProcessor.Classify(
		func(usage usageclients.NodeUsage, threshold thresholds.NodeThresholds) {
			if !thresholdsProcessor.IsNodeWithLowUtilization(usage, threshold.Low) {
				return
			}
			lowNodes = append(lowNodes, NodeInfo{usage, threshold})
		},
		func(usage usageclients.NodeUsage, threshold thresholds.NodeThresholds) {
			if nodeutil.IsNodeUnschedulable(usage.Node) {
				return
			}
			if thresholdsProcessor.IsNodeWithLowUtilization(usage, threshold.Low) {
				return
			}
			highNodes = append(highNodes, NodeInfo{usage, threshold})
		},
	)

	// log message in one line
	klog.V(1).InfoS("Criteria for a node below target utilization", h.underutilizationCriteria...)
	klog.V(1).InfoS("Number of underutilized nodes", "totalNumber", len(lowNodes))

	if len(lowNodes) == 0 {
		klog.V(1).InfoS("No node is underutilized, nothing to do here, you might tune your thresholds further")
		return nil
	}
	if len(lowNodes) <= h.args.NumberOfNodes {
		klog.V(1).InfoS("Number of nodes underutilized is less or equal than NumberOfNodes, nothing to do here", "underutilizedNodes", len(lowNodes), "numberOfNodes", h.args.NumberOfNodes)
		return nil
	}
	if len(lowNodes) == len(nodes) {
		klog.V(1).InfoS("All nodes are underutilized, nothing to do here")
		return nil
	}
	if len(highNodes) == 0 {
		klog.V(1).InfoS("No node is available to schedule the pods, nothing to do here")
		return nil
	}

	// stop if the total available usage has dropped to zero - no more pods can be scheduled
	continueEvictionCond := func(nodeInfo NodeInfo, totalAvailableUsage map[v1.ResourceName]*resource.Quantity) bool {
		for name := range totalAvailableUsage {
			if totalAvailableUsage[name].CmpInt64(0) < 1 {
				return false
			}
		}

		return true
	}

	// Sort the nodes by the usage in ascending order
	sortNodesByUsage(lowNodes, true)

	evictPodsFromSourceNodes(
		ctx,
		h.args.EvictableNamespaces,
		lowNodes,
		highNodes,
		h.handle.Evictor(),
		evictions.EvictOptions{StrategyName: HighNodeUtilizationPluginName},
		h.podFilter,
		h.resourceNames,
		continueEvictionCond,
		h.usageClient,
	)

	return nil
}

func setDefaultForThresholds(curThresholds, targetThresholds api.ResourceThresholds) {
	// check if Pods/CPU/Mem are set, if not, set them to 100
	if _, ok := curThresholds[v1.ResourcePods]; !ok {
		curThresholds[v1.ResourcePods] = thresholds.MaxResourcePercentage
	}
	if _, ok := curThresholds[v1.ResourceCPU]; !ok {
		curThresholds[v1.ResourceCPU] = thresholds.MaxResourcePercentage
	}
	if _, ok := curThresholds[v1.ResourceMemory]; !ok {
		curThresholds[v1.ResourceMemory] = thresholds.MaxResourcePercentage
	}

	// Default targetThreshold resource values to 100
	targetThresholds[v1.ResourcePods] = thresholds.MaxResourcePercentage
	targetThresholds[v1.ResourceCPU] = thresholds.MaxResourcePercentage
	targetThresholds[v1.ResourceMemory] = thresholds.MaxResourcePercentage

	for name := range curThresholds {
		if !nodeutil.IsBasicResource(name) {
			targetThresholds[name] = thresholds.MaxResourcePercentage
		}
	}
}
