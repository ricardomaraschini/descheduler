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

const LowNodeUtilizationPluginName = "LowNodeUtilization"

// LowNodeUtilization evicts pods from overutilized nodes to underutilized nodes. Note that CPU/Memory requests are used
// to calculate nodes' utilization and not the actual resource usage.

type LowNodeUtilization struct {
	handle                   frameworktypes.Handle
	args                     *LowNodeUtilizationArgs
	podFilter                func(pod *v1.Pod) bool
	underutilizationCriteria []interface{}
	overutilizationCriteria  []interface{}
	resourceNames            []v1.ResourceName
	usageClient              usageclients.Interface
}

var _ frameworktypes.BalancePlugin = &LowNodeUtilization{}

// NewLowNodeUtilization builds plugin from its arguments while passing a handle
func NewLowNodeUtilization(args runtime.Object, handle frameworktypes.Handle) (frameworktypes.Plugin, error) {
	lowNodeUtilizationArgsArgs, ok := args.(*LowNodeUtilizationArgs)
	if !ok {
		return nil, fmt.Errorf("want args to be of type LowNodeUtilizationArgs, got %T", args)
	}

	if lowNodeUtilizationArgsArgs.MetricsUtilization.Prometheus.Query != "" {
		uResourceNames := getResourceNames(lowNodeUtilizationArgsArgs.Thresholds)
		oResourceNames := getResourceNames(lowNodeUtilizationArgsArgs.TargetThresholds)
		if len(uResourceNames) != 1 || uResourceNames[0] != usageclients.MetricResource {
			return nil, fmt.Errorf("thresholds are expected to specify a single instance of %q resource, got %v instead", usageclients.MetricResource, uResourceNames)
		}
		if len(oResourceNames) != 1 || oResourceNames[0] != usageclients.MetricResource {
			return nil, fmt.Errorf("targetThresholds are expected to specify a single instance of %q resource, got %v instead", usageclients.MetricResource, oResourceNames)
		}
	} else {
		setDefaultForLNUThresholds(lowNodeUtilizationArgsArgs.Thresholds, lowNodeUtilizationArgsArgs.TargetThresholds, lowNodeUtilizationArgsArgs.UseDeviationThresholds)
	}

	underutilizationCriteria := []interface{}{
		"CPU", lowNodeUtilizationArgsArgs.Thresholds[v1.ResourceCPU],
		"Mem", lowNodeUtilizationArgsArgs.Thresholds[v1.ResourceMemory],
		"Pods", lowNodeUtilizationArgsArgs.Thresholds[v1.ResourcePods],
	}
	for name := range lowNodeUtilizationArgsArgs.Thresholds {
		if !nodeutil.IsBasicResource(name) {
			underutilizationCriteria = append(underutilizationCriteria, string(name), int64(lowNodeUtilizationArgsArgs.Thresholds[name]))
		}
	}

	overutilizationCriteria := []interface{}{
		"CPU", lowNodeUtilizationArgsArgs.TargetThresholds[v1.ResourceCPU],
		"Mem", lowNodeUtilizationArgsArgs.TargetThresholds[v1.ResourceMemory],
		"Pods", lowNodeUtilizationArgsArgs.TargetThresholds[v1.ResourcePods],
	}
	for name := range lowNodeUtilizationArgsArgs.TargetThresholds {
		if !nodeutil.IsBasicResource(name) {
			overutilizationCriteria = append(overutilizationCriteria, string(name), int64(lowNodeUtilizationArgsArgs.TargetThresholds[name]))
		}
	}

	podFilter, err := podutil.NewOptions().
		WithFilter(handle.Evictor().Filter).
		BuildFilterFunc()
	if err != nil {
		return nil, fmt.Errorf("error initializing pod filter function: %v", err)
	}

	resourceNames := getResourceNames(lowNodeUtilizationArgsArgs.Thresholds)

	var usageClient usageclients.Interface
	if lowNodeUtilizationArgsArgs.MetricsUtilization.MetricsServer {
		if handle.MetricsCollector() == nil {
			return nil, fmt.Errorf("metrics client not initialized")
		}
		usageClient = usageclients.NewActualUsageClient(resourceNames, handle.GetPodsAssignedToNodeFunc(), handle.MetricsCollector())
	} else if lowNodeUtilizationArgsArgs.MetricsUtilization.Prometheus.Query != "" {
		if handle.PrometheusClient() == nil {
			return nil, fmt.Errorf("prometheus client not initialized")
		}
		usageClient = usageclients.NewPrometheusUsageClient(handle.GetPodsAssignedToNodeFunc(), handle.PrometheusClient(), lowNodeUtilizationArgsArgs.MetricsUtilization.Prometheus.Query)
	} else {
		usageClient = usageclients.NewRequestedUsageClient(resourceNames, handle.GetPodsAssignedToNodeFunc())
	}

	return &LowNodeUtilization{
		handle:                   handle,
		args:                     lowNodeUtilizationArgsArgs,
		underutilizationCriteria: underutilizationCriteria,
		overutilizationCriteria:  overutilizationCriteria,
		resourceNames:            resourceNames,
		podFilter:                podFilter,
		usageClient:              usageClient,
	}, nil
}

// Name retrieves the plugin name
func (l *LowNodeUtilization) Name() string {
	return LowNodeUtilizationPluginName
}

// Balance extension point implementation for the plugin
func (l *LowNodeUtilization) Balance(ctx context.Context, nodes []*v1.Node) *frameworktypes.Status {
	if err := l.usageClient.Sync(nodes); err != nil {
		return &frameworktypes.Status{
			Err: fmt.Errorf("error getting node usage: %v", err),
		}
	}

	thresholdsProcessor := thresholds.NewNodeProcessor(
		nodes,
		l.args.Thresholds,
		l.args.TargetThresholds,
		l.resourceNames,
		l.args.UseDeviationThresholds,
		l.usageClient,
	)

	lowNodes, highNodes := []NodeInfo{}, []NodeInfo{}
	thresholdsProcessor.Classify(
		func(usage usageclients.NodeUsage, threshold thresholds.NodeThresholds) {
			if nodeutil.IsNodeUnschedulable(usage.Node) {
				klog.V(2).InfoS("Node is unschedulable, thus not considered as underutilized", "node", klog.KObj(usage.Node))
				return
			}
			if !thresholdsProcessor.IsNodeWithLowUtilization(usage, threshold.Low) {
				return
			}
			lowNodes = append(lowNodes, NodeInfo{usage, threshold})
		},
		func(usage usageclients.NodeUsage, threshold thresholds.NodeThresholds) {
			if thresholdsProcessor.IsNodeWithHighUtilization(usage, threshold.High) {
				highNodes = append(highNodes, NodeInfo{usage, threshold})
			}
		},
	)

	// log message for nodes with low utilization
	klog.V(1).InfoS("Criteria for a node under utilization", l.underutilizationCriteria...)
	klog.V(1).InfoS("Number of underutilized nodes", "totalNumber", len(lowNodes))

	// log message for over utilized nodes
	klog.V(1).InfoS("Criteria for a node above target utilization", l.overutilizationCriteria...)
	klog.V(1).InfoS("Number of overutilized nodes", "totalNumber", len(highNodes))

	if len(lowNodes) == 0 {
		klog.V(1).InfoS("No node is underutilized, nothing to do here, you might tune your thresholds further")
		return nil
	}

	if len(lowNodes) <= l.args.NumberOfNodes {
		klog.V(1).InfoS("Number of nodes underutilized is less or equal than NumberOfNodes, nothing to do here", "underutilizedNodes", len(lowNodes), "numberOfNodes", l.args.NumberOfNodes)
		return nil
	}

	if len(lowNodes) == len(nodes) {
		klog.V(1).InfoS("All nodes are underutilized, nothing to do here")
		return nil
	}

	if len(highNodes) == 0 {
		klog.V(1).InfoS("All nodes are under target utilization, nothing to do here")
		return nil
	}

	// stop if node utilization drops below target threshold or any of required capacity (cpu, memory, pods) is moved
	continueEvictionCond := func(nodeInfo NodeInfo, totalAvailableUsage map[v1.ResourceName]*resource.Quantity) bool {
		if !isNodeAboveTargetUtilization(nodeInfo.NodeUsage, nodeInfo.thresholds.High) {
			return false
		}
		for name := range totalAvailableUsage {
			if totalAvailableUsage[name].CmpInt64(0) < 1 {
				return false
			}
		}

		return true
	}

	// Sort the nodes by the usage in descending order
	sortNodesByUsage(highNodes, false)

	evictPodsFromSourceNodes(
		ctx,
		l.args.EvictableNamespaces,
		highNodes,
		lowNodes,
		l.handle.Evictor(),
		evictions.EvictOptions{StrategyName: LowNodeUtilizationPluginName},
		l.podFilter,
		l.resourceNames,
		continueEvictionCond,
		l.usageClient,
	)

	return nil
}

func setDefaultForLNUThresholds(curThresholds, targetThresholds api.ResourceThresholds, useDeviationThresholds bool) {
	// check if Pods/CPU/Mem are set, if not, set them to 100
	if _, ok := curThresholds[v1.ResourcePods]; !ok {
		if useDeviationThresholds {
			curThresholds[v1.ResourcePods] = thresholds.MinResourcePercentage
			targetThresholds[v1.ResourcePods] = thresholds.MinResourcePercentage
		} else {
			curThresholds[v1.ResourcePods] = thresholds.MaxResourcePercentage
			targetThresholds[v1.ResourcePods] = thresholds.MaxResourcePercentage
		}
	}
	if _, ok := curThresholds[v1.ResourceCPU]; !ok {
		if useDeviationThresholds {
			curThresholds[v1.ResourceCPU] = thresholds.MinResourcePercentage
			targetThresholds[v1.ResourceCPU] = thresholds.MinResourcePercentage
		} else {
			curThresholds[v1.ResourceCPU] = thresholds.MaxResourcePercentage
			targetThresholds[v1.ResourceCPU] = thresholds.MaxResourcePercentage
		}
	}
	if _, ok := curThresholds[v1.ResourceMemory]; !ok {
		if useDeviationThresholds {
			curThresholds[v1.ResourceMemory] = thresholds.MinResourcePercentage
			targetThresholds[v1.ResourceMemory] = thresholds.MinResourcePercentage
		} else {
			curThresholds[v1.ResourceMemory] = thresholds.MaxResourcePercentage
			targetThresholds[v1.ResourceMemory] = thresholds.MaxResourcePercentage
		}
	}
}
