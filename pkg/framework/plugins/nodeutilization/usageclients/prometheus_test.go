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
	"encoding/json"
	"fmt"
	"net/http"
	"net/url"
	"testing"

	"github.com/prometheus/common/model"
	v1 "k8s.io/api/core/v1"
	"k8s.io/client-go/informers"
	fakeclientset "k8s.io/client-go/kubernetes/fake"

	podutil "sigs.k8s.io/descheduler/pkg/descheduler/pod"
	"sigs.k8s.io/descheduler/test"
)

func TestPrometheusUsageClient(t *testing.T) {
	n1 := test.BuildTestNode("ip-10-0-17-165.ec2.internal", 2000, 3000, 10, nil)
	n2 := test.BuildTestNode("ip-10-0-51-101.ec2.internal", 2000, 3000, 10, nil)
	n3 := test.BuildTestNode("ip-10-0-94-25.ec2.internal", 2000, 3000, 10, nil)

	nodes := []*v1.Node{n1, n2, n3}

	p1 := test.BuildTestPod("p1", 400, 0, n1.Name, nil)
	p21 := test.BuildTestPod("p21", 400, 0, n2.Name, nil)
	p22 := test.BuildTestPod("p22", 400, 0, n2.Name, nil)
	p3 := test.BuildTestPod("p3", 400, 0, n3.Name, nil)

	tests := []struct {
		name      string
		result    interface{}
		dataType  model.ValueType
		nodeUsage map[string]int64
		err       error
	}{
		{
			name:     "valid data",
			dataType: model.ValVector,
			result: model.Vector{
				sample("instance:node_cpu:rate:sum", "ip-10-0-51-101.ec2.internal", 0.20381818181818104),
				sample("instance:node_cpu:rate:sum", "ip-10-0-17-165.ec2.internal", 0.4245454545454522),
				sample("instance:node_cpu:rate:sum", "ip-10-0-94-25.ec2.internal", 0.5695757575757561),
			},
			nodeUsage: map[string]int64{
				"ip-10-0-51-101.ec2.internal": 20,
				"ip-10-0-17-165.ec2.internal": 42,
				"ip-10-0-94-25.ec2.internal":  56,
			},
		},
		{
			name:     "invalid data missing instance label",
			dataType: model.ValVector,
			result: model.Vector{
				&model.Sample{
					Metric: model.Metric{
						"__name__": model.LabelValue("instance:node_cpu:rate:sum"),
					},
					Value:     model.SampleValue(0.20381818181818104),
					Timestamp: 1728991761711,
				},
			},
			err: fmt.Errorf("The collected metrics sample is missing 'instance' key"),
		},
		{
			name:     "invalid data value out of range",
			dataType: model.ValVector,
			result: model.Vector{
				sample("instance:node_cpu:rate:sum", "ip-10-0-51-101.ec2.internal", 1.20381818181818104),
			},
			err: fmt.Errorf("The collected metrics sample for \"ip-10-0-51-101.ec2.internal\" has value 1.203818181818181 outside of <0; 1> interval"),
		},
		{
			name:     "invalid data not a vector",
			dataType: model.ValScalar,
			result: model.Scalar{
				Value:     model.SampleValue(0.20381818181818104),
				Timestamp: 1728991761711,
			},
			err: fmt.Errorf("expected query results to be of type \"vector\", got \"scalar\" instead"),
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			pClient := &fakePromClient{
				result:   tc.result,
				dataType: tc.dataType,
			}

			clientset := fakeclientset.NewSimpleClientset(n1, n2, n3, p1, p21, p22, p3)

			ctx := context.TODO()
			sharedInformerFactory := informers.NewSharedInformerFactory(clientset, 0)
			podInformer := sharedInformerFactory.Core().V1().Pods().Informer()
			podsAssignedToNode, err := podutil.BuildGetPodsAssignedToNodeFunc(podInformer)
			if err != nil {
				t.Fatalf("Build get pods assigned to node function error: %v", err)
			}

			sharedInformerFactory.Start(ctx.Done())
			sharedInformerFactory.WaitForCacheSync(ctx.Done())

			prometheusUsageClient := NewPrometheusUsageClient(podsAssignedToNode, pClient, "instance:node_cpu:rate:sum")
			err = prometheusUsageClient.Sync(nodes)
			if tc.err == nil {
				if err != nil {
					t.Fatalf("unexpected error: %v", err)
				}
			} else {
				if err == nil {
					t.Fatalf("unexpected %q error, got nil instead", tc.err)
				} else if err.Error() != tc.err.Error() {
					t.Fatalf("expected %q error, got %q instead", tc.err, err)
				}
				return
			}

			for _, node := range nodes {
				nodeUtil := prometheusUsageClient.NodeUtilization(node.Name)
				if nodeUtil[MetricResource].Value() != tc.nodeUsage[node.Name] {
					t.Fatalf("expected %q node utilization to be %v, got %v instead", node.Name, tc.nodeUsage[node.Name], nodeUtil[MetricResource])
				} else {
					t.Logf("%v node utilization: %v", node.Name, nodeUtil[MetricResource])
				}
			}
		})
	}
}

func sample(metricName, nodeName string, value float64) *model.Sample {
	return &model.Sample{
		Metric: model.Metric{
			"__name__": model.LabelValue(metricName),
			"instance": model.LabelValue(nodeName),
		},
		Value:     model.SampleValue(value),
		Timestamp: 1728991761711,
	}
}

type fakePromClient struct {
	result   interface{}
	dataType model.ValueType
}

func (client *fakePromClient) URL(ep string, args map[string]string) *url.URL {
	return &url.URL{}
}

func (client *fakePromClient) Do(ctx context.Context, request *http.Request) (*http.Response, []byte, error) {
	jsonData, err := json.Marshal(fakePayload{
		Status: "success",
		Data: queryResult{
			Type:   client.dataType,
			Result: client.result,
		},
	})

	return &http.Response{StatusCode: 200}, jsonData, err
}

type fakePayload struct {
	Status string      `json:"status"`
	Data   queryResult `json:"data"`
}

type queryResult struct {
	Type   model.ValueType `json:"resultType"`
	Result interface{}     `json:"result"`
}
