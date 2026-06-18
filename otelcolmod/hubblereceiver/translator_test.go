package hubblereceiver

import (
	"testing"

	"github.com/cilium/cilium/api/v1/flow"
	"github.com/cilium/cilium/api/v1/observer"
	"github.com/stretchr/testify/assert"
	semconv "go.opentelemetry.io/otel/semconv/v1.26.0"
	"google.golang.org/protobuf/types/known/timestamppb"
	"google.golang.org/protobuf/types/known/wrapperspb"
)

func TestTranslateFlow_Base(t *testing.T) {
	cfg := &Config{ClusterName: "test-cluster", ClusterID: 42}
	resp := &observer.GetFlowsResponse{
		ResponseTypes: &observer.GetFlowsResponse_Flow{
			Flow: &flow.Flow{
				Time:     &timestamppb.Timestamp{Seconds: 1000},
				Verdict:  flow.Verdict_FORWARDED,
				Type:     flow.FlowType_L3_L4,
				NodeName: "node-1",
				Source: &flow.Endpoint{
					Namespace: "default",
					PodName:   "src-pod",
				},
			},
		},
	}

	logs := translateFlow(resp, cfg)
	assert.Equal(t, 1, logs.ResourceLogs().Len())

	rl := logs.ResourceLogs().At(0)
	res := rl.Resource().Attributes()
	assert.Equal(t, "default", res.AsRaw()[string(semconv.K8SNamespaceNameKey)])
	assert.Equal(t, "src-pod", res.AsRaw()[string(semconv.K8SPodNameKey)])
	assert.Equal(t, "test-cluster", res.AsRaw()[string(semconv.K8SClusterNameKey)])
	assert.Equal(t, "42", res.AsRaw()["hubble.cluster.id"])

	assert.Equal(t, 1, rl.ScopeLogs().Len())
	lr := rl.ScopeLogs().At(0).LogRecords().At(0)
	// 1000 seconds = 1,000,000,000,000 nanoseconds
	assert.Equal(t, uint64(1000000000000), uint64(lr.Timestamp()))
	assert.Equal(t, "INFO", lr.SeverityText())
}

func TestTranslateFlow_NilFlow(t *testing.T) {
	resp := &observer.GetFlowsResponse{}
	logs := translateFlow(resp, &Config{})
	rl := logs.ResourceLogs().At(0)
	// Nil flow still produces a log record with default values (safe proto getters).
	assert.Equal(t, 1, rl.ScopeLogs().At(0).LogRecords().Len())
}

func TestTranslateFlow_DroppedVerdict(t *testing.T) {
	cfg := &Config{}
	resp := &observer.GetFlowsResponse{
		ResponseTypes: &observer.GetFlowsResponse_Flow{
			Flow: &flow.Flow{
				Time:    &timestamppb.Timestamp{Seconds: 1000},
				Verdict: flow.Verdict_DROPPED,
				Type:    flow.FlowType_L3_L4,
				Source:  &flow.Endpoint{},
			},
		},
	}

	logs := translateFlow(resp, cfg)
	lr := logs.ResourceLogs().At(0).ScopeLogs().At(0).LogRecords().At(0)
	assert.Equal(t, "WARN", lr.SeverityText())
	assert.Equal(t, "DROPPED", lr.Attributes().AsRaw()["hubble.verdict"])
}

func TestTranslateFlow_L7HTTP(t *testing.T) {
	cfg := &Config{}
	resp := &observer.GetFlowsResponse{
		ResponseTypes: &observer.GetFlowsResponse_Flow{
			Flow: &flow.Flow{
				Time:   &timestamppb.Timestamp{Seconds: 1000},
				Type:   flow.FlowType_L7,
				Source: &flow.Endpoint{},
				L7: &flow.Layer7{
					LatencyNs: 5000,
					Record: &flow.Layer7_Http{
						Http: &flow.HTTP{
							Method:   "GET",
							Url:      "/api/v1/foo",
							Code:     200,
							Protocol: "HTTP/2",
						},
					},
				},
			},
		},
	}

	logs := translateFlow(resp, cfg)
	lr := logs.ResourceLogs().At(0).ScopeLogs().At(0).LogRecords().At(0)
	attrs := lr.Attributes().AsRaw()
	assert.Equal(t, "GET", attrs["http.request.method"])
	assert.Equal(t, "/api/v1/foo", attrs["url.full"])
	assert.Equal(t, int64(200), attrs["http.response.status_code"])
	assert.Equal(t, int64(5000), attrs["hubble.l7.latency_ns"])
}

func TestTranslateFlow_L7DNS(t *testing.T) {
	cfg := &Config{}
	resp := &observer.GetFlowsResponse{
		ResponseTypes: &observer.GetFlowsResponse_Flow{
			Flow: &flow.Flow{
				Time:   &timestamppb.Timestamp{Seconds: 1000},
				Type:   flow.FlowType_L7,
				Source: &flow.Endpoint{},
				L7: &flow.Layer7{
					Record: &flow.Layer7_Dns{
						Dns: &flow.DNS{
							Query: "example.com",
							Rcode: 0,
							Ips:   []string{"1.2.3.4", "5.6.7.8"},
						},
					},
				},
			},
		},
	}

	logs := translateFlow(resp, cfg)
	lr := logs.ResourceLogs().At(0).ScopeLogs().At(0).LogRecords().At(0)
	attrs := lr.Attributes().AsRaw()
	assert.Equal(t, "example.com", attrs["dns.question.name"])
	assert.Equal(t, int64(0), attrs["hubble.dns.response_code"])
}

func TestTranslateFlow_L4(t *testing.T) {
	cfg := &Config{}
	resp := &observer.GetFlowsResponse{
		ResponseTypes: &observer.GetFlowsResponse_Flow{
			Flow: &flow.Flow{
				Time:   &timestamppb.Timestamp{Seconds: 1000},
				Type:   flow.FlowType_L3_L4,
				Source: &flow.Endpoint{},
				L4: &flow.Layer4{
					Protocol: &flow.Layer4_TCP{
						TCP: &flow.TCP{
							SourcePort:      1234,
							DestinationPort: 80,
						},
					},
				},
				IP: &flow.IP{
					Source:      "10.0.0.1",
					Destination: "10.0.0.2",
					IpVersion:   flow.IPVersion_IPv4,
				},
			},
		},
	}

	logs := translateFlow(resp, cfg)
	lr := logs.ResourceLogs().At(0).ScopeLogs().At(0).LogRecords().At(0)
	attrs := lr.Attributes().AsRaw()
	assert.Equal(t, "tcp", attrs["network.transport"])
	assert.Equal(t, int64(1234), attrs["network.source.port"])
	assert.Equal(t, int64(80), attrs["network.destination.port"])
	assert.Equal(t, "10.0.0.1", attrs["network.source.address"])
	assert.Equal(t, "10.0.0.2", attrs["network.destination.address"])
	assert.Equal(t, "ipv4", attrs["network.type"])
}

func TestTranslateFlow_IsReply(t *testing.T) {
	cfg := &Config{}
	resp := &observer.GetFlowsResponse{
		ResponseTypes: &observer.GetFlowsResponse_Flow{
			Flow: &flow.Flow{
				Time:    &timestamppb.Timestamp{Seconds: 1000},
				Type:    flow.FlowType_L3_L4,
				Source:  &flow.Endpoint{},
				IsReply: &wrapperspb.BoolValue{Value: true},
			},
		},
	}

	logs := translateFlow(resp, cfg)
	lr := logs.ResourceLogs().At(0).ScopeLogs().At(0).LogRecords().At(0)
	assert.Equal(t, true, lr.Attributes().AsRaw()["hubble.is_reply"])
}

func TestTranslateFlow_TraceContext(t *testing.T) {
	cfg := &Config{}
	resp := &observer.GetFlowsResponse{
		ResponseTypes: &observer.GetFlowsResponse_Flow{
			Flow: &flow.Flow{
				Time:   &timestamppb.Timestamp{Seconds: 1000},
				Type:   flow.FlowType_L3_L4,
				Source: &flow.Endpoint{},
				TraceContext: &flow.TraceContext{
					Parent: &flow.TraceParent{
						TraceId: "abc123",
					},
				},
			},
		},
	}

	logs := translateFlow(resp, cfg)
	lr := logs.ResourceLogs().At(0).ScopeLogs().At(0).LogRecords().At(0)
	assert.Equal(t, "abc123", lr.Attributes().AsRaw()["trace_id"])
}

func TestTranslateFlow_Destination(t *testing.T) {
	cfg := &Config{}
	resp := &observer.GetFlowsResponse{
		ResponseTypes: &observer.GetFlowsResponse_Flow{
			Flow: &flow.Flow{
				Time:   &timestamppb.Timestamp{Seconds: 1000},
				Type:   flow.FlowType_L3_L4,
				Source: &flow.Endpoint{},
				Destination: &flow.Endpoint{
					Namespace: "kube-system",
					PodName:   "coredns-123",
					Labels:    []string{"app=coredns", "k8s-app=coredns"},
					Workloads: []*flow.Workload{
						{Name: "coredns", Kind: "Deployment"},
					},
				},
			},
		},
	}

	logs := translateFlow(resp, cfg)
	lr := logs.ResourceLogs().At(0).ScopeLogs().At(0).LogRecords().At(0)
	attrs := lr.Attributes().AsRaw()
	assert.Equal(t, "kube-system", attrs["hubble.dst.namespace"])
	assert.Equal(t, "coredns-123", attrs["hubble.dst.pod"])
}

func TestTranslateFlow_Interface(t *testing.T) {
	cfg := &Config{}
	resp := &observer.GetFlowsResponse{
		ResponseTypes: &observer.GetFlowsResponse_Flow{
			Flow: &flow.Flow{
				Time:   &timestamppb.Timestamp{Seconds: 1000},
				Type:   flow.FlowType_L3_L4,
				Source: &flow.Endpoint{},
				Interface: &flow.NetworkInterface{
					Name:  "eth0",
					Index: 3,
				},
			},
		},
	}

	logs := translateFlow(resp, cfg)
	lr := logs.ResourceLogs().At(0).ScopeLogs().At(0).LogRecords().At(0)
	attrs := lr.Attributes().AsRaw()
	assert.Equal(t, "eth0", attrs["hubble.interface.name"])
	assert.Equal(t, int64(3), attrs["hubble.interface.index"])
}
