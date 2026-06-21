package tetragonreceiver

import (
	"testing"

	"github.com/go-faster/tetragon/api/v1/tetragon"
	"github.com/stretchr/testify/assert"
	semconv "go.opentelemetry.io/otel/semconv/v1.26.0"
	"google.golang.org/protobuf/types/known/timestamppb"
	"google.golang.org/protobuf/types/known/wrapperspb"
)

func TestTranslateEvent_ProcessExec(t *testing.T) {
	resp := &tetragon.GetEventsResponse{
		Event: &tetragon.GetEventsResponse_ProcessExec{
			ProcessExec: &tetragon.ProcessExec{
				Process: &tetragon.Process{
					Pid:       &wrapperspb.UInt32Value{Value: 1001},
					Uid:       &wrapperspb.UInt32Value{Value: 0},
					Binary:    "/usr/bin/curl",
					Arguments: "curl https://example.com",
					ExecId:    "exec-1",
					Cwd:       "/home/user",
					Flags:     "execve",
					Docker:    "abc123",
					StartTime: &timestamppb.Timestamp{Seconds: 1000},
					Pod: &tetragon.Pod{
						Namespace: "default",
						Name:      "app-pod",
						Container: &tetragon.Container{
							Name: "app-container",
							Image: &tetragon.Image{
								Id: "sha256:abc123",
							},
						},
					},
				},
				Parent: &tetragon.Process{
					Pid:    &wrapperspb.UInt32Value{Value: 1},
					Binary: "/sbin/init",
				},
			},
		},
		ClusterName: "test-cluster",
		NodeName:    "node-1",
		Time:        &timestamppb.Timestamp{Seconds: 1000},
	}

	logs, ok := translateEvent(resp, &Config{ClusterID: 42})
	assert.True(t, ok)
	assert.Equal(t, 1, logs.ResourceLogs().Len())

	rl := logs.ResourceLogs().At(0)
	res := rl.Resource().Attributes()

	assert.Equal(t, "default", res.AsRaw()[string(semconv.K8SNamespaceNameKey)])
	assert.Equal(t, "app-pod", res.AsRaw()[string(semconv.K8SPodNameKey)])
	assert.Equal(t, "test-cluster", res.AsRaw()[string(semconv.K8SClusterNameKey)])
	assert.Equal(t, "42", res.AsRaw()["tetragon.cluster.id"])

	assert.Equal(t, 1, rl.ScopeLogs().Len())
	lr := rl.ScopeLogs().At(0).LogRecords().At(0)
	attrs := lr.Attributes().AsRaw()

	assert.Equal(t, "process_exec", attrs["event.name"])
	assert.Equal(t, "node-1", attrs["tetragon.node_name"])
	assert.Equal(t, int64(1001), attrs["process.pid"])
	assert.Equal(t, "/usr/bin/curl", attrs["process.executable.path"])
	assert.Equal(t, "curl https://example.com", attrs["process.command_args"])
	assert.Equal(t, "app-container", attrs[string(semconv.K8SContainerNameKey)])
	assert.Equal(t, "sha256:abc123", attrs["container.image.id"])
	assert.Equal(t, "Tetragon process exec event", lr.Body().AsString())

	assert.Equal(t, int64(1), attrs["tetragon.parent.process.pid"])
	assert.Equal(t, "/sbin/init", attrs["tetragon.parent.process.executable.path"])
}

func TestTranslateEvent_DisableEventDescription(t *testing.T) {
	resp := &tetragon.GetEventsResponse{
		Event: &tetragon.GetEventsResponse_ProcessExit{
			ProcessExit: &tetragon.ProcessExit{
				Process: &tetragon.Process{
					Pod: &tetragon.Pod{Container: &tetragon.Container{}},
				},
			},
		},
		Time: &timestamppb.Timestamp{Seconds: 1000},
	}

	logs, ok := translateEvent(resp, &Config{DisableEventDescription: true})
	assert.True(t, ok)
	lr := logs.ResourceLogs().At(0).ScopeLogs().At(0).LogRecords().At(0)
	assert.Empty(t, lr.Body().AsString())
}

func TestTranslateEvent_ProcessExec_WithAncestors(t *testing.T) {
	resp := &tetragon.GetEventsResponse{
		Event: &tetragon.GetEventsResponse_ProcessExec{
			ProcessExec: &tetragon.ProcessExec{
				Process: &tetragon.Process{
					Pid:    &wrapperspb.UInt32Value{Value: 2001},
					Binary: "/bin/bash",
					Pod: &tetragon.Pod{
						Namespace: "default",
						Name:      "shell-pod",
						Container: &tetragon.Container{Name: "shell"},
					},
				},
				Ancestors: []*tetragon.Process{
					{Pid: &wrapperspb.UInt32Value{Value: 1}, Binary: "/sbin/init"},
				},
			},
		},
		Time: &timestamppb.Timestamp{Seconds: 1000},
	}

	logs, ok := translateEvent(resp, &Config{})
	assert.True(t, ok)
	attrs := logs.ResourceLogs().At(0).ScopeLogs().At(0).LogRecords().At(0).Attributes().AsRaw()
	assert.Contains(t, attrs, "tetragon.ancestors_json")
}

func TestTranslateEvent_ProcessExit(t *testing.T) {
	resp := &tetragon.GetEventsResponse{
		Event: &tetragon.GetEventsResponse_ProcessExit{
			ProcessExit: &tetragon.ProcessExit{
				Process: &tetragon.Process{
					Pid:    &wrapperspb.UInt32Value{Value: 1001},
					Binary: "/usr/bin/curl",
					Pod: &tetragon.Pod{
						Namespace: "default",
						Name:      "app-pod",
						Container: &tetragon.Container{Name: "app"},
					},
				},
			},
		},
		Time: &timestamppb.Timestamp{Seconds: 1000},
	}

	logs, ok := translateEvent(resp, &Config{})
	assert.True(t, ok)
	lr := logs.ResourceLogs().At(0).ScopeLogs().At(0).LogRecords().At(0)
	assert.Equal(t, "process_exit", lr.Attributes().AsRaw()["event.name"])
	assert.Equal(t, "INFO", lr.SeverityText())
}

func TestTranslateEvent_ProcessKprobe(t *testing.T) {
	resp := &tetragon.GetEventsResponse{
		Event: &tetragon.GetEventsResponse_ProcessKprobe{
			ProcessKprobe: &tetragon.ProcessKprobe{
				Process: &tetragon.Process{
					Pid:    &wrapperspb.UInt32Value{Value: 1001},
					Binary: "/usr/bin/curl",
					Pod: &tetragon.Pod{
						Namespace: "default",
						Name:      "app-pod",
						Container: &tetragon.Container{Name: "app"},
					},
				},
				FunctionName: "sys_write",
			},
		},
		Time: &timestamppb.Timestamp{Seconds: 1000},
	}

	logs, ok := translateEvent(resp, &Config{})
	assert.True(t, ok)
	lr := logs.ResourceLogs().At(0).ScopeLogs().At(0).LogRecords().At(0)
	assert.Equal(t, "process_kprobe", lr.Attributes().AsRaw()["event.name"])
	assert.Equal(t, "sys_write", lr.Attributes().AsRaw()["tetragon.kprobe.function_name"])
	assert.Equal(t, "DEBUG", lr.SeverityText())
}

func TestTranslateEvent_ProcessTracepoint(t *testing.T) {
	resp := &tetragon.GetEventsResponse{
		Event: &tetragon.GetEventsResponse_ProcessTracepoint{
			ProcessTracepoint: &tetragon.ProcessTracepoint{
				Process: &tetragon.Process{
					Binary: "/usr/bin/curl",
					Pod: &tetragon.Pod{
						Namespace: "default",
						Name:      "app-pod",
						Container: &tetragon.Container{Name: "app"},
					},
				},
			},
		},
		Time: &timestamppb.Timestamp{Seconds: 1000},
	}

	logs, ok := translateEvent(resp, &Config{})
	assert.True(t, ok)
	lr := logs.ResourceLogs().At(0).ScopeLogs().At(0).LogRecords().At(0)
	assert.Equal(t, "process_tracepoint", lr.Attributes().AsRaw()["event.name"])
	assert.Equal(t, "DEBUG", lr.SeverityText())
}

func TestTranslateEvent_ProcessLoader(t *testing.T) {
	resp := &tetragon.GetEventsResponse{
		Event: &tetragon.GetEventsResponse_ProcessLoader{
			ProcessLoader: &tetragon.ProcessLoader{
				Process: &tetragon.Process{
					Binary: "/usr/lib/ld-linux.so",
					Pod: &tetragon.Pod{
						Namespace: "default",
						Name:      "app-pod",
						Container: &tetragon.Container{Name: "app"},
					},
				},
			},
		},
		Time: &timestamppb.Timestamp{Seconds: 1000},
	}

	logs, ok := translateEvent(resp, &Config{})
	assert.True(t, ok)
	attrs := logs.ResourceLogs().At(0).ScopeLogs().At(0).LogRecords().At(0).Attributes().AsRaw()
	assert.Equal(t, "process_loader", attrs["event.name"])
}

func TestTranslateEvent_Unknown(t *testing.T) {
	resp := &tetragon.GetEventsResponse{
		Event: nil,
		Time:  &timestamppb.Timestamp{Seconds: 1000},
	}
	_, ok := translateEvent(resp, &Config{})
	assert.False(t, ok)
}
