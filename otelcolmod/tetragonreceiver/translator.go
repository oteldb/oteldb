package tetragonreceiver

import (
	"encoding/json"
	"strconv"
	"time"

	"github.com/go-faster/tetragon/api/v1/tetragon"
	"go.opentelemetry.io/collector/pdata/pcommon"
	"go.opentelemetry.io/collector/pdata/plog"
	semconv "go.opentelemetry.io/otel/semconv/v1.26.0"
)

func translateEvent(r *tetragon.GetEventsResponse, clusterID int64) (plog.Logs, bool) {
	logs := plog.NewLogs()
	rl := logs.ResourceLogs().AppendEmpty()

	var process, parent *tetragon.Process
	var eventName string
	var extraAttrs func(pcommon.Map)

	switch v := r.Event.(type) {
	case *tetragon.GetEventsResponse_ProcessExec:
		e := v.ProcessExec
		process = e.GetProcess()
		parent = e.GetParent()
		eventName = "process_exec"
		if anc := e.GetAncestors(); len(anc) > 0 {
			data, _ := json.Marshal(anc)
			extraAttrs = func(m pcommon.Map) {
				m.PutStr("tetragon.ancestors_json", string(data))
			}
		}
	case *tetragon.GetEventsResponse_ProcessExit:
		e := v.ProcessExit
		process = e.GetProcess()
		parent = e.GetParent()
		eventName = "process_exit"
	case *tetragon.GetEventsResponse_ProcessKprobe:
		e := v.ProcessKprobe
		process = e.GetProcess()
		parent = e.GetParent()
		eventName = "process_kprobe"
		fn := e.GetFunctionName()
		extraAttrs = func(m pcommon.Map) {
			m.PutStr("tetragon.kprobe.function_name", fn)
		}
	case *tetragon.GetEventsResponse_ProcessTracepoint:
		e := v.ProcessTracepoint
		process = e.GetProcess()
		parent = e.GetParent()
		eventName = "process_tracepoint"
	case *tetragon.GetEventsResponse_ProcessLoader:
		e := v.ProcessLoader
		process = e.GetProcess()
		eventName = "process_loader"
	default:
		return plog.Logs{}, false
	}

	// Resource attributes.
	res := rl.Resource().Attributes()
	res.PutStr(string(semconv.K8SNamespaceNameKey), process.GetPod().GetNamespace())
	res.PutStr(string(semconv.K8SPodNameKey), process.GetPod().GetName())
	if cn := r.GetClusterName(); cn != "" {
		res.PutStr(string(semconv.K8SClusterNameKey), cn)
	}
	if clusterID != 0 {
		res.PutStr("tetragon.cluster.id", strconv.FormatInt(clusterID, 10))
	}

	sl := rl.ScopeLogs().AppendEmpty()
	lr := sl.LogRecords().AppendEmpty()

	lr.SetTimestamp(pcommon.NewTimestampFromTime(r.GetTime().AsTime()))

	sev, sevText := eventSeverity(eventName)
	lr.SetSeverityNumber(sev)
	lr.SetSeverityText(sevText)

	attrs := lr.Attributes()
	attrs.PutStr("event.name", eventName)
	attrs.PutStr("tetragon.node_name", r.GetNodeName())

	appendProcess(attrs, "", process)
	appendProcess(attrs, "tetragon.parent.", parent)

	pod := process.GetPod()
	attrs.PutStr(string(semconv.K8SContainerNameKey), pod.GetContainer().GetName())
	if img := pod.GetContainer().GetImage().GetId(); img != "" {
		attrs.PutStr("container.image.id", img)
	}

	if extraAttrs != nil {
		extraAttrs(attrs)
	}

	return logs, true
}

func appendProcess(m pcommon.Map, prefix string, p *tetragon.Process) {
	if p == nil {
		return
	}
	m.PutInt(prefix+"process.pid", int64(p.GetPid().GetValue()))
	m.PutStr(prefix+"process.executable.path", p.GetBinary())
	m.PutStr(prefix+"process.command_args", p.GetArguments())
	m.PutInt(prefix+"process.owner.id", int64(p.GetUid().GetValue()))
	m.PutStr(prefix+"tetragon.process.exec_id", p.GetExecId())
	m.PutStr(prefix+"tetragon.process.cwd", p.GetCwd())
	m.PutStr(prefix+"tetragon.process.flags", p.GetFlags())
	m.PutStr(prefix+"tetragon.process.docker", p.GetDocker())
	if t := p.GetStartTime(); t != nil {
		m.PutStr(prefix+"tetragon.process.start_time", t.AsTime().Format(time.RFC3339Nano))
	}
}

func eventSeverity(name string) (sev plog.SeverityNumber, text string) {
	switch name {
	case "process_kprobe", "process_tracepoint":
		return plog.SeverityNumberDebug, "DEBUG"
	default:
		return plog.SeverityNumberInfo, "INFO"
	}
}
