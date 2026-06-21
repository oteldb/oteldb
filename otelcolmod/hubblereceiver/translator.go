package hubblereceiver

import (
	"encoding/hex"
	"strconv"

	"github.com/cilium/cilium/api/v1/flow"
	"github.com/cilium/cilium/api/v1/observer"
	"go.opentelemetry.io/collector/pdata/pcommon"
	"go.opentelemetry.io/collector/pdata/plog"
	semconv "go.opentelemetry.io/otel/semconv/v1.26.0"
)

func translateFlow(resp *observer.GetFlowsResponse, cfg *Config) plog.Logs {
	f := resp.GetFlow()

	logs := plog.NewLogs()
	rl := logs.ResourceLogs().AppendEmpty()

	res := rl.Resource().Attributes()
	res.PutStr(string(semconv.K8SNamespaceNameKey), f.GetSource().GetNamespace())
	res.PutStr(string(semconv.K8SPodNameKey), f.GetSource().GetPodName())
	if cfg.ClusterName != "" {
		res.PutStr(string(semconv.K8SClusterNameKey), cfg.ClusterName)
	}
	if cfg.ClusterID != 0 {
		res.PutStr("hubble.cluster.id", strconv.FormatInt(cfg.ClusterID, 10))
	}

	sl := rl.ScopeLogs().AppendEmpty()
	lr := sl.LogRecords().AppendEmpty()

	lr.SetTimestamp(pcommon.NewTimestampFromTime(f.GetTime().AsTime()))

	sev, sevText := verdictSeverity(f.GetVerdict())
	lr.SetSeverityNumber(sev)
	lr.SetSeverityText(sevText)
	if !cfg.DisableEventDescription {
		lr.Body().SetStr(flowDescription(f))
	}

	attrs := lr.Attributes()

	attrs.PutStr("event.name", "hubble.flow")
	attrs.PutStr("hubble.flow.type", f.GetType().String())
	attrs.PutStr("hubble.verdict", f.GetVerdict().String())
	attrs.PutStr("hubble.node_name", f.GetNodeName())
	attrs.PutStr("hubble.traffic_direction", f.GetTrafficDirection().String())
	attrs.PutStr("hubble.trace_observation_point", f.GetTraceObservationPoint().String())
	if f.GetDropReasonDesc() != 0 {
		attrs.PutStr("hubble.drop_reason", f.GetDropReasonDesc().String())
	}
	if v := f.GetIsReply(); v != nil {
		attrs.PutBool("hubble.is_reply", v.GetValue())
	}

	src := f.GetSource()
	if workloads := src.GetWorkloads(); len(workloads) > 0 {
		names := attrs.PutEmptySlice("hubble.src.workload.names")
		kinds := attrs.PutEmptySlice("hubble.src.workload.kinds")
		for _, w := range workloads {
			names.AppendEmpty().SetStr(w.GetName())
			kinds.AppendEmpty().SetStr(w.GetKind())
		}
	}

	dst := f.GetDestination()
	attrs.PutStr("hubble.dst.namespace", dst.GetNamespace())
	attrs.PutStr("hubble.dst.pod", dst.GetPodName())
	if labels := dst.GetLabels(); len(labels) > 0 {
		ls := attrs.PutEmptySlice("hubble.dst.labels")
		for _, l := range labels {
			ls.AppendEmpty().SetStr(l)
		}
	}
	if workloads := dst.GetWorkloads(); len(workloads) > 0 {
		names := attrs.PutEmptySlice("hubble.dst.workload.names")
		kinds := attrs.PutEmptySlice("hubble.dst.workload.kinds")
		for _, w := range workloads {
			names.AppendEmpty().SetStr(w.GetName())
			kinds.AppendEmpty().SetStr(w.GetKind())
		}
	}

	if ip := f.GetIP(); ip != nil {
		attrs.PutStr("network.source.address", ip.GetSource())
		attrs.PutStr("network.destination.address", ip.GetDestination())
		switch ip.GetIpVersion() {
		case observer.IPVersion_IPv4:
			attrs.PutStr("network.type", "ipv4")
		case observer.IPVersion_IPv6:
			attrs.PutStr("network.type", "ipv6")
		}
	}

	if l4 := f.GetL4(); l4 != nil {
		switch {
		case l4.GetTCP() != nil:
			attrs.PutStr("network.transport", "tcp")
			attrs.PutInt("network.source.port", int64(l4.GetTCP().GetSourcePort()))
			attrs.PutInt("network.destination.port", int64(l4.GetTCP().GetDestinationPort()))
		case l4.GetUDP() != nil:
			attrs.PutStr("network.transport", "udp")
			attrs.PutInt("network.source.port", int64(l4.GetUDP().GetSourcePort()))
			attrs.PutInt("network.destination.port", int64(l4.GetUDP().GetDestinationPort()))
		case l4.GetSCTP() != nil:
			attrs.PutStr("network.transport", "sctp")
			attrs.PutInt("network.source.port", int64(l4.GetSCTP().GetSourcePort()))
			attrs.PutInt("network.destination.port", int64(l4.GetSCTP().GetDestinationPort()))
		case l4.GetICMPv4() != nil:
			attrs.PutStr("network.transport", "icmp")
			attrs.PutInt("hubble.icmp.type", int64(l4.GetICMPv4().GetType()))
			attrs.PutInt("hubble.icmp.code", int64(l4.GetICMPv4().GetCode()))
		case l4.GetICMPv6() != nil:
			attrs.PutStr("network.transport", "icmpv6")
			attrs.PutInt("hubble.icmp.type", int64(l4.GetICMPv6().GetType()))
			attrs.PutInt("hubble.icmp.code", int64(l4.GetICMPv6().GetCode()))
		}
	}

	if l7 := f.GetL7(); l7 != nil {
		attrs.PutInt("hubble.l7.latency_ns", int64(l7.GetLatencyNs()))
		switch {
		case l7.GetHttp() != nil:
			h := l7.GetHttp()
			attrs.PutStr("http.request.method", h.GetMethod())
			attrs.PutStr("url.full", h.GetUrl())
			attrs.PutInt("http.response.status_code", int64(h.GetCode()))
			attrs.PutStr("network.protocol.name", "http")
			attrs.PutStr("network.protocol.version", h.GetProtocol())
		case l7.GetDns() != nil:
			d := l7.GetDns()
			attrs.PutStr("dns.question.name", d.GetQuery())
			attrs.PutInt("hubble.dns.response_code", int64(d.GetRcode()))
			if ips := d.GetIps(); len(ips) > 0 {
				s := attrs.PutEmptySlice("hubble.dns.response_ips")
				for _, ip := range ips {
					s.AppendEmpty().SetStr(ip)
				}
			}
		case l7.GetKafka() != nil:
			k := l7.GetKafka()
			attrs.PutStr("hubble.kafka.api_key", k.GetApiKey())
			attrs.PutStr("hubble.kafka.topic", k.GetTopic())
			attrs.PutInt("hubble.kafka.error_code", int64(k.GetErrorCode()))
		}
	}

	if tid := f.GetTraceContext().GetParent().GetTraceId(); tid != "" {
		if b, err := hex.DecodeString(tid); err == nil && len(b) == len(pcommon.TraceID{}) {
			lr.SetTraceID(pcommon.TraceID(b))
		}
	}

	if iface := f.GetInterface(); iface != nil {
		attrs.PutStr("hubble.interface.name", iface.GetName())
		attrs.PutInt("hubble.interface.index", int64(iface.GetIndex()))
	}

	return logs
}

func flowDescription(f *flow.Flow) string {
	return "Hubble " + f.GetType().String() + " flow " + f.GetVerdict().String()
}

func verdictSeverity(v observer.Verdict) (sev plog.SeverityNumber, text string) {
	switch v {
	case observer.Verdict_DROPPED:
		return plog.SeverityNumberWarn, "WARN"
	case observer.Verdict_ERROR:
		return plog.SeverityNumberError, "ERROR"
	case observer.Verdict_AUDIT:
		return plog.SeverityNumberInfo, "INFO"
	case observer.Verdict_REDIRECTED, observer.Verdict_TRACED, observer.Verdict_TRANSLATED:
		return plog.SeverityNumberDebug, "DEBUG"
	default:
		return plog.SeverityNumberInfo, "INFO"
	}
}
