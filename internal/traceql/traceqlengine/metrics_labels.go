package traceqlengine

import (
	"strconv"
	"strings"

	"github.com/go-faster/errors"
	"github.com/prometheus/prometheus/model/labels"

	"github.com/oteldb/oteldb/internal/tempoapi"
	"github.com/oteldb/oteldb/internal/traceql"
)

// Runtime label strings encode type and bits; formatting never merges 1 with "1".
func encodeMetricsLabel(v traceql.Static) string {
	return strconv.Itoa(int(v.Type)) + ":" + strconv.FormatUint(v.Data, 16) + ":" + v.Str
}

func decodeMetricsLabel(s string) (traceql.Static, error) {
	typ, rest, ok := strings.Cut(s, ":")
	if !ok {
		return traceql.Static{}, errors.New("invalid typed metrics label")
	}
	bits, value, ok := strings.Cut(rest, ":")
	if !ok {
		return traceql.Static{}, errors.New("invalid typed metrics label")
	}
	t, err := strconv.Atoi(typ)
	if err != nil {
		return traceql.Static{}, err
	}
	data, err := strconv.ParseUint(bits, 16, 64)
	if err != nil {
		return traceql.Static{}, err
	}
	return traceql.Static{Type: traceql.StaticType(t), Data: data, Str: value}, nil
}

func metricsLabels(ls labels.Labels) (tempoapi.Attributes, error) {
	out := make(tempoapi.Attributes, 0, ls.Len())
	var resultErr error
	ls.Range(func(label labels.Label) {
		if resultErr != nil {
			return
		}
		var value tempoapi.AnyValue
		if label.Name == "__name__" {
			value.SetStringValue(tempoapi.StringValue{StringValue: label.Value})
		} else {
			v, err := decodeMetricsLabel(label.Value)
			if err != nil {
				resultErr = err
				return
			}
			if v.Type == traceql.TypeNil {
				return
			}
			switch v.Type {
			case traceql.TypeString:
				value.SetStringValue(tempoapi.StringValue{StringValue: v.Str})
			case traceql.TypeBool:
				value.SetBoolValue(tempoapi.BoolValue{BoolValue: v.AsBool()})
			case traceql.TypeInt:
				value.SetIntValue(tempoapi.IntValue{IntValue: v.AsInt()})
			case traceql.TypeNumber:
				value.SetDoubleValue(tempoapi.DoubleValue{DoubleValue: v.AsNumber()})
			case traceql.TypeDuration:
				value.SetStringValue(tempoapi.StringValue{StringValue: v.AsDuration().String()})
			case traceql.TypeSpanStatus:
				value.SetStringValue(tempoapi.StringValue{StringValue: strings.ToLower(v.AsSpanStatus().String())})
			case traceql.TypeSpanKind:
				value.SetStringValue(tempoapi.StringValue{StringValue: strings.ToLower(v.AsSpanKind().String())})
			default:
				resultErr = errors.New("unsupported typed metrics label")
				return
			}
		}
		out = append(out, tempoapi.KeyValue{Key: label.Name, Value: value})
	})
	return out, resultErr
}
