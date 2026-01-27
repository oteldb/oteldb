package lokihandler

import (
	"math"
	"strconv"
	"strings"
	"time"

	"github.com/go-faster/errors"
	"github.com/prometheus/common/model"

	"github.com/go-faster/oteldb/internal/logql/logqlengine"
	"github.com/go-faster/oteldb/internal/lokiapi"
)

// parseTimeRange parses optional parameters and returns time range
//
// Default values:
//
//   - since = 1 * time.Hour
//   - end == now
//   - start = end.Add(-since) if not end.After(now)
func parseTimeRange(
	now time.Time,
	startParam lokiapi.OptLokiTime,
	endParam lokiapi.OptLokiTime,
	sinceParam lokiapi.OptPrometheusDuration,
	defaultSince time.Duration,
) (start, end time.Time, err error) {
	since := defaultSince
	if v, ok := sinceParam.Get(); ok {
		d, err := model.ParseDuration(string(v))
		if err != nil {
			return start, end, errors.Wrap(err, "parse since")
		}
		since = time.Duration(d)
	}

	endValue := endParam.Or("")
	end, err = ParseTimestamp(endValue, now)
	if err != nil {
		return start, end, errors.Wrapf(err, "parse end %q", endValue)
	}

	endOrNow := end
	if end.After(now) {
		endOrNow = now
	}

	startValue := startParam.Or("")
	start, err = ParseTimestamp(startValue, endOrNow.Add(-since))
	if err != nil {
		return start, end, errors.Wrapf(err, "parse start %q", startValue)
	}
	return start, end, nil
}

// ParseTimestamp parses Loki API timestamp from given string.
//
// If string is empty, def is returned.
func ParseTimestamp[S ~string](lt S, def time.Time) (time.Time, error) {
	value := string(lt)
	if value == "" {
		return def, nil
	}

	if strings.Contains(value, ".") {
		if t, err := strconv.ParseFloat(value, 64); err == nil {
			s, ns := math.Modf(t)
			ns = math.Round(ns*1000) / 1000
			return time.Unix(int64(s), int64(ns*float64(time.Second))), nil
		}
	}
	nanos, err := strconv.ParseInt(value, 10, 64)
	if err != nil {
		return time.Parse(time.RFC3339Nano, value)
	}
	if len(value) <= 10 {
		return time.Unix(nanos, 0), nil
	}
	return time.Unix(0, nanos), nil
}

func parseStep(param lokiapi.OptPrometheusDuration, start, end time.Time) (time.Duration, error) {
	v, ok := param.Get()
	if !ok {
		return defaultStep(start, end), nil
	}
	return ParseDuration(v)
}

func defaultStep(start, end time.Time) time.Duration {
	seconds := math.Max(
		math.Floor(end.Sub(start).Seconds()/250),
		1,
	)
	return time.Duration(seconds) * time.Second
}

// ParseDuration parses Loki API duration from given string.
func ParseDuration[S ~string](param S) (time.Duration, error) {
	value := string(param)
	if !strings.ContainsAny(value, "smhdwy") {
		f, err := strconv.ParseFloat(value, 64)
		if err == nil {
			d := f * float64(time.Second)
			return time.Duration(d), nil
		}
	}
	md, err := model.ParseDuration(value)
	return time.Duration(md), err
}

func parseDirection(opt lokiapi.OptDirection) (r logqlengine.Direction, _ error) {
	switch d := opt.Or(lokiapi.DirectionBackward); d {
	case lokiapi.DirectionBackward:
		return logqlengine.DirectionBackward, nil
	case lokiapi.DirectionForward:
		return logqlengine.DirectionForward, nil
	default:
		return r, errors.Errorf("invalid direction %q", d)
	}
}
