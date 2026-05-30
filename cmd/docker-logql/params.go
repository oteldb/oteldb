package main

import (
	"fmt"
	"math"
	"strconv"
	"strings"
	"time"

	"github.com/go-faster/errors"
	"github.com/prometheus/common/model"
	"github.com/spf13/pflag"

	"github.com/oteldb/oteldb/internal/logql/logqlengine"
	"github.com/oteldb/oteldb/internal/lokiapi"
)

// APIFlag is [pflag.Value] wrapping ogen optional.
type APIFlag[
	O interface {
		SetTo(S)
	},
	S ~string,
] struct {
	Val        O
	DefaultVal S
}

func apiFlagFor[
	T interface {
		Get() (S, bool)
	},
	P interface {
		*T
		SetTo(S)
	},
	S ~string,
](defaultVal S) APIFlag[P, S] {
	var zero T
	return APIFlag[P, S]{
		Val:        &zero,
		DefaultVal: defaultVal,
	}
}

var _ pflag.Value = (*APIFlag[*lokiapi.OptLokiTime, lokiapi.LokiTime])(nil)

// String implements [pflag.Value].
func (f *APIFlag[O, S]) String() string {
	return string(f.DefaultVal)
}

// Set implements [pflag.Value].
func (f *APIFlag[O, S]) Set(val string) error {
	f.Val.SetTo(S(val))
	return nil
}

// Type implements [pflag.Value].
func (f *APIFlag[O, S]) Type() string {
	var zero S
	return fmt.Sprintf("%T", zero)
}

// parseTimeRange parses optional parameters and returns time range
//
// Default values:
//
//   - since = 6 * time.Hour
//   - end = now
//   - start = end.Add(-since)
func parseTimeRange(
	now time.Time,
	startParam lokiapi.OptLokiTime,
	endParam lokiapi.OptLokiTime,
	sinceParam lokiapi.OptPrometheusDuration,
) (start, end time.Time, err error) {
	since := 6 * time.Hour
	if v, ok := sinceParam.Get(); ok {
		d, err := model.ParseDuration(string(v))
		if err != nil {
			return start, end, errors.Wrap(err, "parse since")
		}
		since = time.Duration(d)
	}

	endValue := endParam.Or("")
	end, err = parseTimestamp(endValue, now)
	if err != nil {
		return start, end, errors.Wrapf(err, "parse end %q", endValue)
	}

	endOrNow := end
	if end.After(now) {
		endOrNow = now
	}

	startValue := startParam.Or("")
	start, err = parseTimestamp(startValue, endOrNow.Add(-since))
	if err != nil {
		return start, end, errors.Wrapf(err, "parse start %q", startValue)
	}
	return start, end, nil
}

func parseTimestamp(lt lokiapi.LokiTime, def time.Time) (time.Time, error) {
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
	return parseDuration(v)
}

func defaultStep(start, end time.Time) time.Duration {
	seconds := math.Max(
		math.Floor(end.Sub(start).Seconds()/250),
		1,
	)
	return time.Duration(seconds) * time.Second
}

func parseDuration(param lokiapi.PrometheusDuration) (time.Duration, error) {
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

var directionMap = func() map[string]logqlengine.Direction {
	m := map[string]logqlengine.Direction{}
	for _, s := range []struct {
		dir    logqlengine.Direction
		values []string
	}{
		{
			logqlengine.DirectionBackward,
			[]string{"desc", "descending"},
		},
		{
			logqlengine.DirectionForward,
			[]string{"asc", "ascending"},
		},
	} {
		m[s.dir.String()] = s.dir
		for _, v := range s.values {
			m[v] = s.dir
		}
	}
	return m
}()

func parseDirection(s string) (logqlengine.Direction, error) {
	orig := s
	s = strings.ToLower(s)

	d, ok := directionMap[s]
	if !ok {
		return "", errors.Errorf("unexpected direction %q", orig)
	}
	return d, nil
}
