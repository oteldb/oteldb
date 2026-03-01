// Copyright 2016 The Prometheus Authors
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package promhandler

import (
	"maps"
	"math"
	"net/http"
	"strconv"
	"strings"
	"time"

	"github.com/go-faster/errors"
	"github.com/prometheus/common/model"
	"github.com/prometheus/prometheus/model/labels"
	"github.com/prometheus/prometheus/promql"
	"github.com/prometheus/prometheus/promql/parser"

	"github.com/go-faster/oteldb/internal/promapi"
)

var (
	minTimeFormatted = promapi.MinTime.Format(time.RFC3339Nano)
	maxTimeFormatted = promapi.MaxTime.Format(time.RFC3339Nano)
)

// ParseTimeRange parses query time range parameters.
func ParseTimeRange[
	OptTimestamp interface {
		Or(TimestampType) TimestampType
		Get() (TimestampType, bool)
	},
	TimestampType ~string,
	OptDuration interface {
		Or(DurationType) DurationType
		Get() (DurationType, bool)
	},
	DurationType ~string,
](
	now time.Time,
	startParam OptTimestamp,
	endParam OptTimestamp,
	sinceParam OptDuration,
	defaultSince time.Duration,
) (start, end time.Time, err error) {
	since := defaultSince
	if v, ok := sinceParam.Get(); ok {
		d, err := ParseDuration(string(v))
		if err != nil {
			return start, end, errors.Wrap(err, "parse since")
		}
		if d < 0 {
			return start, end, errors.Errorf(`since=%q could not be negative`, v)
		}
		since = d
	}

	end, err = ParseOptTimestamp(endParam, now)
	if err != nil {
		return start, end, errors.Wrapf(err, "parse end %q", endParam.Or(""))
	}

	endOrNow := end
	if end.After(now) {
		endOrNow = now
	}

	start, err = ParseOptTimestamp(startParam, endOrNow.Add(-since))
	if err != nil {
		return start, end, errors.Wrapf(err, "parse start %q", startParam.Or(""))
	}
	if end.Before(start) {
		return start, end, errors.Errorf("end=%q is before start=%q", end, start)
	}
	return start, end, nil
}

// ParseOptTimestamp parses Prometheus-like timestamp from given optional.
func ParseOptTimestamp[
	OptType interface {
		Get() (TimestampType, bool)
	},
	TimestampType ~string,
](opt OptType, def time.Time) (time.Time, error) {
	v, ok := opt.Get()
	if !ok || len(v) == 0 {
		return def, nil
	}
	ts, err := ParseTimestamp(v)
	if err != nil {
		return ts, err
	}
	return ts, nil
}

// ParseTimestamp parses Prometheus-like timestamp from given string.
//
// If string is empty, def is returned.
func ParseTimestamp[S ~string](lt S) (r time.Time, rerr error) {
	defer func() {
		if rerr == nil {
			r = r.UTC()
		}
	}()

	value := string(lt)
	switch value {
	case "":
		return time.Time{}, errors.New("timestamp is empty")
	case minTimeFormatted:
		return promapi.MinTime, nil
	case maxTimeFormatted:
		return promapi.MaxTime, nil
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

// ParseDuration parses Prometheus duration from given string.
func ParseDuration[S ~string](raw S) (d time.Duration, _ error) {
	if seconds, parseErr := strconv.ParseFloat(string(raw), 64); parseErr == nil {
		if math.IsNaN(seconds) || math.IsInf(seconds, 0) {
			return d, errors.Errorf("invalid duration %q", raw)
		}
		d = time.Duration(seconds * float64(time.Second))
	} else {
		md, err := model.ParseDuration(string(raw))
		if err != nil {
			return d, errors.Errorf("invalid duration %q", raw)
		}
		d = time.Duration(md)
	}
	if d < 0 {
		return 0, errors.New(`duration could must be non-negative`)
	}
	return d, nil
}

func parseStep[S ~string](raw S, defaultStep time.Duration) (time.Duration, error) {
	if len(raw) == 0 {
		return defaultStep, nil
	}
	d, err := ParseDuration(raw)
	if err != nil {
		return 0, err
	}
	if d <= 0 {
		return 0, errors.New(`zero or negative query resolution step widths are not accepted`)
	}
	return d, nil
}

func parseQueryOpts(
	deltaParam, statsParam promapi.OptString,
	defaultDelta time.Duration,
) (_ promql.QueryOpts, err error) {
	delta := defaultDelta
	if rawDelta, ok := deltaParam.Get(); ok {
		delta, err = ParseDuration(rawDelta)
		if err != nil {
			return nil, validationErr("parse lookback delta", err)
		}
	}
	return promql.NewPrometheusQueryOpts(
		statsParam.Or("") == "all",
		delta,
	), nil
}

func parseLabelMatchers(matchers []string) ([][]*labels.Matcher, error) {
	var matcherSets [][]*labels.Matcher
	for _, s := range matchers {
		matchers, err := parser.ParseMetricSelector(s)
		if err != nil {
			return nil, errors.Wrapf(err, "parse selector %q", s)
		}
		matcherSets = append(matcherSets, matchers)
	}

OUTER:
	for _, ms := range matcherSets {
		for _, lm := range ms {
			if lm != nil && !lm.Matches("") {
				continue OUTER
			}
		}
		return nil, errors.New("match[] must contain at least one non-empty matcher")
	}
	return matcherSets, nil
}

// PatchForm patches request form with parameters from URL.
//
// This is required for some PromQL client, like this one alerting tool.
//
// See https://github.com/VictoriaMetrics/VictoriaMetrics/blob/v1.134.0/app/vmalert/datasource/client.go.
func PatchForm(next http.Handler) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, req *http.Request) {
		switch req.Method {
		case http.MethodPost,
			http.MethodPatch,
			http.MethodPut:
			if !strings.Contains(
				req.Header.Get("Content-Type"),
				"application/x-www-form-urlencoded",
			) {
				next.ServeHTTP(w, req)
				return
			}
		default:
			next.ServeHTTP(w, req)
			return
		}

		q := req.URL.Query()
		patched := req.Clone(req.Context())
		noForm := patched.PostForm == nil
		if noForm {
			if err := patched.ParseForm(); err != nil {
				// Let handler deal with invalid form.
				next.ServeHTTP(w, patched)
				return
			}
		}
		maps.Copy(patched.PostForm, q)
		if c := patched.ContentLength; noForm && c == 0 {
			patched.ContentLength = int64(len(req.URL.RawQuery))
		}
		next.ServeHTTP(w, patched)
	})
}
