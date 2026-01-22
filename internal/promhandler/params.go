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

func parseOptTimestamp(t promapi.OptPrometheusTimestamp, or time.Time) (time.Time, error) {
	v, ok := t.Get()
	if !ok {
		return or, nil
	}
	return parseTimestamp(v)
}

func parseTimestamp[S ~string](raw S) (time.Time, error) {
	// https://github.com/prometheus/prometheus/blob/e9b94515caa4c0d7a0e31f722a1534948ebad838/web/api/v1/api.go#L1790-L1811
	s := string(raw)

	if t, err := strconv.ParseFloat(s, 64); err == nil {
		s, ns := math.Modf(t)
		ns = math.Round(ns*1000) / 1000
		return time.Unix(int64(s), int64(ns*float64(time.Second))).UTC(), nil
	}
	if t, err := time.Parse(time.RFC3339Nano, s); err == nil {
		return t, nil
	}

	switch s {
	case minTimeFormatted:
		return promapi.MinTime, nil
	case maxTimeFormatted:
		return promapi.MaxTime, nil
	}
	return time.Time{}, errors.Errorf("cannot parse %q to a valid timestamp", s)
}

func parseStep[S ~string](raw S, defaultStep time.Duration) (time.Duration, error) {
	if len(raw) == 0 {
		return defaultStep, nil
	}
	d, err := parseDuration(raw)
	if err != nil {
		return 0, err
	}
	if d <= 0 {
		return 0, errors.New(`zero or negative query resolution step widths are not accepted`)
	}
	return d, nil
}

func parseDuration[S ~string](raw S) (time.Duration, error) {
	if seconds, err := strconv.ParseFloat(string(raw), 64); err == nil {
		return time.Duration(seconds * float64(time.Second)), nil
	}
	if d, err := model.ParseDuration(string(raw)); err == nil {
		return time.Duration(d), nil
	}
	return 0, errors.Errorf("invalid duration %q", raw)
}

func parseQueryOpts(
	deltaParam, statsParam promapi.OptString,
	defaultDelta time.Duration,
) (_ promql.QueryOpts, err error) {
	delta := defaultDelta
	if rawDelta, ok := deltaParam.Get(); ok {
		delta, err = parseDuration(rawDelta)
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
