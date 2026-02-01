package promhandler

import (
	"context"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"net/http/httptest"
	"net/url"
	"strings"
	"testing"
	"time"

	"github.com/prometheus/prometheus/model/labels"
	"github.com/stretchr/testify/require"

	"github.com/go-faster/oteldb/internal/lokiapi"
	"github.com/go-faster/oteldb/internal/promapi"
)

func stringToOpt[
	S ~string,
	O interface {
		SetTo(S)
	},
](
	input string,
	opt O,
) {
	if input != "" {
		opt.SetTo(S(input))
	}
}

var defaultTime = time.Date(2000, time.January, 1, 13, 0, 59, 0, time.UTC)

func TestParseTimeRange(t *testing.T) {
	someDate := defaultTime.Add(-time.Hour)

	tests := []struct {
		startParam string
		endParam   string
		sinceParam string

		wantStart time.Time
		wantEnd   time.Time
		wantErr   bool
	}{
		{``, ``, ``, defaultTime.Add(-6 * time.Hour), defaultTime, false},
		{``, ``, `5m`, defaultTime.Add(-5 * time.Minute), defaultTime, false},
		{``, someDate.Format(time.RFC3339Nano), ``, someDate.Add(-6 * time.Hour), someDate, false},

		// Invalid since.
		{``, ``, `a`, time.Time{}, time.Time{}, true},
		// Invalid end.
		{``, `a`, ``, time.Time{}, time.Time{}, true},
		// Invalid start.
		{`a`, ``, ``, time.Time{}, time.Time{}, true},
	}
	for i, tt := range tests {
		tt := tt
		t.Run(fmt.Sprintf("Test%d", i+1), func(t *testing.T) {
			var (
				start, end lokiapi.OptLokiTime
				since      lokiapi.OptPrometheusDuration
			)
			stringToOpt[lokiapi.LokiTime](tt.startParam, &start)
			stringToOpt[lokiapi.LokiTime](tt.endParam, &end)
			stringToOpt[lokiapi.PrometheusDuration](tt.sinceParam, &since)

			gotStart, gotEnd, err := ParseTimeRange(
				defaultTime,
				start,
				end,
				since,
				6*time.Hour,
			)
			if tt.wantErr {
				require.Error(t, err)
				return
			}
			require.NoError(t, err)
			require.True(t, tt.wantStart.Equal(gotStart))
			require.True(t, tt.wantEnd.Equal(gotEnd))
		})
	}
}

func TestParseTimestamp(t *testing.T) {
	someDate := time.Date(2010, time.February, 4, 3, 2, 1, 0, time.UTC)
	tests := []struct {
		raw     string
		want    time.Time
		wantErr bool
	}{
		// A Unix timestamp.
		{`1688650387`, time.Unix(1688650387, 0), false},
		// A Unix nano timestamp.
		{`1688650387000000001`, time.Unix(1688650387, 1), false},
		// A floating point timestamp with fractions of second.
		//
		// .001 (1/1000) of seconds is 1ms
		{`1688650387.001`, time.Unix(1688650387, int64(time.Millisecond)), false},
		// RFC3339.
		{`2015-07-01T20:10:51Z`, time.Date(2015, 7, 1, 20, 10, 51, 0, time.UTC), false},
		// RFC3339Nano.
		{someDate.Format(time.RFC3339Nano), someDate, false},
		{`2015-07-01T20:10:51.781Z`, time.Date(2015, 7, 1, 20, 10, 51, int(781*time.Millisecond), time.UTC), false},
		// Min/Max time.
		{minTimeFormatted, promapi.MinTime, false},
		{maxTimeFormatted, promapi.MaxTime, false},

		{`foo`, time.Time{}, true},
		{``, time.Time{}, true},
	}
	for i, tt := range tests {
		tt := tt
		t.Run(fmt.Sprintf("Test%d", i+1), func(t *testing.T) {
			got, err := ParseTimestamp(tt.raw)
			if tt.wantErr {
				require.Error(t, err)
				return
			}
			require.NoError(t, err)
			require.Equal(t, time.UTC, got.Location())
			require.Equal(t, tt.want.UTC(), got)
		})
	}
}

func TestParseDuration(t *testing.T) {
	tests := []struct {
		raw     string
		want    time.Duration
		wantErr bool
	}{
		{`0`, 0, false},
		{`0s`, 0, false},
		{`10.256`, 10*time.Second + 256*time.Millisecond, false},
		{`1s`, time.Second, false},
		{`1h`, time.Hour, false},
		{`1d`, 24 * time.Hour, false},
		{`1y`, 365 * 24 * time.Hour, false},

		{``, 0, true},
		{`nan`, 0, true},
		{`NaN`, 0, true},
		{`inf`, 0, true},
		{`-inf`, 0, true},
		{`+inf`, 0, true},
		{`-1`, 0, true},
		{`-1.0`, 0, true},
		{`-1s`, 0, true},
	}
	for i, tt := range tests {
		tt := tt
		t.Run(fmt.Sprintf("Test%d", i+1), func(t *testing.T) {
			got, err := ParseDuration(tt.raw)
			if tt.wantErr {
				require.Error(t, err)
				return
			}
			require.NoError(t, err)
			require.Equal(t, tt.want, got)
		})
	}
}

func TestParseStep(t *testing.T) {
	const defaultStep = 10 * time.Hour
	tests := []struct {
		raw     string
		want    time.Duration
		wantErr bool
	}{
		{`10.256`, 10*time.Second + 256*time.Millisecond, false},
		{`1s`, time.Second, false},
		{`1h`, time.Hour, false},

		// Default
		{``, defaultStep, false},

		// Non-positive steps are not allowed.
		{`0`, 0, true},
		{`-10`, 0, true},
		{`foo`, 0, true},
	}
	for i, tt := range tests {
		tt := tt
		t.Run(fmt.Sprintf("Test%d", i+1), func(t *testing.T) {
			got, err := parseStep(tt.raw, defaultStep)
			if tt.wantErr {
				require.Error(t, err)
				return
			}
			require.NoError(t, err)
			require.Equal(t, tt.want, got)
		})
	}
}

func TestParseLabelMatchers(t *testing.T) {
	tests := []struct {
		param   []string
		want    [][]*labels.Matcher
		wantErr bool
	}{
		{nil, nil, false},
		{
			[]string{
				`{foo=~"foo"}`,
				`{bar=~"bar", baz="baz"}`,
			},
			[][]*labels.Matcher{
				{labels.MustNewMatcher(labels.MatchRegexp, "foo", "foo")},
				{
					labels.MustNewMatcher(labels.MatchRegexp, "bar", "bar"),
					labels.MustNewMatcher(labels.MatchEqual, "baz", "baz"),
				},
			},
			false,
		},

		// Invalid syntax.
		{[]string{"{"}, nil, true},
		// Invalid regexp.
		{[]string{`{foo=~"\\"}`}, nil, true},
		// At least one matcher is required.
		{[]string{"{}"}, nil, true},
	}
	for i, tt := range tests {
		tt := tt
		t.Run(fmt.Sprintf("Test%d", i+1), func(t *testing.T) {
			got, err := parseLabelMatchers(tt.param)
			if tt.wantErr {
				require.Error(t, err)
				return
			}
			require.NoError(t, err)

			// LabelMatcher cannot be compared with DeepEqual.
			//
			// See https://github.com/prometheus/prometheus/blob/3b8b57700c469c7cde84e1d8f9d383cb8fe11ab0/promql/parser/parse_test.go#L3719.
			require.Len(t, got, len(tt.want))
			for i, set := range tt.want {
				gotSet := got[i]
				require.Len(t, gotSet, len(set))
				for i, m := range set {
					gotMatcher := gotSet[i]
					require.Equal(t, m.String(), gotMatcher.String())
				}
			}
		})
	}
}

func TestPatchForm(t *testing.T) {
	type testResp struct {
		ContentLength int64
		Values        url.Values
	}
	h := http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		data, err := json.Marshal(testResp{
			ContentLength: r.ContentLength,
			Values:        r.PostForm,
		})
		if err != nil {
			http.Error(w, err.Error(), http.StatusInternalServerError)
			return
		}

		_, _ = w.Write(data)
	})
	srv := httptest.NewServer(PatchForm(h))
	t.Cleanup(srv.Close)
	client := srv.Client()

	makeRequest := func(ctx context.Context, method string, query, form url.Values) (testResp, error) {
		var body io.Reader = http.NoBody
		if form != nil {
			body = strings.NewReader(form.Encode())
		}

		req, err := http.NewRequestWithContext(ctx, method, srv.URL, body)
		if err != nil {
			return testResp{}, fmt.Errorf("make request: %w", err)
		}
		req.Header.Set("Content-Type", "application/x-www-form-urlencoded")
		if query != nil {
			req.URL.RawQuery = query.Encode()
		}

		resp, err := client.Do(req)
		if err != nil {
			return testResp{}, fmt.Errorf("do request: %w", err)
		}
		data, err := io.ReadAll(resp.Body)
		if err != nil {
			return testResp{}, fmt.Errorf("read body: %w", err)
		}

		var response testResp
		if err := json.Unmarshal(data, &response); err != nil {
			return testResp{}, fmt.Errorf("unmarshal response: %w", err)
		}
		return response, nil
	}

	for _, tt := range []struct {
		name              string
		method            string
		query             url.Values
		form              url.Values
		wantValues        url.Values
		wantContentLength int64
	}{
		{
			name:       "GetRequest",
			method:     http.MethodGet,
			wantValues: nil,
		},
		{
			name:       "EmptyPostRequest",
			method:     http.MethodPost,
			wantValues: url.Values{},
		},
		{
			name:       "EmptyFormRequest",
			method:     http.MethodPost,
			form:       url.Values{},
			wantValues: url.Values{},
		},
		{
			name:              "OnlyForm",
			method:            http.MethodPost,
			form:              url.Values{"hello": []string{"world"}},
			wantValues:        url.Values{"hello": []string{"world"}},
			wantContentLength: 11,
		},
		{
			name:              "OnlyQuery",
			method:            http.MethodPost,
			query:             url.Values{"hello": []string{"world"}},
			wantValues:        url.Values{"hello": []string{"world"}},
			wantContentLength: 11,
		},
		{
			name:   "Disjoint",
			method: http.MethodPost,
			query:  url.Values{"queryParam": []string{"queryValue"}},
			form:   url.Values{"formParam": []string{"formValue"}},
			wantValues: url.Values{
				"queryParam": []string{"queryValue"},
				"formParam":  []string{"formValue"},
			},
			wantContentLength: 19,
		},
		{
			name:   "Merge",
			method: http.MethodPost,
			query: url.Values{
				"param1":     []string{"q1"},
				"queryParam": []string{"queryValue"},
			},
			form: url.Values{
				"param1":    []string{"f1"},
				"formParam": []string{"formValue"},
			},
			wantValues: url.Values{
				"param1":     []string{"q1"}, // Query have precedence.
				"queryParam": []string{"queryValue"},
				"formParam":  []string{"formValue"},
			},
			wantContentLength: 29,
		},
	} {
		tt := tt
		t.Run(tt.name, func(t *testing.T) {
			ctx := t.Context()

			r, err := makeRequest(ctx, tt.method, tt.query, tt.form)
			require.NoError(t, err)
			require.Equal(t, tt.wantValues, r.Values)
			require.Equal(t, tt.wantContentLength, r.ContentLength)
		})
	}
}
