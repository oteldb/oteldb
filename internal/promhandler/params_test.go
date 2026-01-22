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
)

func TestParseTimestamp(t *testing.T) {
	tests := []struct {
		raw     string
		want    time.Time
		wantErr bool
	}{
		{`1600000000.123`, time.UnixMilli(1600000000123).UTC(), false},
		{`2015-07-01T20:10:51.781Z`, time.Date(2015, 7, 1, 20, 10, 51, int(781*time.Millisecond), time.UTC), false},

		{`foo`, time.Time{}, true},
		{``, time.Time{}, true},
	}
	for i, tt := range tests {
		tt := tt
		t.Run(fmt.Sprintf("Test%d", i+1), func(t *testing.T) {
			got, err := parseTimestamp(tt.raw)
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
	tests := []struct {
		raw     string
		want    time.Duration
		wantErr bool
	}{
		{`10.256`, 10*time.Second + 256*time.Millisecond, false},
		{`1s`, time.Second, false},
		{`1h`, time.Hour, false},

		// Non-positive steps are not allowed.
		{`0`, 0, true},
		{`-10`, 0, true},
		{`foo`, 0, true},
		{``, 0, true},
	}
	for i, tt := range tests {
		tt := tt
		t.Run(fmt.Sprintf("Test%d", i+1), func(t *testing.T) {
			got, err := parseStep(tt.raw)
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
		if query != nil {
			req.URL.RawQuery = query.Encode()
		}
		if form != nil {
			req.Header.Set("Content-Type", "application/x-www-form-urlencoded")
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
