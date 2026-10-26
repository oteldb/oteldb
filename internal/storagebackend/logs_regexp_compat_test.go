package storagebackend_test

import (
	"fmt"
	"regexp"
	"slices"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
	"go.opentelemetry.io/collector/pdata/pcommon"
	"go.opentelemetry.io/collector/pdata/plog"

	"github.com/oteldb/storage"
	"github.com/oteldb/storage/backend"
	"github.com/oteldb/storage/signal"

	"github.com/oteldb/oteldb/internal/logql/logqlengine"
	"github.com/oteldb/oteldb/internal/storagebackend"
)

func TestLineRegexpSubstringCompatibility(t *testing.T) {
	t.Parallel()
	ctx := t.Context()
	store, err := storage.Open(ctx, storage.Options{}, storage.WithBackend(backend.Memory()))
	require.NoError(t, err)
	t.Cleanup(func() { require.NoError(t, store.Close(ctx)) })
	bodies := []string{
		"", "foo", "bar", "xfooy", "xbarz", "FOO", "foobar", "foo\nbar",
		"foo bar baz", "xfoo bar bazY", "foo bar other", "xfoo middle bar bazY",
		"before\nfoo\nafter", "αfoo bar bazω", "unrelated noise",
	}
	base := time.Unix(1_700_000_000, 0).UTC()
	ld := plog.NewLogs()
	rl := ld.ResourceLogs().AppendEmpty()
	rl.Resource().Attributes().PutStr("job", "api")
	records := rl.ScopeLogs().AppendEmpty().LogRecords()
	for i, body := range bodies {
		r := records.AppendEmpty()
		r.SetTimestamp(pcommon.Timestamp(base.Add(time.Duration(i) * time.Millisecond).UnixNano()))
		r.Body().SetStr(body)
	}
	require.NoError(t, storagebackend.New(store).ConsumeLogs(ctx, ld))
	params := logqlengine.EvalParams{Start: base.Add(-time.Second), End: base.Add(time.Second), Limit: 1000}
	for _, phase := range []string{"head", "parts"} {
		if phase == "parts" {
			require.NoError(t, store.Admin().Flush(ctx, "default", signal.Log))
		}
		t.Run(phase, func(t *testing.T) {
			for _, remote := range []bool{false, true} {
				t.Run(fmt.Sprintf("remote=%t", remote), func(t *testing.T) {
					var src storagebackend.Source = store
					if remote {
						src = newHintPeer(t, store, false)
					}
					engine, err := logqlengine.NewEngine(storagebackend.NewQuery(src).Logs(), logqlengine.Options{
						Optimizers: []logqlengine.Optimizer{&storagebackend.LogQLOptimizer{}},
					})
					require.NoError(t, err)
					for _, pattern := range []string{
						`foo`, `foo|bar`, `^(foo|bar)$`, `^foo|bar$`, `foo bar baz`,
						`foo.*bar baz`, `foo(?: middle)? bar baz`, `(?i)foo bar baz`,
						`(?i:foo) bar baz`, `^foo bar baz$`, `(?m)^foo$`, `(?s)foo.*bar`,
						`^(|foo)$`, `^$`, `\A(?:foo|bar)\z`,
					} {
						t.Run(pattern, func(t *testing.T) {
							re := regexp.MustCompile(pattern)
							var want []string
							for _, body := range bodies {
								if re.MatchString(body) {
									want = append(want, "line:"+body)
								}
							}
							slices.Sort(want)
							q, err := engine.NewQuery(ctx, fmt.Sprintf(`{job="api"} |~ %q`, pattern))
							require.NoError(t, err)
							got, err := q.Eval(ctx, params)
							require.NoError(t, err)
							require.Equal(t, want, normalize(t, got))
						})
					}
				})
			}
		})
	}
}
