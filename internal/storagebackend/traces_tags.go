package storagebackend

import (
	"time"

	"github.com/oteldb/storage/index/bloom"
	"github.com/oteldb/storage/query/fetch"
	"github.com/oteldb/storage/signal"
	sigtrace "github.com/oteldb/storage/signal/trace"

	"github.com/oteldb/oteldb/internal/tracestorage"
)

func tagSearchConditions(tags map[string]string, opts tracestorage.SearchTagsOptions) []fetch.Condition {
	var conditions []fetch.Condition
	if name, ok := tags["name"]; ok {
		condition := fetch.Condition{
			Column: sigtrace.ColName,
			Match: func(v signal.Value) bool {
				return name != "" && string(v.Str()) == name
			},
			Tokens: bloom.SafeTokens(nil, []byte(name), true, true),
		}
		if name != "" {
			condition.Equal = &fetch.EqualMatcher{Name: sigtrace.ColName, Value: name}
		}
		conditions = append(conditions, condition)
	}
	if opts.MinDuration > 0 || opts.MaxDuration > 0 {
		conditions = append(conditions, fetch.Condition{
			Column: sigtrace.ColDuration,
			Match: func(v signal.Value) bool {
				duration := time.Duration(v.Int())
				return (opts.MinDuration <= 0 || duration >= opts.MinDuration) &&
					(opts.MaxDuration <= 0 || duration <= opts.MaxDuration)
			},
		})
	}
	return conditions
}
