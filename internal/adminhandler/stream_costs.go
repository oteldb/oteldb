package adminhandler

import (
	"context"

	"github.com/go-faster/errors"

	"github.com/oteldb/storage"
	"github.com/oteldb/storage/signal"

	"github.com/oteldb/oteldb/internal/adminapi"
)

// defaultStreamCostTopN bounds the report when the caller does not, so the default request stays a
// drill-down rather than a dump of every stream a tenant has.
const defaultStreamCostTopN = 20

// GetStreamCosts implements getStreamCosts operation.
func (a *AdminAPI) GetStreamCosts(ctx context.Context, params adminapi.GetStreamCostsParams) (*adminapi.StreamCosts, error) {
	out := &adminapi.StreamCosts{
		StorageEnabled: a.opts.Engine != nil,
		Signal:         params.Signal,
		Tenant:         params.Tenant.Or(""),
		GroupBy:        adminapi.NewOptString(params.GroupBy.Or("")),
		Groups:         []adminapi.StreamCost{},
	}
	if a.opts.Engine == nil {
		return out, nil
	}

	sig, err := recordSignal(params.Signal)
	if err != nil {
		return nil, err
	}

	costs, err := a.opts.Engine.StreamCosts(ctx, signal.TenantID(out.Tenant), sig, storage.StreamCostOptions{
		GroupBy: params.GroupBy.Or(""),
		Columns: params.Columns,
		TopN:    params.TopN.Or(defaultStreamCostTopN),
	})
	if err != nil {
		return nil, errors.Wrap(err, "collect stream costs")
	}

	for _, c := range costs {
		out.Groups = append(out.Groups, mapStreamCost(c))
	}
	return out, nil
}

func mapStreamCost(c storage.StreamCost) adminapi.StreamCost {
	sc := adminapi.StreamCost{
		Key:               c.Key,
		Streams:           int64(c.Streams),
		Rows:              c.Rows,
		RawBytes:          c.RawBytes,
		DiskBytes:         c.DiskBytes,
		DistinctEstimated: c.DistinctEstimated,
		Columns:           make([]adminapi.ColumnCost, 0, len(c.Columns)),
	}
	for _, cc := range c.Columns {
		sc.Columns = append(sc.Columns, adminapi.ColumnCost{
			Name:               cc.Name,
			RawBytes:           cc.RawBytes,
			DiskBytes:          cc.DiskBytes,
			Distinct:           cc.Distinct,
			DistinctNormalized: cc.DistinctNormalized,
		})
	}
	return sc
}

// recordSignal maps the attributable-signal enum onto a storage signal. The enum excludes metrics,
// so the storage library's rejection of them is unreachable through this API.
func recordSignal(s adminapi.RecordSignal) (signal.Signal, error) {
	switch s {
	case adminapi.RecordSignalLogs:
		return signal.Log, nil
	case adminapi.RecordSignalTraces:
		return signal.Trace, nil
	case adminapi.RecordSignalProfiles:
		return signal.Profile, nil
	default:
		return 0, errors.Errorf("signal %q is not attributable", s)
	}
}
