package profilehandler

import (
	"bytes"
	"compress/gzip"
	"context"
	"io"
	"strings"
	"time"

	"connectrpc.com/connect"
	"github.com/go-faster/errors"
	"google.golang.org/protobuf/proto"

	googlev1 "github.com/grafana/pyroscope/api/gen/proto/go/google/v1"
	querierv1 "github.com/grafana/pyroscope/api/gen/proto/go/querier/v1"
	"github.com/grafana/pyroscope/api/gen/proto/go/querier/v1/querierv1connect"
	typesv1 "github.com/grafana/pyroscope/api/gen/proto/go/types/v1"

	"github.com/oteldb/oteldb/internal/profileql"
	"github.com/oteldb/oteldb/internal/profileql/profileqlengine"
	"github.com/oteldb/oteldb/internal/profilestorage"
)

// maxSeriesBuckets bounds the number of time buckets [QuerierService.SelectSeries] evaluates, so a
// wide range with a tiny step cannot fan out into an unbounded number of merge queries.
const maxSeriesBuckets = 120

// QuerierService implements the connect QuerierService API used by Grafana's built-in Pyroscope
// datasource, backed by the same ProfileQL engine and [profilestorage.Querier] as the legacy
// Pyroscope HTTP API ([PyroscopeAPI]). The two are served side by side on the Pyroscope listener.
//
// Methods that have no representation in oteldb's storage (span profiles, heatmaps, diffs, async
// queries) are left to the embedded Unimplemented handler, which returns CodeUnimplemented.
type QuerierService struct {
	querierv1connect.UnimplementedQuerierServiceHandler
	q      profilestorage.Querier
	engine *profileqlengine.Engine
}

var _ querierv1connect.QuerierServiceHandler = (*QuerierService)(nil)

// NewQuerierService returns a QuerierService backed by q and engine.
func NewQuerierService(q profilestorage.Querier, engine *profileqlengine.Engine) *QuerierService {
	return &QuerierService{q: q, engine: engine}
}

// ProfileTypes implements the ProfileTypes RPC.
func (s *QuerierService) ProfileTypes(ctx context.Context, req *connect.Request[querierv1.ProfileTypesRequest]) (*connect.Response[querierv1.ProfileTypesResponse], error) {
	types, err := s.q.ProfileTypes(ctx, profilestorage.ProfileTypesOptions{
		Start: msToTime(req.Msg.Start),
		End:   msToTime(req.Msg.End),
	})
	if err != nil {
		return nil, connectErr(err)
	}
	resp := &querierv1.ProfileTypesResponse{ProfileTypes: make([]*typesv1.ProfileType, 0, len(types))}
	for _, t := range types {
		resp.ProfileTypes = append(resp.ProfileTypes, profileTypeProto(t))
	}
	return connect.NewResponse(resp), nil
}

// LabelNames implements the LabelNames RPC.
func (s *QuerierService) LabelNames(ctx context.Context, req *connect.Request[typesv1.LabelNamesRequest]) (*connect.Response[typesv1.LabelNamesResponse], error) {
	matchers, typ, err := parseMatchers(req.Msg.Matchers)
	if err != nil {
		return nil, connectErr(err)
	}
	names, err := s.q.LabelNames(ctx, profilestorage.LabelNamesOptions{
		Type:     typ,
		Matchers: matchers,
		Start:    msToTime(req.Msg.Start),
		End:      msToTime(req.Msg.End),
	})
	if err != nil {
		return nil, connectErr(err)
	}
	return connect.NewResponse(&typesv1.LabelNamesResponse{Names: names}), nil
}

// LabelValues implements the LabelValues RPC.
func (s *QuerierService) LabelValues(ctx context.Context, req *connect.Request[typesv1.LabelValuesRequest]) (*connect.Response[typesv1.LabelValuesResponse], error) {
	matchers, typ, err := parseMatchers(req.Msg.Matchers)
	if err != nil {
		return nil, connectErr(err)
	}
	start, end := msToTime(req.Msg.Start), msToTime(req.Msg.End)

	// The synthetic profile-type label resolves to the list of profile type IDs, matching the
	// legacy API and what Grafana expects from the __profile_type__ / __name__ labels.
	if name := req.Msg.Name; name == profileql.LabelProfileType || name == profileql.LabelName {
		types, err := s.q.ProfileTypes(ctx, profilestorage.ProfileTypesOptions{Start: start, End: end})
		if err != nil {
			return nil, connectErr(err)
		}
		values := make([]string, 0, len(types))
		for _, t := range types {
			values = append(values, t.ID())
		}
		return connect.NewResponse(&typesv1.LabelValuesResponse{Names: values}), nil
	}

	values, err := s.q.LabelValues(ctx, req.Msg.Name, profilestorage.LabelValuesOptions{
		Type:     typ,
		Matchers: matchers,
		Start:    start,
		End:      end,
	})
	if err != nil {
		return nil, connectErr(err)
	}
	return connect.NewResponse(&typesv1.LabelValuesResponse{Names: values}), nil
}

// Series implements the Series RPC. oteldb's querier does not enumerate full series (unique label
// sets), so this returns a best-effort approximation: for each requested label name, one single-label
// series per distinct value. It is enough for Grafana to populate group-by and label-value pickers.
func (s *QuerierService) Series(ctx context.Context, req *connect.Request[querierv1.SeriesRequest]) (*connect.Response[querierv1.SeriesResponse], error) {
	matchers, typ, err := parseMatchers(req.Msg.Matchers)
	if err != nil {
		return nil, connectErr(err)
	}
	start, end := msToTime(req.Msg.Start), msToTime(req.Msg.End)

	var sets []*typesv1.Labels
	for _, name := range req.Msg.LabelNames {
		values, err := s.q.LabelValues(ctx, name, profilestorage.LabelValuesOptions{
			Type:     typ,
			Matchers: matchers,
			Start:    start,
			End:      end,
		})
		if err != nil {
			return nil, connectErr(err)
		}
		for _, v := range values {
			sets = append(sets, &typesv1.Labels{Labels: []*typesv1.LabelPair{{Name: name, Value: v}}})
		}
	}
	return connect.NewResponse(&querierv1.SeriesResponse{LabelsSet: sets}), nil
}

// SelectMergeStacktraces implements the SelectMergeStacktraces RPC, returning the merged flamegraph.
func (s *QuerierService) SelectMergeStacktraces(ctx context.Context, req *connect.Request[querierv1.SelectMergeStacktracesRequest]) (*connect.Response[querierv1.SelectMergeStacktracesResponse], error) {
	result, err := s.selectMerge(ctx, req.Msg.ProfileTypeID, req.Msg.LabelSelector, req.Msg.Start, req.Msg.End, int(req.Msg.GetMaxNodes()))
	if err != nil {
		return nil, err
	}
	return connect.NewResponse(&querierv1.SelectMergeStacktracesResponse{
		Flamegraph: flameGraphProto(result),
	}), nil
}

// SelectMergeProfile implements the SelectMergeProfile RPC, returning the merged profile in pprof
// (google.v1.Profile) format.
func (s *QuerierService) SelectMergeProfile(ctx context.Context, req *connect.Request[querierv1.SelectMergeProfileRequest]) (*connect.Response[googlev1.Profile], error) {
	result, err := s.selectMerge(ctx, req.Msg.ProfileTypeID, req.Msg.LabelSelector, req.Msg.Start, req.Msg.End, int(req.Msg.GetMaxNodes()))
	if err != nil {
		return nil, err
	}
	gz, err := result.Pprof()
	if err != nil {
		return nil, connectErr(err)
	}
	// Result.Pprof returns gzip-compressed pprof; the connect API returns the protobuf message, so
	// decompress to the raw google.v1.Profile wire bytes and decode them.
	gr, err := gzip.NewReader(bytes.NewReader(gz))
	if err != nil {
		return nil, connectErr(errors.Wrap(err, "open gzip"))
	}
	raw, err := io.ReadAll(gr)
	if err != nil {
		return nil, connectErr(errors.Wrap(err, "decompress pprof"))
	}
	var prof googlev1.Profile
	if err := proto.Unmarshal(raw, &prof); err != nil {
		return nil, connectErr(errors.Wrap(err, "decode pprof"))
	}
	return connect.NewResponse(&prof), nil
}

// SelectSeries implements the SelectSeries RPC by evaluating the merged total per time bucket. The
// group_by labels are not honored (a single total series is returned); the engine has no grouped
// time-series primitive, so this trades fidelity for a useful timeline.
func (s *QuerierService) SelectSeries(ctx context.Context, req *connect.Request[querierv1.SelectSeriesRequest]) (*connect.Response[querierv1.SelectSeriesResponse], error) {
	msg := req.Msg
	start, end := msToTime(msg.Start), msToTime(msg.End)
	if start.IsZero() || end.IsZero() || !end.After(start) {
		return nil, connect.NewError(connect.CodeInvalidArgument, errors.New("start and end are required"))
	}
	query := msg.ProfileTypeID + msg.LabelSelector

	rangeDur := end.Sub(start)
	step := time.Duration(msg.Step * float64(time.Second))
	if step <= 0 || rangeDur/step > maxSeriesBuckets {
		step = rangeDur / maxSeriesBuckets
	}
	if step <= 0 {
		step = rangeDur
	}

	var points []*typesv1.Point
	for t := start; t.Before(end); t = t.Add(step) {
		bucketEnd := t.Add(step)
		if bucketEnd.After(end) {
			bucketEnd = end
		}
		// The engine's time range is inclusive on both ends, so trim a nanosecond to keep buckets
		// half-open [t, bucketEnd) and avoid double-counting a sample that lands on a boundary.
		result, err := s.engine.Select(ctx, query, profileqlengine.EvalParams{Start: t, End: bucketEnd.Add(-time.Nanosecond)})
		if err != nil {
			return nil, connectErr(err)
		}
		var total int64
		if result.Tree != nil {
			total = result.Tree.Total()
		}
		points = append(points, &typesv1.Point{Value: float64(total), Timestamp: t.UnixMilli()})
	}
	return connect.NewResponse(&querierv1.SelectSeriesResponse{
		Series: []*typesv1.Series{{Points: points}},
	}), nil
}

// GetProfileStats implements the GetProfileStats RPC. Grafana uses it to detect whether the
// datasource has any data; oteldb does not track ingestion timestamps, so only data_ingested is set.
func (s *QuerierService) GetProfileStats(ctx context.Context, _ *connect.Request[typesv1.GetProfileStatsRequest]) (*connect.Response[typesv1.GetProfileStatsResponse], error) {
	types, err := s.q.ProfileTypes(ctx, profilestorage.ProfileTypesOptions{})
	if err != nil {
		return nil, connectErr(err)
	}
	return connect.NewResponse(&typesv1.GetProfileStatsResponse{DataIngested: len(types) > 0}), nil
}

// selectMerge runs a merge query for the given profile type id and label selector, returning the
// ProfileQL result.
func (s *QuerierService) selectMerge(ctx context.Context, profileTypeID, labelSelector string, startMs, endMs int64, maxNodes int) (*profileqlengine.Result, error) {
	if s.engine == nil {
		return nil, connect.NewError(connect.CodeUnimplemented, errors.New("ProfileQL engine is disabled"))
	}
	result, err := s.engine.Select(ctx, profileTypeID+labelSelector, profileqlengine.EvalParams{
		Start:    msToTime(startMs),
		End:      msToTime(endMs),
		MaxNodes: maxNodes,
	})
	if err != nil {
		return nil, connectErr(err)
	}
	return result, nil
}

// parseMatchers parses a list of label-selector strings into ProfileQL matchers and an optional
// profile type (from a leading type token or a __name__/__profile_type__ matcher).
func parseMatchers(selectors []string) (matchers []profileql.LabelMatcher, typ *profileql.ProfileType, _ error) {
	for _, sel := range selectors {
		if strings.TrimSpace(sel) == "" {
			continue
		}
		s, err := profileql.ParseSelector(sel)
		if err != nil {
			return nil, nil, err
		}
		matchers = append(matchers, s.Matchers...)
		if s.Type != nil {
			typ = s.Type
		}
	}
	return matchers, typ, nil
}

// profileTypeProto converts a ProfileQL profile type into its proto representation.
func profileTypeProto(t profileql.ProfileType) *typesv1.ProfileType {
	return &typesv1.ProfileType{
		ID:         t.ID(),
		Name:       t.Name,
		SampleType: t.SampleType,
		SampleUnit: t.SampleUnit,
		PeriodType: t.PeriodType,
		PeriodUnit: t.PeriodUnit,
	}
}

// flameGraphProto converts a ProfileQL result's flamebearer into the connect FlameGraph message.
// Both use the same level encoding (chunks of four: x-offset delta, total, self, name index), so the
// conversion is a direct copy.
func flameGraphProto(result *profileqlengine.Result) *querierv1.FlameGraph {
	fb := result.Flamebearer().Flamebearer
	levels := make([]*querierv1.Level, len(fb.Levels))
	for i, level := range fb.Levels {
		values := make([]int64, len(level))
		for j, v := range level {
			values[j] = int64(v)
		}
		levels[i] = &querierv1.Level{Values: values}
	}
	return &querierv1.FlameGraph{
		Names:   fb.Names,
		Levels:  levels,
		Total:   int64(fb.NumTicks),
		MaxSelf: int64(fb.MaxSelf),
	}
}

// msToTime converts milliseconds since the Unix epoch to a time. A non-positive value yields the
// zero time, which the querier treats as an unbounded range bound.
func msToTime(ms int64) time.Time {
	if ms <= 0 {
		return time.Time{}
	}
	return time.UnixMilli(ms)
}

// connectErr maps a handler error to a connect error, classifying ProfileQL parse errors as
// invalid-argument and everything else as internal.
func connectErr(err error) error {
	if _, ok := errors.Into[*profileql.ParseError](err); ok {
		return connect.NewError(connect.CodeInvalidArgument, err)
	}
	return connect.NewError(connect.CodeInternal, err)
}
