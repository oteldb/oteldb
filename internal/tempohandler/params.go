package tempohandler

import (
	"time"

	"github.com/go-faster/errors"

	"github.com/go-faster/oteldb/internal/promhandler"
	"github.com/go-faster/oteldb/internal/tempoapi"
)

func parseTagsTimeRange(
	now time.Time,
	startParam tempoapi.OptTempoTime,
	endParam tempoapi.OptTempoTime,
	sinceParam tempoapi.OptPrometheusDuration,
	defaultSince time.Duration,
) (start, end time.Time, err error) {
	// TODO(tdakkota): limit time range by default.
	return parseSearchTimeRange(now, startParam, endParam, sinceParam)
}

func parseQueryTimeRange(
	now time.Time,
	startParam tempoapi.OptTempoTime,
	endParam tempoapi.OptTempoTime,
	sinceParam tempoapi.OptPrometheusDuration,
	defaultSince time.Duration,
) (start, end time.Time, err error) {
	// TODO(tdakkota): limit time range by default.
	return parseSearchTimeRange(now, startParam, endParam, sinceParam)
}

// parseSearchTimeRange parses search time range parameters.
func parseSearchTimeRange(
	now time.Time,
	startParam tempoapi.OptTempoTime,
	endParam tempoapi.OptTempoTime,
	sinceParam tempoapi.OptPrometheusDuration,
) (start, end time.Time, err error) {
	anySet := startParam.Set || endParam.Set || sinceParam.Set
	if !anySet {
		return time.Time{}, time.Time{}, nil
	}

	var since time.Duration
	if v, ok := sinceParam.Get(); ok {
		d, err := promhandler.ParseDuration(string(v))
		if err != nil {
			return start, end, errors.Wrap(err, "parse since")
		}
		if d < 0 {
			return start, end, errors.Errorf(`since=%q could not be negative`, v)
		}
		since = d
	}

	end, err = promhandler.ParseOptTimestamp(endParam, now)
	if err != nil {
		return start, end, errors.Wrapf(err, "parse end %q", endParam.Or(""))
	}

	endOrNow := end
	if end.After(now) {
		endOrNow = now
	}

	canComputeStart := startParam.Set || sinceParam.Set
	if canComputeStart {
		start, err = promhandler.ParseOptTimestamp(startParam, endOrNow.Add(-since))
		if err != nil {
			return start, end, errors.Wrapf(err, "parse start %q", startParam.Or(""))
		}
		if end.Before(start) {
			return start, end, errors.Errorf("end=%q is before start=%q", end, start)
		}
	}
	return start, end, nil
}
