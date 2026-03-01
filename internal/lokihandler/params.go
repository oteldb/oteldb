package lokihandler

import (
	"math"
	"time"

	"github.com/go-faster/errors"

	"github.com/go-faster/oteldb/internal/logql/logqlengine"
	"github.com/go-faster/oteldb/internal/lokiapi"
	"github.com/go-faster/oteldb/internal/promhandler"
)

// ParseTimestamp parses Loki API timestamp from given string.
//
// If string is empty, def is returned.
func ParseTimestamp[S ~string](lt S, def time.Time) (time.Time, error) {
	opt := lokiapi.OptLokiTime{
		Value: lokiapi.LokiTime(lt),
		Set:   lt != "",
	}
	return promhandler.ParseOptTimestamp(opt, def)
}

// parseTimeRange parses optional parameters and returns time range
//
// Default values:
//
//   - since = 1 * time.Hour
//   - end == now
//   - start = end.Add(-since) if not end.After(now)
func parseTimeRange(
	now time.Time,
	startParam lokiapi.OptLokiTime,
	endParam lokiapi.OptLokiTime,
	sinceParam lokiapi.OptPrometheusDuration,
	defaultSince time.Duration,
) (start, end time.Time, err error) {
	return promhandler.ParseTimeRange(now, startParam, endParam, sinceParam, defaultSince)
}

func parseStep(param lokiapi.OptPrometheusDuration, start, end time.Time) (time.Duration, error) {
	v, ok := param.Get()
	if !ok {
		return defaultStep(start, end), nil
	}
	return promhandler.ParseDuration(v)
}

func defaultStep(start, end time.Time) time.Duration {
	seconds := math.Max(
		math.Floor(end.Sub(start).Seconds()/250),
		1,
	)
	return time.Duration(seconds) * time.Second
}

func parseDirection(opt lokiapi.OptDirection) (r logqlengine.Direction, _ error) {
	switch d := opt.Or(lokiapi.DirectionBackward); d {
	case lokiapi.DirectionBackward:
		return logqlengine.DirectionBackward, nil
	case lokiapi.DirectionForward:
		return logqlengine.DirectionForward, nil
	default:
		return r, errors.Errorf("invalid direction %q", d)
	}
}
