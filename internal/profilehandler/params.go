package profilehandler

import (
	"strconv"
	"strings"
	"time"

	"github.com/go-faster/errors"

	"github.com/oteldb/oteldb/internal/pyroscopeapi"
)

// parseTimeRange resolves the [start, end] range from Pyroscope `attime`
// parameters, defaulting the end to now and the start to end-defaultSince.
func parseTimeRange(
	now time.Time,
	from, until pyroscopeapi.OptAtTime,
	defaultSince time.Duration,
) (start, end time.Time, _ error) {
	end = now
	if v, ok := until.Get(); ok {
		t, err := parseAtTime(string(v), now)
		if err != nil {
			return start, end, errors.Wrap(err, "parse until")
		}
		end = t
	}

	start = end.Add(-defaultSince)
	if v, ok := from.Get(); ok {
		t, err := parseAtTime(string(v), now)
		if err != nil {
			return start, end, errors.Wrap(err, "parse from")
		}
		start = t
	}

	if end.Before(start) {
		return start, end, errors.Errorf("end %s is before start %s", end, start)
	}
	return start, end, nil
}

// parseAtTime parses a Pyroscope `attime` timestamp.
//
// It supports:
//   - unix timestamps in seconds, milliseconds, microseconds or nanoseconds
//     (distinguished by digit count);
//   - the "now" reference with an optional relative offset, e.g. "now-1h".
//
// It mirrors the behavior of Pyroscope's attime parser.
func parseAtTime(s string, now time.Time) (time.Time, error) {
	s = strings.TrimSpace(s)
	s = strings.NewReplacer("_", "", ",", "", " ", "").Replace(s)
	if s == "" {
		return now, nil
	}

	if isDigits(s) {
		v, err := strconv.ParseInt(s, 10, 64)
		if err != nil {
			return time.Time{}, errors.Wrap(err, "parse unix timestamp")
		}
		switch len(s) {
		case 19:
			return time.Unix(0, v), nil
		case 16:
			return time.UnixMicro(v), nil
		case 13:
			return time.UnixMilli(v), nil
		default:
			return time.Unix(v, 0), nil
		}
	}

	ref, offset := s, ""
	if i := strings.IndexAny(s, "+-"); i != -1 {
		ref, offset = s[:i], s[i:]
	}
	if ref != "now" {
		return time.Time{}, errors.Errorf("unsupported time reference %q", ref)
	}

	d, err := parseTimeOffset(offset)
	if err != nil {
		return time.Time{}, err
	}
	return now.Add(d), nil
}

func parseTimeOffset(offset string) (time.Duration, error) {
	if offset == "" {
		return 0, nil
	}

	sign := time.Duration(1)
	switch offset[0] {
	case '-':
		sign = -1
		offset = offset[1:]
	case '+':
		offset = offset[1:]
	}

	var d time.Duration
	for offset != "" {
		i := 0
		for i < len(offset) && isDigit(offset[i]) {
			i++
		}
		if i == 0 {
			return 0, errors.Errorf("invalid offset %q: expected number", offset)
		}
		num, err := strconv.Atoi(offset[:i])
		if err != nil {
			return 0, errors.Wrap(err, "parse offset number")
		}
		offset = offset[i:]

		j := 0
		for j < len(offset) && !isDigit(offset[j]) {
			j++
		}
		unit := offset[:j]
		offset = offset[j:]

		mult, err := unitDuration(unit)
		if err != nil {
			return 0, err
		}
		d += sign * time.Duration(num) * mult
	}
	return d, nil
}

func unitDuration(s string) (time.Duration, error) {
	switch {
	case strings.HasPrefix(s, "s"):
		return time.Second, nil
	case strings.HasPrefix(s, "mon"), strings.HasPrefix(s, "M"):
		return 30 * 24 * time.Hour, nil
	case strings.HasPrefix(s, "min"), strings.HasPrefix(s, "m"):
		return time.Minute, nil
	case strings.HasPrefix(s, "h"):
		return time.Hour, nil
	case strings.HasPrefix(s, "d"):
		return 24 * time.Hour, nil
	case strings.HasPrefix(s, "w"):
		return 7 * 24 * time.Hour, nil
	case strings.HasPrefix(s, "y"):
		return 365 * 24 * time.Hour, nil
	default:
		return 0, errors.Errorf("unknown time unit %q", s)
	}
}

func isDigits(s string) bool {
	if s == "" {
		return false
	}
	for i := 0; i < len(s); i++ {
		if !isDigit(s[i]) {
			return false
		}
	}
	return true
}

func isDigit(b byte) bool {
	return b >= '0' && b <= '9'
}
