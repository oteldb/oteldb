package chstorage

import "time"

// forEachDayBucket calls fn for each UTC-day-aligned window covering [mint, maxt]. Day-aligning
// keeps each scan inside a single daily table partition; the first bucket's lower bound is clamped
// to mint so a `since`-limited window skips the early part of its first day instead of scanning the
// whole calendar day.
//
// Only the lower bound is clamped. The upper bound stays at the day's end because mint/maxt come
// from queryMinMaxTimestamp, which floors to whole seconds (toDateTime): clamping the top to a
// floored maxt would drop any sub-second data in maxt's final second. Since no data exists past the
// true max, a full-day upper bound reads the same rows without that risk.
func forEachDayBucket(mint, maxt time.Time, fn func(from, to time.Time) error) error {
	const step = 24 * time.Hour
	start := mint.Truncate(step)
	end := maxt.Truncate(step).Add(step)
	for ts := start; ts.Before(end); ts = ts.Add(step) {
		from, to := ts, ts.Add(step)
		if from.Before(mint) {
			from = mint
		}
		if err := fn(from, to); err != nil {
			return err
		}
	}
	return nil
}
