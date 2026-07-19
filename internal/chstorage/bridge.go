package chstorage

import "time"

// forEachDayBucket calls fn for each UTC-day-aligned window covering [mint, maxt], with the first
// and last buckets clamped to mint/maxt respectively. Day-aligning keeps each scan inside a single
// daily table partition, while clamping ensures a bounded scan (e.g. a `since`-limited window)
// reads only the requested range instead of the whole calendar day at each end.
func forEachDayBucket(mint, maxt time.Time, fn func(from, to time.Time) error) error {
	const step = 24 * time.Hour
	start := mint.Truncate(step)
	end := maxt.Truncate(step).Add(step)
	for ts := start; ts.Before(end); ts = ts.Add(step) {
		from, to := ts, ts.Add(step)
		if from.Before(mint) {
			from = mint
		}
		if to.After(maxt) {
			to = maxt
		}
		if err := fn(from, to); err != nil {
			return err
		}
	}
	return nil
}
