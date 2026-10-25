package scarecrow

import "context"

type histogramBucketValues struct {
	values    []float64
	positions []int
	valid     []uint64
	cursor    int
}

func copyHistogramBucket(ctx context.Context, column *Column, count int) (histogramBucketValues, error) {
	if err := ctx.Err(); err != nil {
		return histogramBucketValues{}, err
	}
	out := histogramBucketValues{values: make([]float64, 0, count)}
	if count == column.Steps() {
		out.values = append(out.values, column.V...)
		return out, ctx.Err()
	}
	if count < wordsFor(column.Steps()) {
		out.positions = make([]int, 0, count)
	} else {
		out.valid = append([]uint64(nil), column.Valid...)
	}
	for step := range column.Steps() {
		if step&4095 == 0 {
			if err := ctx.Err(); err != nil {
				return histogramBucketValues{}, err
			}
		}
		if column.IsSet(step) {
			out.values = append(out.values, column.V[step])
			if out.positions != nil {
				out.positions = append(out.positions, step)
			}
		}
	}
	return out, nil
}

// valueAt visits steps in order, advancing through compact sparse values only when present.
func (b *histogramBucketValues) valueAt(step int) (float64, bool) {
	if b.positions == nil && b.valid == nil {
		return b.values[step], true
	}
	if b.cursor == len(b.values) {
		return 0, false
	}
	if b.positions != nil {
		if b.positions[b.cursor] != step {
			return 0, false
		}
	} else if !bitSet(b.valid, step) {
		return 0, false
	}
	value := b.values[b.cursor]
	b.cursor++
	return value, true
}
