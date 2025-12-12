package chstorage

import (
	"errors"
	"strconv"

	"github.com/go-faster/jx"
)

type metricMapping int8

func (m metricMapping) NameSuffix() string {
	switch m {
	case histogramMin:
		return "_min"
	case histogramMax:
		return "_max"
	case histogramBucket:
		return "_bucket"
	case histogramCount, summaryCount:
		return "_count"
	case histogramSum, summarySum:
		return "_sum"
	default:
		return ""
	}
}

func (m metricMapping) IsHistogram() bool {
	switch m {
	case histogramCount,
		histogramSum,
		histogramMin,
		histogramMax:
		return true
	default:
		return false
	}
}

func (m metricMapping) IsSummary() bool {
	switch m {
	case summaryCount,
		summaryQuantile,
		summarySum:
		return true
	default:
		return false
	}
}

const (
	noMapping metricMapping = iota
	histogramCount
	histogramSum
	histogramMin
	histogramMax
	histogramBucket
	summaryCount
	summarySum
	summaryQuantile
)

const (
	metricMappingDDL = `
		'NO_MAPPING' = 0,
		'HISTOGRAM_COUNT' = 1,
		'HISTOGRAM_SUM' = 2,
		'HISTOGRAM_MIN' = 3,
		'HISTOGRAM_MAX' = 4,
		'HISTOGRAM_BUCKET' = 5,
		'SUMMARY_COUNT' = 6,
		'SUMMARY_SUM' = 7,
		'SUMMARY_QUANTILE' = 8
		`
	metricLabelScopeDDL = `'NONE' = 0, 'RESOURCE' = 1, 'INSTRUMENTATION' = 2, 'ATTRIBUTE' = 4`
)

func parseLabels(s []byte, to map[string]string) error {
	d := jx.DecodeBytes(s)
	return d.ObjBytes(func(d *jx.Decoder, key []byte) error {
		switch d.Next() {
		case jx.String:
			val, err := d.Str()
			if err != nil {
				return err
			}
			to[string(key)] = val
			return nil
		case jx.Number:
			val, err := d.Num()
			if err != nil {
				return err
			}
			to[string(key)] = val.String()
			return nil
		case jx.Null:
			return d.Null()
		case jx.Bool:
			val, err := d.Bool()
			if err != nil {
				return err
			}
			to[string(key)] = strconv.FormatBool(val)
			return nil
		case jx.Array, jx.Object:
			val, err := d.Raw()
			if err != nil {
				return err
			}
			to[string(key)] = val.String()
			return nil
		default:
			return errors.New("invalid type")
		}
	})
}
