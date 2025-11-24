package chstorage

import (
	"time"

	"github.com/ClickHouse/ch-go/proto"

	"github.com/go-faster/oteldb/internal/chstorage/chsql"
	"github.com/go-faster/oteldb/internal/ddl"
)

type timeseriesColumns struct {
	name *proto.ColLowCardinality[string]
	hash *colSimpleAggregateFunction[[16]byte]

	firstSeen *colSimpleAggregateFunction[time.Time]
	lastSeen  *colSimpleAggregateFunction[time.Time]

	attributes *Attributes
	scope      *Attributes
	resource   *Attributes
}

func newTimeseriesColumns() *timeseriesColumns {
	return &timeseriesColumns{
		name: new(proto.ColStr).LowCardinality(),

		hash:      &colSimpleAggregateFunction[[16]byte]{Function: "any", Data: new(proto.ColFixedStr16)},
		firstSeen: &colSimpleAggregateFunction[time.Time]{Function: "min", Data: new(proto.ColDateTime64).WithPrecision(proto.PrecisionNano)},
		lastSeen:  &colSimpleAggregateFunction[time.Time]{Function: "max", Data: new(proto.ColDateTime64).WithPrecision(proto.PrecisionNano)},

		attributes: NewAttributes(colAttrs),
		scope:      NewAttributes(colScope),
		resource:   NewAttributes(colResource),
	}
}

func (c *timeseriesColumns) Columns() Columns {
	return MergeColumns(
		Columns{
			{Name: "name", Data: c.name},
			{Name: "hash", Data: c.hash},
			{Name: "first_seen", Data: c.firstSeen},
			{Name: "last_seen", Data: c.lastSeen},
		},
		c.attributes.Columns(),
		c.scope.Columns(),
		c.resource.Columns(),
	)
}

func (c *timeseriesColumns) Input() proto.Input                { return c.Columns().Input() }
func (c *timeseriesColumns) Result() proto.Results             { return c.Columns().Result() }
func (c *timeseriesColumns) ChsqlResult() []chsql.ResultColumn { return c.Columns().ChsqlResult() }

func (c *timeseriesColumns) DDL() ddl.Table {
	table := ddl.Table{
		Engine:     ddl.Engine{Type: "AggregatingMergeTree"},
		PrimaryKey: []string{"name", "resource", "scope", "attribute"},
		OrderBy:    []string{"name", "resource", "scope", "attribute"},
		Columns: []ddl.Column{
			{
				Name:  "name",
				Type:  c.name.Type(),
				Codec: "ZSTD(1)",
			},
			{
				Name: "first_seen",
				Type: c.firstSeen.Type(),
			},
			{
				Name: "last_seen",
				Type: c.lastSeen.Type(),
			},
			{
				Name: "hash",
				Type: c.hash.Type(),
			},
		},
	}

	c.attributes.DDL(&table)
	c.resource.DDL(&table)
	c.scope.DDL(&table)

	return table
}

type pointColumns struct {
	hash      proto.ColumnOf[[16]byte]
	timestamp *proto.ColDateTime64

	value proto.ColFloat64

	mapping proto.ColEnum8
	flags   proto.ColUInt8
}

func newPointColumns() *pointColumns {
	return &pointColumns{
		timestamp: new(proto.ColDateTime64).WithPrecision(proto.PrecisionMilli),
		hash:      proto.NewLowCardinality(&proto.ColFixedStr16{}),
	}
}

func (c *pointColumns) Columns() Columns {
	return MergeColumns(
		Columns{
			{Name: "hash", Data: c.hash},
			{Name: "timestamp", Data: c.timestamp},

			{Name: "value", Data: &c.value},

			{Name: "mapping", Data: proto.Wrap(&c.mapping, metricMappingDDL)},
			{Name: "flags", Data: &c.flags},
		},
	)
}

func (c *pointColumns) Input() proto.Input                { return c.Columns().Input() }
func (c *pointColumns) Result() proto.Results             { return c.Columns().Result() }
func (c *pointColumns) ChsqlResult() []chsql.ResultColumn { return c.Columns().ChsqlResult() }

func (c *pointColumns) DDL() ddl.Table {
	table := ddl.Table{
		Engine:      ddl.Engine{Type: "MergeTree"},
		PartitionBy: "toYYYYMMDD(timestamp)",
		OrderBy:     []string{"toStartOfHour(timestamp)", "hash", "timestamp"},
		TTL:         ddl.TTL{Field: "timestamp"},
		Columns: []ddl.Column{
			{
				Name: "hash",
				Type: c.hash.Type(),
			},
			{
				Name:  "timestamp",
				Type:  c.timestamp.Type(),
				Codec: "Delta, ZSTD(1)",
			},
			{
				Name:  "value",
				Type:  c.value.Type(),
				Codec: "FPC, ZSTD(1)",
			},
			{
				Name:  "mapping",
				Type:  c.mapping.Type().Sub(metricMappingDDL),
				Codec: "T64, ZSTD(1)",
			},
			{
				Name:  "flags",
				Type:  c.flags.Type(),
				Codec: "T64, ZSTD(1)",
			},
		},
	}

	return table
}

type expHistogramColumns struct {
	hash      proto.ColFixedStr16
	timestamp *proto.ColDateTime64

	count                proto.ColUInt64
	sum                  *proto.ColNullable[float64]
	min                  *proto.ColNullable[float64]
	max                  *proto.ColNullable[float64]
	scale                proto.ColInt32
	zerocount            proto.ColUInt64
	positiveOffset       proto.ColInt32
	positiveBucketCounts *proto.ColArr[uint64]
	negativeOffset       proto.ColInt32
	negativeBucketCounts *proto.ColArr[uint64]

	flags proto.ColUInt8
}

func newExpHistogramColumns() *expHistogramColumns {
	return &expHistogramColumns{
		timestamp: new(proto.ColDateTime64).WithPrecision(proto.PrecisionMilli),

		sum:                  new(proto.ColFloat64).Nullable(),
		min:                  new(proto.ColFloat64).Nullable(),
		max:                  new(proto.ColFloat64).Nullable(),
		positiveBucketCounts: new(proto.ColUInt64).Array(),
		negativeBucketCounts: new(proto.ColUInt64).Array(),
	}
}

func (c *expHistogramColumns) Columns() Columns {
	return MergeColumns(
		Columns{
			{Name: "hash", Data: &c.hash},
			{Name: "timestamp", Data: c.timestamp},

			{Name: "exp_histogram_count", Data: &c.count},
			{Name: "exp_histogram_sum", Data: c.sum},
			{Name: "exp_histogram_min", Data: c.min},
			{Name: "exp_histogram_max", Data: c.max},
			{Name: "exp_histogram_scale", Data: &c.scale},
			{Name: "exp_histogram_zerocount", Data: &c.zerocount},
			{Name: "exp_histogram_positive_offset", Data: &c.positiveOffset},
			{Name: "exp_histogram_positive_bucket_counts", Data: c.positiveBucketCounts},
			{Name: "exp_histogram_negative_offset", Data: &c.negativeOffset},
			{Name: "exp_histogram_negative_bucket_counts", Data: c.negativeBucketCounts},

			{Name: "flags", Data: &c.flags},
		},
	)
}

func (c *expHistogramColumns) Input() proto.Input                { return c.Columns().Input() }
func (c *expHistogramColumns) Result() proto.Results             { return c.Columns().Result() }
func (c *expHistogramColumns) ChsqlResult() []chsql.ResultColumn { return c.Columns().ChsqlResult() }

func (c *expHistogramColumns) DDL() ddl.Table {
	table := ddl.Table{
		Engine:      ddl.Engine{Type: "MergeTree"},
		PartitionBy: "toYYYYMMDD(timestamp)",
		OrderBy:     []string{"toStartOfHour(timestamp)", "hash", "timestamp"},
		Columns: []ddl.Column{
			{
				Name: "hash",
				Type: c.hash.Type(),
			},
			{
				Name:  "timestamp",
				Type:  c.timestamp.Type(),
				Codec: "Delta, ZSTD(1)",
			},
			{
				Name: "exp_histogram_count",
				Type: c.count.Type(),
			},
			{
				Name: "exp_histogram_sum",
				Type: c.sum.Type(),
			},
			{
				Name: "exp_histogram_min",
				Type: c.min.Type(),
			},
			{
				Name: "exp_histogram_max",
				Type: c.max.Type(),
			},
			{
				Name: "exp_histogram_scale",
				Type: c.scale.Type(),
			},
			{
				Name: "exp_histogram_zerocount",
				Type: c.zerocount.Type(),
			},
			{
				Name: "exp_histogram_positive_offset",
				Type: c.positiveOffset.Type(),
			},
			{
				Name: "exp_histogram_positive_bucket_counts",
				Type: c.positiveBucketCounts.Type(),
			},
			{
				Name: "exp_histogram_negative_offset",
				Type: c.negativeOffset.Type(),
			},
			{
				Name: "exp_histogram_negative_bucket_counts",
				Type: c.negativeBucketCounts.Type(),
			},
			{
				Name:  "flags",
				Type:  c.flags.Type(),
				Codec: "T64, ZSTD(1)",
			},
		},
	}

	return table
}

type labelsColumns struct {
	name  *proto.ColLowCardinality[string]
	value proto.ColStr
	scope proto.ColEnum8
}

func newLabelsColumns() *labelsColumns {
	return &labelsColumns{
		name: new(proto.ColStr).LowCardinality(),
	}
}

func (c *labelsColumns) Columns() Columns {
	return Columns{
		{Name: "name", Data: c.name},
		{Name: "value", Data: &c.value},
		{Name: "scope", Data: proto.Wrap(&c.scope, metricLabelScopeDDL)},
	}
}
func (c *labelsColumns) Input() proto.Input                { return c.Columns().Input() }
func (c *labelsColumns) Result() proto.Results             { return c.Columns().Result() }
func (c *labelsColumns) ChsqlResult() []chsql.ResultColumn { return c.Columns().ChsqlResult() }

func (c *labelsColumns) DDL() ddl.Table {
	return ddl.Table{
		Engine:  ddl.Engine{Type: "ReplacingMergeTree"},
		OrderBy: []string{"name", "value", "scope"},
		Columns: []ddl.Column{
			{
				Name: "name",
				Type: c.name.Type(),
			},
			{
				Name: "value",
				Type: c.value.Type(),
			},
			{
				Name: "scope",
				Type: c.scope.Type().Sub(metricLabelScopeDDL),
			},
		},
	}
}

type exemplarColumns struct {
	hash      proto.ColFixedStr16
	timestamp *proto.ColDateTime64

	filteredAttributes proto.ColBytes
	exemplarTimestamp  *proto.ColDateTime64
	value              proto.ColFloat64
	spanID             proto.ColFixedStr8
	traceID            proto.ColFixedStr16
}

func newExemplarColumns() *exemplarColumns {
	return &exemplarColumns{
		timestamp:         new(proto.ColDateTime64).WithPrecision(proto.PrecisionMilli),
		exemplarTimestamp: new(proto.ColDateTime64).WithPrecision(proto.PrecisionMilli),
	}
}

func (c *exemplarColumns) Columns() Columns {
	return MergeColumns(
		Columns{
			{Name: "hash", Data: &c.hash},
			{Name: "timestamp", Data: c.timestamp},

			{Name: "filtered_attributes", Data: &c.filteredAttributes},
			{Name: "exemplar_timestamp", Data: c.exemplarTimestamp},
			{Name: "value", Data: &c.value},
			{Name: "span_id", Data: &c.spanID},
			{Name: "trace_id", Data: &c.traceID},
		},
	)
}

func (c *exemplarColumns) Input() proto.Input                { return c.Columns().Input() }
func (c *exemplarColumns) Result() proto.Results             { return c.Columns().Result() }
func (c *exemplarColumns) ChsqlResult() []chsql.ResultColumn { return c.Columns().ChsqlResult() }

func (c *exemplarColumns) DDL() ddl.Table {
	table := ddl.Table{
		Engine:      ddl.Engine{Type: "MergeTree"},
		PartitionBy: "toYYYYMMDD(timestamp)",
		OrderBy:     []string{"toStartOfHour(timestamp)", "hash", "timestamp"},
		Columns: []ddl.Column{
			{
				Name: "hash",
				Type: c.hash.Type(),
			},
			{
				Name:  "timestamp",
				Type:  c.timestamp.Type(),
				Codec: "Delta, ZSTD(1)",
			},
			{
				Name: "filtered_attributes",
				Type: c.filteredAttributes.Type(),
			},
			{
				Name:  "exemplar_timestamp",
				Type:  c.exemplarTimestamp.Type(),
				Codec: "Delta, ZSTD(1)",
			},
			{
				Name: "value",
				Type: c.value.Type(),
			},
			{
				Name: "trace_id",
				Type: c.traceID.Type(),
			},
			{
				Name: "span_id",
				Type: c.spanID.Type(),
			},
		},
	}

	return table
}
