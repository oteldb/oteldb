CREATE TABLE IF NOT EXISTS `metrics_exp_histograms`
(
	`hash`                                 FixedString(16),
	`timestamp`                            DateTime64(3)     CODEC(Delta, ZSTD(1)),
	`exp_histogram_count`                  UInt64,
	`exp_histogram_sum`                    Nullable(Float64),
	`exp_histogram_min`                    Nullable(Float64),
	`exp_histogram_max`                    Nullable(Float64),
	`exp_histogram_scale`                  Int32,
	`exp_histogram_zerocount`              UInt64,
	`exp_histogram_positive_offset`        Int32,
	`exp_histogram_positive_bucket_counts` Array(UInt64),
	`exp_histogram_negative_offset`        Int32,
	`exp_histogram_negative_bucket_counts` Array(UInt64),
	`flags`                                UInt8             CODEC(T64, ZSTD(1))
)
ENGINE = MergeTree
PARTITION BY toYYYYMMDD(timestamp)
ORDER BY (`hash`, `timestamp`)
