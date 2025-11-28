CREATE TABLE IF NOT EXISTS `metrics_points`
(
	`hash`      LowCardinality(FixedString(16)),
	`timestamp` DateTime64(3)                   CODEC(Delta, ZSTD(1)),
	`value`     Float64                         CODEC(FPC, ZSTD(1)),
	`mapping`   Enum8(
		'NO_MAPPING' = 0,
		'HISTOGRAM_COUNT' = 1,
		'HISTOGRAM_SUM' = 2,
		'HISTOGRAM_MIN' = 3,
		'HISTOGRAM_MAX' = 4,
		'HISTOGRAM_BUCKET' = 5,
		'SUMMARY_COUNT' = 6,
		'SUMMARY_SUM' = 7,
		'SUMMARY_QUANTILE' = 8
		) CODEC(T64, ZSTD(1)),
	`flags`     UInt8                           CODEC(T64, ZSTD(1))
)
ENGINE = MergeTree
PARTITION BY toYYYYMMDD(timestamp)
ORDER BY (`hash`, `timestamp`)
TTL toDateTime(`timestamp`) + toIntervalSecond(259200)