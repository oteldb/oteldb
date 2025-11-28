CREATE TABLE IF NOT EXISTS `metrics_exemplars`
(
	`hash`                FixedString(16),
	`timestamp`           DateTime64(3)   CODEC(Delta, ZSTD(1)),
	`filtered_attributes` String,
	`exemplar_timestamp`  DateTime64(3)   CODEC(Delta, ZSTD(1)),
	`value`               Float64,
	`trace_id`            FixedString(16),
	`span_id`             FixedString(8)
)
ENGINE = MergeTree
PARTITION BY toYYYYMMDD(timestamp)
ORDER BY (`hash`, `timestamp`)
