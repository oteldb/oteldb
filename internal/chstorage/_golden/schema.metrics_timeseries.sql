CREATE TABLE IF NOT EXISTS `metrics_timeseries`
(
	`tenant_id`   LowCardinality(String),
	`name`        LowCardinality(String)                                   CODEC(ZSTD(1)),
	`unit`        SimpleAggregateFunction(anyLast, LowCardinality(String)) CODEC(ZSTD(1)),
	`description` SimpleAggregateFunction(anyLast, String)                 CODEC(ZSTD(3)),
	`first_seen`  SimpleAggregateFunction(min, DateTime64(9)),
	`last_seen`   SimpleAggregateFunction(max, DateTime64(9)),
	`hash`        SimpleAggregateFunction(any, FixedString(16)),
	`attribute`   LowCardinality(String),
	`resource`    LowCardinality(String),
	`scope`       LowCardinality(String)
)
ENGINE = AggregatingMergeTree
ORDER BY (`tenant_id`, `name`, `resource`, `scope`, `attribute`)
PRIMARY KEY (`tenant_id`, `name`, `resource`, `scope`, `attribute`)
