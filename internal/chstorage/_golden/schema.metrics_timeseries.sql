CREATE TABLE IF NOT EXISTS `metrics_timeseries`
(
	`name`       LowCardinality(String)                        CODEC(ZSTD(1)),
	`first_seen` SimpleAggregateFunction(min, DateTime64(9)),
	`last_seen`  SimpleAggregateFunction(max, DateTime64(9)),
	`hash`       SimpleAggregateFunction(any, FixedString(16)),
	`attribute`  LowCardinality(String),
	`resource`   LowCardinality(String),
	`scope`      LowCardinality(String)
)
ENGINE = AggregatingMergeTree
ORDER BY (`name`, `resource`, `scope`, `attribute`)
PRIMARY KEY (`name`, `resource`, `scope`, `attribute`)
