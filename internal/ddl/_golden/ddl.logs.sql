CREATE TABLE IF NOT EXISTS `logs` ON CLUSTER 'foo'
(
	`a`   Int32,
	`bar` LowCardinality(String) CODEC(ZSTD(1)),
	`c`   String                 COMMENT 'foo.bar' CODEC(ZSTD(1)),

	INDEX `idx_trace_id`   trace_id TYPE bloom_filter(0.001) GRANULARITY 1,
	INDEX `idx_body`       body TYPE tokenbf_v1(32768, 3, 0) GRANULARITY 1,
	INDEX `idx_ts`         timestamp TYPE minmax GRANULARITY 8192,
	INDEX `attribute_keys` arrayConcat(JSONExtractKeys(attribute), JSONExtractKeys(scope), JSONExtractKeys(resource)) TYPE set(100)
)
ENGINE = MergeTree
ORDER BY (`a`, toStartOfHour(b))
TTL toDateTime(`timestamp`) + toIntervalSecond(36000)