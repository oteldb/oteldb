CREATE TABLE IF NOT EXISTS `logs_attrs`
(
	`tenant_id` LowCardinality(String),
	`name`      String                 COMMENT 'foo_bar',
	`key`       String                 COMMENT 'foo.bar'
)
ENGINE = ReplacingMergeTree
ORDER BY (`tenant_id`, `name`)
