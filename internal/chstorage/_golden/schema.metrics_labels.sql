CREATE TABLE IF NOT EXISTS `metrics_labels`
(
	`tenant_id` LowCardinality(String),
	`name`      LowCardinality(String),
	`value`     String,
	`scope`     Enum8('NONE' = 0, 'RESOURCE' = 1, 'INSTRUMENTATION' = 2, 'ATTRIBUTE' = 4)
)
ENGINE = ReplacingMergeTree
ORDER BY (`tenant_id`, `name`, `value`, `scope`)
