CREATE TABLE IF NOT EXISTS `traces_tags`
(
	`tenant_id`  LowCardinality(String),
	`name`       LowCardinality(String),
	`value`      String,
	`value_type` Enum8('EMPTY' = 0,'STR' = 1,'INT' = 2,'DOUBLE' = 3,'BOOL' = 4,'MAP' = 5,'SLICE' = 6,'BYTES' = 7),
	`scope`      Enum8('NONE' = 0, 'RESOURCE' = 1, 'SPAN' = 2, 'INSTRUMENTATION' = 3)
)
ENGINE = ReplacingMergeTree
ORDER BY (`tenant_id`, `value_type`, `name`, `value`)
