CREATE TABLE IF NOT EXISTS `table`
(
	`a` Int32 DEFAULT now(),
	`b` Int32 MATERIALIZED 1
)
ENGINE = ReplacingMergeTree(a)
ORDER BY (`a`, `b`)
