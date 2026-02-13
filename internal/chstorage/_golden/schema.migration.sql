CREATE TABLE IF NOT EXISTS `migration`
(
	`table`    String,
	`ddl`      String,
	`ddl_hash` String,
	`ts`       DateTime DEFAULT now()
)
ENGINE = ReplacingMergeTree(ts)
ORDER BY (`table`)
