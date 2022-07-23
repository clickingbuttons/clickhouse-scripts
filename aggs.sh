#!/bin/bash
clickhouse-client --query="drop view if exists us_equities.agg1m"
clickhouse-client --query="CREATE MATERIALIZED VIEW us_equities.agg1m
	ENGINE = AggregatingMergeTree
	PARTITION BY toYYYYMM(time)
	POPULATE
	ORDER BY (ticker, time) AS
	SELECT
			ticker,
			toStartOfMinute(time) AS time,
			any(price) AS open,
			max(price) AS high,
			min(price) AS low,
			anyLast(price) AS close,
			sum(size) AS volume,
			sum(price * size) / sum(size) AS vwap,
			count() AS n
	FROM us_equities.trades
	WHERE update_intraday = true
	GROUP BY
			ticker,
			time"
clickhouse-client --query="drop view if exists us_equities.agg1d"
clickhouse-client --query="CREATE MATERIALIZED VIEW us_equities.agg1d
	ENGINE = AggregatingMergeTree
	PARTITION BY toYear(time)
	POPULATE
	ORDER BY (ticker, time) AS
	SELECT
			ticker,
			toStartOfDay(time) AS time,
			any(price) AS open,
			max(price) AS high,
			min(price) AS low,
			anyLast(price) AS close,
			sum(size) AS volume,
			sum(price * size) / sum(size) AS vwap,
			count() AS n
	FROM us_equities.trades
	WHERE update_day = true
	GROUP BY
			ticker,
			time"

#clickhouse-client --query="drop view if exists us_equities.agg1m_cons"
#clickhouse-client --query="CREATE MATERIALIZED VIEW us_equities.agg1m_cons
#	ENGINE = AggregatingMergeTree
#	PARTITION BY toYYYYMM(time)
#	ORDER BY (ticker, time) AS
#	SELECT *
#	FROM
#	(
#			SELECT
#					ticker,
#					toStartOfMinute(time) AS time,
#					anyIf(price, update_high_low) AS open,
#					maxIf(price, update_last) AS high,
#					minIf(price, update_last) AS low,
#					anyLastIf(price, update_high_low) AS close,
#					sumIf(size, update_volume) AS volume,
#					sum(price * size) / sum(size) AS vwap,
#					count() AS n
#			FROM us_equities.trades
#			GROUP BY
#					ticker,
#					time
#	)
#	WHERE (open != 0) AND (high != 0) AND (low != 0) AND (close != 0)"
#clickhouse-client --query="drop view if exists us_equities.agg1d_cons"
#clickhouse-client --query="CREATE MATERIALIZED VIEW us_equities.agg1d_cons
#	ENGINE = AggregatingMergeTree
#	PARTITION BY toYear(time)
#	ORDER BY (ticker, time) AS
#	SELECT *
#	FROM
#	(
#			SELECT
#					ticker,
#					toStartOfDay(time) AS time,
#					anyIf(price, update_high_low) AS open,
#					maxIf(price, update_last) AS high,
#					minIf(price, update_last) AS low,
#					anyLastIf(price, update_high_low) AS close,
#					sumIf(size, update_volume) AS volume,
#					sum(price * size) / sum(size) AS vwap,
#					count() AS n
#			FROM us_equities.trades
#			GROUP BY
#					ticker,
#					time
#	)
#	WHERE (open != 0) AND (high != 0) AND (low != 0) AND (close != 0)"
