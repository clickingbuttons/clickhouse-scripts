#!/bin/bash
set -eou pipefail

# we make tables and then material views TO those tables so that cascading
# works properly

# agg1m
clickhouse-client --query="drop table if exists us_equities.agg1m"
clickhouse-client --query="CREATE TABLE us_equities.agg1m (
		ticker	LowCardinality(String),
		time		DateTime('America/New_York'),
		open		Float64,
		high		Float64,
		low			Float64,
		close		Float64,
		volume	UInt32,
		vwap		Float64,
		count		UInt32
	)
	ENGINE = AggregatingMergeTree
	PARTITION BY toYYYYMM(time)
	ORDER BY (ticker, time)"
clickhouse-client --query="drop view if exists us_equities.agg1m_agger"
clickhouse-client --query="CREATE MATERIALIZED VIEW us_equities.agg1m_agger
	TO us_equities.agg1m
	AS SELECT
		ticker,
		toStartOfMinute(time) AS time,
		any(price) AS open,
		max(price) AS high,
		min(price) AS low,
		anyLast(price) AS close,
		sum(size) AS volume,
		sum(price * size) / sum(size) AS vwap,
		count() AS count
	FROM us_equities.trades
	WHERE update_intraday = true
	GROUP BY
			ticker,
			time"

# agg1d
clickhouse-client --query="drop table if exists us_equities.agg1d"
clickhouse-client --query="CREATE TABLE us_equities.agg1d (
		ticker	LowCardinality(String),
		time		DateTime('America/New_York'),
		open		Float64,
		high		Float64,
		low			Float64,
		close		Float64,
		volume	UInt64,
		vwap		Float64,
		count		UInt64
	)
	ENGINE = AggregatingMergeTree
	PARTITION BY toYear(time)
	ORDER BY (ticker, time)"
clickhouse-client --query="drop view if exists us_equities.agg1d_agger"
clickhouse-client --query="CREATE MATERIALIZED VIEW us_equities.agg1d_agger
	TO us_equities.agg1d
	AS SELECT
		ticker,
		toStartOfDay(time) AS time,
		any(price) AS open,
		max(price) AS high,
		min(price) AS low,
		anyLast(price) AS close,
		sum(size) AS volume,
		sum(price * size) / sum(size) AS vwap,
		count() AS count
	FROM us_equities.trades
	WHERE update_day = true
	GROUP BY
		ticker,
		time"

# agg1d_intra
clickhouse-client --query="drop table if exists us_equities.agg1d_intra"
clickhouse-client --query="CREATE TABLE us_equities.agg1d_intra (
		ticker	LowCardinality(String),
		time		DateTime('America/New_York'),
		open		Float64,
		high		Float64,
		low			Float64,
		close		Float64,
		volume	UInt64,
		vwap		Float64,
		count		UInt64
	)
	ENGINE = AggregatingMergeTree
	PARTITION BY toYear(time)
	ORDER BY (ticker, time)"
clickhouse-client --query="drop view if exists us_equities.agg1d_intra_agger"
clickhouse-client --query="CREATE MATERIALIZED VIEW us_equities.agg1d_intra_agger
	TO us_equities.agg1d_intra
	AS SELECT
		ticker,
		toStartOfDay(time) AS time,
		any(a.open) AS open,
		max(a.high) AS high,
		min(a.low) AS low,
		anyLast(a.close) AS close,
		sum(a.volume) AS volume,
		sum(a.vwap * a.volume) / sum(a.volume) AS vwap,
		sum(a.count) AS count
	FROM us_equities.agg1m as a
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
