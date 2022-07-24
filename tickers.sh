#!/bin/bash
set -eou pipefail

table="us_equities.tickers"
clickhouse-client --query="drop table if exists $table"
clickhouse-client --query="CREATE TABLE IF NOT EXISTS $table (
	ticker													LowCardinality(String),
	time														Date,
	name														LowCardinality(String),
	primary_exchange								LowCardinality(String),
	type														LowCardinality(String),
	cik															LowCardinality(String),
	composite_figi									LowCardinality(String),
	share_class_figi								LowCardinality(String),
	phone_number										LowCardinality(String),
	description											LowCardinality(String),
	sic_code												Nullable(UInt16),
	ticker_root											LowCardinality(String),
	homepage_url										LowCardinality(String),
	total_employees									Nullable(UInt32),
	list_date												Nullable(Date),
	share_class_shares_outstanding	Nullable(Float64),
	weighted_shares_outstanding			Nullable(Float64)
)
Engine = MergeTree
PARTITION BY toYear(time)
ORDER BY (ticker, time);"

