#!/bin/bash
set -eou pipefail

table="us_equities.tickers"
clickhouse-client --query="drop table if exists $table"
clickhouse-client --query="create table if not exists $table (
	symbol						LowCardinality(String),
	last_updated_utc	Date,
	name							LowCardinality(String),
	primary_exchange	LowCardinality(String),
	type							LowCardinality(String),
	cik								LowCardinality(String),
	composite_figi		LowCardinality(String),
	share_class_figi	LowCardinality(String)
)
Engine = MergeTree
partition by toYear(last_updated_utc)
order by (last_updated_utc);"

data_dir="/mnt/raid0/csv/data/tickers"
for f in $data_dir/*; do
	echo $f
	sed 's/T00:00:00+00:00//g' $f | \
		sed 's/,null$/,/g' | \
		clickhouse-client \
		--input_format_skip_unknown_fields=true \
		--format_csv_allow_single_quotes=0 \
		--date_time_input_format='best_effort' \
		--query="INSERT INTO $table FORMAT CSVWithNames"
done
