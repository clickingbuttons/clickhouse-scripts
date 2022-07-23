#!/bin/bash
set -eou pipefail

table="us_equities.trades"
# CTA interpretation... kinda
## page 43 https://utpplan.com/DOC/UtpBinaryOutputSpec.pdf
## page 64 https://www.ctaplan.com/publicdocs/ctaplan/CTS_Pillar_Output_Specification.pdf
#clickhouse-client --query="drop function if exists neverAgg"
#clickhouse-client --query="create function neverAgg as (conditions) -> not hasAny(conditions, [2, 7, 21, 37, 15, 20, 16, 29, 52, 53])"
#
## TODO: figure out state for only updating last if there is no last set
## hint: https://clickhouse.com/docs/en/sql-reference/functions/other-functions#runningaccumulate
#clickhouse-client --query="drop function if exists ctaUpdateHighLow"
#clickhouse-client --query="drop function if exists ctaUpdateLast"
#clickhouse-client --query="create function ctaUpdateHighLow as (conditions) -> neverAgg(conditions)"
#clickhouse-client --query="create function ctaUpdateLast as (conditions) -> neverAgg(conditions)"
#
#clickhouse-client --query="drop function if exists utpUpdateHighLow"
#clickhouse-client --query="drop function if exists utpUpdateLast"
#clickhouse-client --query="create function utpUpdateHighLow as (conditions) -> neverAgg(conditions)"
#clickhouse-client --query="create function utpUpdateLast as (conditions) -> neverAgg(conditions)"
#
#clickhouse-client --query="drop function if exists updateHighLow"
#clickhouse-client --query="create function updateHighLow as (conditions, tape) -> tape=1 ? ctaUpdateHighLow(conditions) : utpUpdateHighLow(conditions)"
#clickhouse-client --query="drop function if exists updateLast"
#clickhouse-client --query="create function updateLast as (conditions, tape) -> tape=1 ? ctaUpdateLast(conditions) : utpUpdateLast(conditions)"
#clickhouse-client --query="drop function if exists updateVolume"
#clickhouse-client --query="create function updateVolume as (conditions) -> not hasAny(conditions, [15, 16, 38])"

# zack's interpretation
# page 17 https://www.nyse.com/publicdocs/nyse/data/Daily_TAQ_Client_Spec_v3.0.pdf
# page 16 https://utpplan.com/doc/utpbinaryoutputspec.pdf
clickhouse-client --query="drop function if exists updateIntraday"
clickhouse-client --query="create function updateIntraday as (price, size, conditions) -> price != 0 and size != 0 and not(hasAny(conditions, [10, 15, 16, 17, 18, 19, 21, 22, 23, 24, 29, 30, 33, 38, 40, 46, 52, 53]));"
clickhouse-client --query="drop function if exists updateDay"
clickhouse-client --query="create function updateDay as (price, size, conditions) -> not hasAny(conditions, [12]) and updateIntraday(price, size, conditions)"
#clickhouse-client --query="drop table if exists $table"

# TODO: why are there 4 FINRAs?
# https://api.polygon.io/v3/reference/exchanges?asset_class=stocks&apiKey=
#'FINRA NYSE TRF' = 4,
#'FINRA Nasdaq TRF Carteret' = 5,
#'FINRA Nasdaq TRF Chicago' = 6,
#participants="Enum8(
#	'NYSE American, LLC' = 1,
#	'Nasdaq OMX BX, Inc.' = 2,
#	'NYSE National, Inc.' = 3,
#	'FINRA Alternative Display Facility' = 4,
#	'Unlisted Trading Privileges' = 5,
#	'International Securities Exchange, LLC - Stocks' = 6,
#	'Cboe EDGA' = 7,
#	'Cboe EDGX' = 8,
#	'NYSE Chicago, Inc.' = 9,
#	'New York Stock Exchange' = 10,
#	'NYSE Arca, Inc.' = 11,
#	'Nasdaq' = 12,
#	'Consolidated Tape Association' = 13,
#	'Long-Term Stock Exchange' = 14,
#	'Investors Exchange' = 15,
#	'Cboe Stock Exchange' = 16,
#	'Nasdaq Philadelphia Exchange LLC' = 17,
#	'Cboe BYX' = 18,
#	'Cboe BZX' = 19,
#	'MIAX Pearl' = 20,
#	'Members Exchange' = 21,
#	'OTC Equity Security' = 62
#)"
#clickhouse-client --query="drop table if exists $table"
#clickhouse-client --query="create table $table
#	(
#		sequence_number  UInt64,
#		tape             Enum8('A-NYSE', 'B-ARCA', 'C-NASD'),
#		id               UInt64,
#		ticker           LowCardinality(String),
#		time             Datetime64(9, 'America/New_York'),
#		time_participant Nullable(Datetime64(9, 'America/New_York')),
#		time_trf         Nullable(Datetime64(9, 'America/New_York')),
#		price            Float64,
#		size             UInt32,
#		conditions       Array(UInt8),
#		correction       UInt8,
#		exchange         $participants,
#		trf              Nullable($participants),
#		update_intraday  Bool materialized updateIntraday(price, size, conditions),
#		update_day       Bool materialized updateDay(price, size, conditions)
#	)
#	Engine = MergeTree
#	partition by toYYYYMMDD(time)
#	order by (ticker, time);"

data_dir="/mnt/raid0/csv/data/trades"
# this takes a while, let's backfill most recent first.
files=$(ls $data_dir/* | sort -r)
IFS='
'
from=$(date -d '20080820' +%s)
for f in $files; do
	date=$(basename $f)
	date=${date:0:10}
	date=$(date -d $date +%s)
	if [ $date -gt $from ]; then
		continue
	fi
	echo $f
	zstdcat $f | clickhouse-client \
		--format_csv_delimiter='|' \
		--query="INSERT INTO $table FORMAT CSVWithNames"
done
