#!/bin/bash
participants="'NYSE American, LLC',
	'Nasdaq OMX BX, Inc.',
	'NYSE National, Inc.',
	'FINRA NYSE TRF',
	'FINRA Nasdaq TRF Carteret',
	'FINRA Nasdaq TRF Chicago',
	'FINRA Alternative Display Facility',
	'Unlisted Trading Privileges',
	'International Securities Exchange, LLC - Stocks',
	'Cboe EDGA',
	'Cboe EDGX',
	'NYSE Chicago, Inc.',
	'New York Stock Exchange',
	'NYSE Arca, Inc.',
	'Nasdaq',
	'Consolidated Tape Association',
	'Long-Term Stock Exchange',
	'Investors Exchange',
	'Cboe Stock Exchange',
	'Nasdaq Philadelphia Exchange LLC',
	'Cboe BYX',
	'Cboe BZX',
	'MIAX Pearl',
	'Members Exchange',
	'OTC Equity Security'"

# page 17 https://www.nyse.com/publicdocs/nyse/data/Daily_TAQ_Client_Spec_v3.0.pdf
# page 16 https://utpplan.com/doc/utpbinaryoutputspec.pdf

# zack's interpretation
clickhouse-client --query="drop function if exists updateIntraday"
clickhouse-client --query="create function updateIntraday as (price, size, conditions) -> price != 0 and size != 0 and not(hasAny(conditions, [10, 15, 16, 17, 18, 19, 21, 22, 23, 24, 29, 30, 33, 38, 40, 46, 52, 53]));"
clickhouse-client --query="drop function if exists updateDay"
clickhouse-client --query="create function updateDay as (price, size, conditions) -> not hasAny(conditions, [12]) and updateIntraday(price, size, conditions)"

# page 43 https://utpplan.com/DOC/UtpBinaryOutputSpec.pdf
# page 64 https://www.ctaplan.com/publicdocs/ctaplan/CTS_Pillar_Output_Specification.pdf
clickhouse-client --query="drop function if exists neverAgg"
clickhouse-client --query="create function neverAgg as (conditions) -> not hasAny(conditions, [2, 7, 21, 37, 15, 20, 16, 29, 52, 53])"

# TODO: figure out state for only updating last if there is no last set
# hint: https://clickhouse.com/docs/en/sql-reference/functions/other-functions#runningaccumulate
clickhouse-client --query="drop function if exists ctaUpdateHighLow"
clickhouse-client --query="drop function if exists ctaUpdateLast"
clickhouse-client --query="create function ctaUpdateHighLow as (conditions) -> neverAgg(conditions)"
clickhouse-client --query="create function ctaUpdateLast as (conditions) -> neverAgg(conditions)"

clickhouse-client --query="drop function if exists utpUpdateHighLow"
clickhouse-client --query="drop function if exists utpUpdateLast"
clickhouse-client --query="create function utpUpdateHighLow as (conditions) -> neverAgg(conditions)"
clickhouse-client --query="create function utpUpdateLast as (conditions) -> neverAgg(conditions)"

clickhouse-client --query="drop function if exists updateHighLow"
clickhouse-client --query="create function updateHighLow as (conditions, tape) -> tape=1 ? ctaUpdateHighLow(conditions) : utpUpdateHighLow(conditions)"
clickhouse-client --query="drop function if exists updateLast"
clickhouse-client --query="create function updateLast as (conditions, tape) -> tape=1 ? ctaUpdateLast(conditions) : utpUpdateLast(conditions)"
clickhouse-client --query="drop function if exists updateVolume"
clickhouse-client --query="create function updateVolume as (conditions) -> not hasAny(conditions, [15, 16, 38])"

clickhouse-client --query="create database if not exists us_equities"
clickhouse-client --query="drop table if exists us_equities.trades"
clickhouse-client --query="create table us_equities.trades
	(
		sequence_number  UInt64,
		id               UInt64,
		ticker           LowCardinality(String),
		time             Datetime64(9, 'America/New_York'),
		time_participant Nullable(Datetime64(9, 'America/New_York')),
		time_trf         Nullable(Datetime64(9, 'America/New_York')),
		price            Float64,
		size             UInt32,
		conditions       Array(UInt8),
		correction       UInt8,
		exchange         Enum8($participants),
		trf              Nullable(Enum8($participants)),
		tape             Enum8('A-NYSE', 'B-ARCA', 'C-NASD'),
		update_intraday  Bool materialized updateIntraday(price, size, conditions),
		update_day       Bool materialized updateDay(price, size, conditions),
		update_high_low  Bool materialized updateHighLow(conditions, tape),
		update_last			 Bool materialized updateLast(conditions, tape),
		update_volume		 Bool materialized updateVolume(conditions)
	)
	Engine = MergeTree
	partition by toYYYYMMDD(time)
	order by (ticker, time);"
clickhouse-client --query="drop view if exists us_equities.agg1m"
clickhouse-client --query="create materialized view us_equities.agg1m
	engine = AggregatingMergeTree
	partition by toYYYYMM(time)
	order by (ticker, time)
	as select
		ticker,
		toStartOfMinute(time) as time,
		any(price) as open,
		max(price) as high,
		min(price) as low,
		anyLast(price) as close,
		sum(size) as volume,
		sum(price*size)/sum(size) as vwap,
		count() as n
	from us_equities.trades
	where update_intraday=true
	group by ticker, time"
clickhouse-client --query="drop view if exists us_equities.agg1d"
clickhouse-client --query="create materialized view us_equities.agg1d
	engine = AggregatingMergeTree
	partition by toYear(time)
	order by (ticker, time)
	as select
		ticker,
		toStartOfDay(time) as time,
		any(price) as open,
		max(price) as high,
		min(price) as low,
		anyLast(price) as close,
		sum(size) as volume,
		sum(price*size)/sum(size) as vwap,
		count() as n
	from us_equities.trades
	where update_day=true
	group by ticker, time"
clickhouse-client --query="drop view if exists us_equities.agg1m_cons"
clickhouse-client --query="create materialized view us_equities.agg1m_cons
	engine = AggregatingMergeTree
	partition by toYYYYMM(time)
	order by (ticker, time)
	as select * from (select
		ticker,
		toStartOfMinute(time) as time,
		anyIf(price, update_high_low) as open,
		maxIf(price, update_last) as high,
		minIf(price, update_last) as low,
		anyLastIf(price, update_high_low) as close,
		sumIf(size, update_volume) as volume,
		sum(price*size)/sum(size) as vwap,
		count() as n
	from us_equities.trades
	group by ticker, time)
	where open!=0 and high!=0 and low!=0 and close!=0"
clickhouse-client --query="drop view if exists us_equities.agg1d_cons"
clickhouse-client --query="create materialized view us_equities.agg1d_cons
	engine = AggregatingMergeTree
	partition by toYear(time)
	order by (ticker, time)
	as select * from (select
		ticker,
		toStartOfDay(time) as time,
		anyIf(price, update_high_low) as open,
		maxIf(price, update_last) as high,
		minIf(price, update_last) as low,
		anyLastIf(price, update_high_low) as close,
		sumIf(size, update_volume) as volume,
		sum(price*size)/sum(size) as vwap,
		count() as n
		from us_equities.trades
		group by ticker, time)
		where open!=0 and high!=0 and low!=0 and close!=0"

# can't curl google drive, download this yourself
# https://drive.google.com/u/0/uc?id=1OhzlSrBm5fbx0kr1LVqFnYuJKpVUWGGl&export=download
gzip -dc RAW_CSV_SIP_TRADES_2012_201205_trades-2012-05-25.csv.gz | \
	awk -v q='"' 'BEGIN {FS="|"; OFS=","} {if (NR!=1) {print $5,$6,$1,$2,$3,$4,$13,$11,q"["$12"]"q,$8,$9,$10,$14}}' | \
	clickhouse-client --query='INSERT INTO us_equities.trades FORMAT CSV'
