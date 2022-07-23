#!/bin/bash
set -eou pipefail

clickhouse-client --query="drop database if exists us_equities"
clickhouse-client --query="create database if not exists us_equities"
./tickers.sh
./trades.sh
./aggs.sh

echo "good to start using. now optimizing"
clickhouse-client --query="optimize table us_equities.tickers"
clickhouse-client --query="optimize table us_equities.trades"
