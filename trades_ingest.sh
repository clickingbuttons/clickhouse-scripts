#!/bin/bash
set -eou pipefail

data_dir="/mnt/raid0/csv/data/trades"
# this takes a while, let's backfill most recent first.
files=$(ls $data_dir/* | sort -r)
IFS='
'
from=$(date +%s)
for f in $files; do
	date=$(basename $f)
	date=${date:0:10}
	date=$(date -d $date +%s)
	if [ $date -gt $from ]; then
		continue
	fi
	echo $(date +"%Y-%m-%dT%H:%M:%S") $f
	zstdcat $f | clickhouse-client \
		--format_csv_delimiter='|' \
		--query="INSERT INTO us_equities.trades FORMAT CSVWithNames"
done
