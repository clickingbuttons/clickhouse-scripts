#!/bin/bash
set -eou pipefail

data_dir="/mnt/raid0/csv/data/tickers"
for f in $data_dir/*; do
	echo $f
	date=$(basename -s '.csv' $f)
	awk "{if (NR==1) {print \"time,\" \$0} else {print \"$date,\" \$0}}" $f | \
		clickhouse-client \
		--input_format_skip_unknown_fields=true \
		--format_csv_allow_single_quotes=0 \
		--date_time_input_format='best_effort' \
		--query="INSERT INTO us_equities.tickers FORMAT CSVWithNames"
done
