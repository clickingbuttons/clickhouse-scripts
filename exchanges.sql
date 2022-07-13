CREATE dictionary us_equities.exchanges
(
	id Int32,
	acronym String,
	asset_class String,
	locale String,
	mic String,
	name String,
	operating_mic String,
	participant_id String,
	type String,
	url String
)
primary key id
source(file(path '/var/lib/clickhouse/user_files/exchanges.csv' format 'CSVWithNames'))
layout(flat(initial_array_size 100 max_array_size 10000))
lifetime(300);

curl https://api.polygon.io/v3/reference/exchanges\?asset_class\=stocks\&apiKey\=$POLY_API_KEY\&limit\=1000 | jq -r '.results | (.[0] | keys_unsorted) as $keys | $keys, map([.[ $keys[] ]])[] | @csv' | sudo tee /var/lib/clickhouse/user_files/exchanges.csv
