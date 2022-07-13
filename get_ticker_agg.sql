SELECT
    toJSONString(toFloat64(volume)),
    toJSONString(round(vwap, 4)),
    toJSONString(open),
    toJSONString(close),
    toJSONString(high),
    toJSONString(low),
    toInt64(time) * 1000,
    n
FROM us_equities.agg1m_cons
WHERE ticker = 'A'
