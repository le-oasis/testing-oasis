# sql.py
query_trip_data = """
SELECT 
    formatDateTime(pickup_datetime, '%Y-%m') AS month,
    COUNTIf(toDayOfWeek(pickup_datetime) % 7 = 6) / countIf(toYYYYMM(pickup_datetime) = toYYYYMM(pickup_datetime)) AS sat_mean_trip_count,
    AVGIf(fare_amount, toDayOfWeek(pickup_datetime) % 7 = 6) AS sat_mean_fare_per_trip,
    AVGIf(toUnixTimestamp(dropoff_datetime) - toUnixTimestamp(pickup_datetime), toDayOfWeek(pickup_datetime) % 7 = 6) AS sat_mean_duration_per_trip,
    COUNTIf(toDayOfWeek(pickup_datetime) % 7 = 0) / countIf(toYYYYMM(pickup_datetime) = toYYYYMM(pickup_datetime)) AS sun_mean_trip_count,
    AVGIf(fare_amount, toDayOfWeek(pickup_datetime) % 7 = 0) AS sun_mean_fare_per_trip,
    AVGIf(toUnixTimestamp(dropoff_datetime) - toUnixTimestamp(pickup_datetime), toDayOfWeek(pickup_datetime) % 7 = 0) AS sun_mean_duration_per_trip
FROM tripdata
WHERE pickup_datetime BETWEEN '2014-01-01' AND '2016-12-31'
GROUP BY month
ORDER BY month
"""