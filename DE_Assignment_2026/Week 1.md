Question 1: uv run python -m pip --version
Question 2: postgres:5433
Question 3: 
```SQL
SELECT 
    COUNT(*) 
FROM 
    yellow_tripdata 
WHERE 
    trip_distance <= 1.0;
```

Question 4
```SQL
SELECT 
    CAST(tpep_pickup_datetime AS DATE) AS pickup_day,
    MAX(trip_distance) AS max_dist
FROM 
    yellow_tripdata
WHERE 
    trip_distance < 100
GROUP BY 
    1
ORDER BY 
    max_dist DESC
LIMIT 1;
```

Question 5
```SQL
SELECT 
    z."Zone", 
    SUM(t.total_amount) AS total_amount_sum
FROM 
    yellow_tripdata t
JOIN 
    zones z ON t."PULocationID" = z."LocationID"
WHERE 
    CAST(t.tpep_pickup_datetime AS DATE) = '2025-11-18'
GROUP BY 
    z."Zone"
ORDER BY 
    total_amount_sum DESC
LIMIT 1;
```

Question 6
```SQL
SELECT 
    zdo."Zone" AS dropoff_zone,
    MAX(t.tip_amount) AS max_tip
FROM 
    yellow_tripdata t
JOIN 
    zones zpu ON t."PULocationID" = zpu."LocationID"
JOIN 
    zones zdo ON t."DOLocationID" = zdo."LocationID"
WHERE 
    zpu."Zone" = 'East Harlem North'
    AND t.tpep_pickup_datetime >= '2025-11-01'
    AND t.tpep_pickup_datetime < '2025-12-01'
GROUP BY 
    zdo."Zone"
ORDER BY 
    max_tip DESC
LIMIT 1;
```
