CREATE OR REPLACE EXTERNAL TABLE `abiding-pod-411817.ny_taxi.external_green_2022_tripdata`
OPTIONS ( format = 'parquet',
    uris = ['gs://zoomcamp-olga/green/green_tripdata_2022-*.parquet']);

CREATE TABLE ny_taxi.green_taxi_2022 AS
SELECT * FROM abiding-pod-411817.ny_taxi.external_green_2022_tripdata;

-- Question 1: What is count of records for the 2022 Green Taxi Data??
SELECT COUNT(VendorID) FROM abiding-pod-411817.ny_taxi.green_taxi_2022; --840402

--Question 2: Write a query to count the distinct number of PULocationIDs for the entire dataset on both the tables.
--What is the estimated amount of data that will be read when this query is executed on the External Table and the Table?

SELECT COUNT(DISTINCT(PULocationID)) FROM abiding-pod-411817.ny_taxi.external_green_2022_tripdata; --258

SELECT COUNT(DISTINCT(PULocationID)) FROM abiding-pod-411817.ny_taxi.green_taxi_2022; --258

--Question 3: How many records have a fare_amount of 0?
SELECT COUNT(fare_amount) FROM abiding-pod-411817.ny_taxi.green_taxi_2022
WHERE fare_amount = 0; --1622

-- Question 4
-- What is the best strategy to make an optimized table in Big Query if your query will always order the results by PUlocationID and filter based on lpep_pickup_datetime? (Create a new table with this strategy)
CREATE OR REPLACE TABLE abiding-pod-411817.ny_taxi.green_2022_tripdata_partitoned_clustered
PARTITION BY DATE(lpep_pickup_datetime)
CLUSTER BY PULocationID AS
SELECT * FROM abiding-pod-411817.ny_taxi.green_taxi_2022;

-- Question 5
-- Write a query to retrieve the distinct PULocationID between lpep_pickup_datetime 06/01/2022 and 06/30/2022 (inclusive)
--Use the materialized table you created earlier in your from clause and note the estimated bytes. Now change the table in the from clause to the partitioned table you created for question 4 and note the estimated bytes processed. What are these values?

SELECT DISTINCT(PULocationID)
FROM abiding-pod-411817.ny_taxi.green_2022_tripdata_partitoned_clustered
WHERE DATE(lpep_pickup_datetime) BETWEEN '2022-06-01' AND '2022-06-30';


SELECT DISTINCT(PULocationID)
FROM abiding-pod-411817.ny_taxi.green_taxi_2022
WHERE DATE(lpep_pickup_datetime) BETWEEN '2022-06-01' AND '2022-06-30';

-- Question 8
SELECT * FROM abiding-pod-411817.ny_taxi.green_taxi_2022;

SELECT * FROM abiding-pod-411817.ny_taxi.external_green_2022_tripdata;