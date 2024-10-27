-- Create the database if it does not already exist
CREATE DATABASE IF NOT EXISTS nyse_db;
	
-- Switch to the newly created or existing nyse_db database
USE nyse_db;

-- Create the partitioned table nyse_daily if it does not already exist
CREATE TABLE IF NOT EXISTS nyse_daily (
    ticker STRING,
    tradedate INT,
    openprice FLOAT,
    highprice FLOAT,
    lowprice FLOAT,
    closeprice FLOAT,
    volume BIGINT
) PARTITIONED BY (trademonth INT)
STORED AS parquet;

-- Create the staging table nyse_stg if it does not already exist
CREATE TABLE IF NOT EXISTS nyse_stg (
    ticker STRING,
    tradedate INT,    
    openprice FLOAT,
    highprice FLOAT,
    lowprice FLOAT,
    closeprice FLOAT,
    volume BIGINT
) ROW FORMAT DELIMITED FIELDS TERMINATED BY ',';

-- Load data into the nyse_stg staging table of the data warehouse from a data lake
-- Note : both the data warehouse and the data lake are located in an HDFS environment
    LOAD DATA INPATH
    '/user/${username}/data/nyse'
    OVERWRITE INTO TABLE nyse_stg;

-- Set dynamic partition mode to nonstrict to allow dynamic partitioning
SET hive.exec.dynamic.partition.mode=nonstrict;

-- Populate data into the partitioned NYSE table (nyse_daily) from the staging table (nyse_stg)
-- Note : the two tables are located in the data warehouse
INSERT OVERWRITE TABLE nyse_daily PARTITION (trademonth)
SELECT ns.*, substr(tradedate, 1, 6) AS trademonth FROM nyse_stg AS ns;

-- Show the partitions in the nyse_daily table to verify data partitioning
SHOW PARTITIONS nyse_daily;

-- Retrieve the number of trades for each trade month, grouped and ordered by trade month
SELECT trademonth, count(*) AS trade_count
FROM nyse_daily
GROUP BY trademonth
ORDER BY trademonth;
