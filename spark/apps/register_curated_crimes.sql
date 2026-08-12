-- Idempotent registration of the curated Parquet lake as an external Hive table
-- for the Spark Thrift Server (port 10000) / Power BI via JDBC/ODBC.
--
-- Layout on HDFS: /curated/year=YYYY/month=M/
--   (month partition values are written unpadded by Spark, e.g. month=1)
-- The DROP is metadata-only (external table), the data files are untouched.
DROP TABLE IF EXISTS default.curated_crimes;

CREATE TABLE default.curated_crimes (
  ID STRING,
  `Case Number` STRING,
  `Date` STRING,
  `Block` STRING,
  `IUCR` STRING,
  `Primary Type` STRING,
  `Description` STRING,
  `Location Description` STRING,
  `Arrest` BOOLEAN,
  `Domestic` BOOLEAN,
  `Beat` INT,
  `District` INT,
  `Ward` INT,
  `Community Area` INT,
  `FBI Code` STRING,
  `X Coordinate` INT,
  `Y Coordinate` INT,
  `Updated On` STRING,
  `Latitude` DOUBLE,
  `Longitude` DOUBLE,
  `Location` STRING
)
USING parquet
PARTITIONED BY (year INT, month INT)
LOCATION 'hdfs://namenode:9000/curated';

MSCK REPAIR TABLE default.curated_crimes;
