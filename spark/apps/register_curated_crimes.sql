-- Idempotent registration of the curated Parquet lake as an external Hive table
-- for the Spark Thrift Server (port 10000) / Power BI via JDBC/ODBC.
--
-- Layout on HDFS: /curated/year=YYYY/month=M/
--   (month partition values are written unpadded by Spark, e.g. month=1)
--
-- CREATE TABLE IF NOT EXISTS is safe to run on every monthly batch: it does not
-- touch existing data or metadata. MSCK REPAIR TABLE then registers any
-- partition directories that were written since the last run, so a brand-new
-- month becomes visible to SQL. This script is executed by the monthly DAG
-- (chicago_crime_pipeline_v2, task ensure_sql_table) and can also be run
-- manually against the Thrift Server:
--
--   docker exec spark-thrift-server beeline \
--       -u jdbc:hive2://localhost:10000 \
--       -f /opt/spark-apps/register_curated_crimes.sql
CREATE TABLE IF NOT EXISTS default.curated_crimes (
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
