DROP TABLE movies;
DROP TABLE movies_orc;

CREATE EXTERNAL TABLE movies(id STRING, title STRING, genres STRING)
ROW FORMAT SERDE 'org.apache.hadoop.hive.serde2.OpenCSVSerde'
WITH SERDEPROPERTIES ("separatorChar" = ",", "quoteChar" = "\"", "escapeChar" = "\\")
STORED AS SEQUENCEFILE
LOCATION '/user/flume/movies';


