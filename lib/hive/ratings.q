DROP TABLE ratings;
DROP TABLE ratings_orc;

CREATE EXTERNAL TABLE ratings(userId STRING, movieId STRING, rating STRING, time STRING)
ROW FORMAT SERDE 'org.apache.hadoop.hive.serde2.OpenCSVSerde'
WITH SERDEPROPERTIES ("separatorChar" = ",", "quoteChar" = "\"", "escapeChar" = "\\")
STORED AS SEQUENCEFILE
LOCATION '/user/flume/ratings';
