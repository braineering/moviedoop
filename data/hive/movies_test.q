DROP TABLE movies_test;
DROP TABLE movies_test_orc;

CREATE TABLE movies_test(id STRING, title STRING, genres STRING)
ROW FORMAT SERDE 'org.apache.hadoop.hive.serde2.OpenCSVSerde'
WITH SERDEPROPERTIES ("separatorChar" = ",", "quoteChar" = "\"", "escapeChar" = "\\")
STORED AS TEXTFILE;

LOAD DATA LOCAL INPATH '/users/micheleporretta/Desktop/ml-20m/test/movies.csv' INTO TABLE movies_test;

CREATE TABLE movies_test_orc(id STRING, title STRING, genres STRING)
STORED AS ORC;

INSERT INTO TABLE movies_test_orc SELECT * FROM movies_test;