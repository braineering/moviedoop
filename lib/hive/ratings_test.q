DROP TABLE ratings_test;
DROP TABLE ratings_test_orc;

CREATE TABLE ratings_test(userId STRING, movieId STRING, rating STRING, time STRING)
ROW FORMAT SERDE 'org.apache.hadoop.hive.serde2.OpenCSVSerde'
WITH SERDEPROPERTIES ("separatorChar" = ",", "quoteChar" = "\"", "escapeChar" = "\\")
STORED AS TEXTFILE;

LOAD DATA LOCAL INPATH '/home/gmarciani/Workspace/moviedoop/data/test/ratings.csv' INTO TABLE ratings_test;

CREATE TABLE ratings_test_orc(userId STRING, movieId STRING, rating STRING, time STRING)
STORED AS ORC;

INSERT INTO TABLE ratings_test_orc SELECT * FROM ratings_test;
