DROP TABLE res_query1_1;
DROP TABLE res_query1_1_hbase;

CREATE TABLE res_query1_1(title STRING, avgrating STRING)
ROW FORMAT SERDE 'org.apache.hadoop.hive.serde2.OpenCSVSerde'
WITH SERDEPROPERTIES ("separatorChar" = "\t", "quoteChar" = "\"", "escapeChar" = "\\")
STORED AS TEXTFILE;

LOAD DATA INPATH '/moviedoop/test/output/query1_1/*' INTO TABLE res_query1_1;
