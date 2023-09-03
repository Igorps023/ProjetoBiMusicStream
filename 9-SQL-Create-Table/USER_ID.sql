CREATE EXTERNAL TABLE IF NOT EXISTS `music_stream`.`user_id` (
  `user_id` string COMMENT 'unique_value'
) COMMENT "All data related to user information"
ROW FORMAT SERDE 'org.apache.hadoop.hive.ql.io.parquet.serde.ParquetHiveSerDe'
STORED AS INPUTFORMAT 'org.apache.hadoop.hive.ql.io.parquet.MapredParquetInputFormat' OUTPUTFORMAT 'org.apache.hadoop.hive.ql.io.parquet.MapredParquetOutputFormat'
LOCATION 's3://bkt-musicstream-bi/Files/RefinedZone/Listening_History_fact.parquet/'
TBLPROPERTIES ('classification' = 'parquet');