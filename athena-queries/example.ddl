CREATE EXTERNAL TABLE IF NOT EXISTS temperature.example (
  `device` string,
  `timestamp` bigint,
  `temperature` float 
)
ROW FORMAT SERDE 'org.openx.data.jsonserde.JsonSerDe'
WITH SERDEPROPERTIES (
  'serialization.format' = '1'
) LOCATION 's3://kgregory-generated-data/example/'
TBLPROPERTIES ('has_encrypted_data'='false');
