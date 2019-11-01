CREATE EXTERNAL TABLE IF NOT EXISTS temperature.partitioned (
  `device` string,
  `timestamp` bigint,
  `temperature` float 
)
PARTITIONED BY (src string)
ROW FORMAT SERDE 'org.openx.data.jsonserde.JsonSerDe'
WITH SERDEPROPERTIES (
  'serialization.format' = '1'
)
LOCATION 's3://kgregory-generated-data/partitioned/'


MSCK REPAIR TABLE temperature.partitioned
