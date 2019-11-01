create table temperature.avro
with (
  external_location = 's3://kgregory-generated-data/avro'
  )
as select device, from_unixtime(timestamp/1000) as timestamp, temperature
from temperature.readings
