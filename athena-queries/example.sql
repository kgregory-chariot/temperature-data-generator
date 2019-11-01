select  device, from_unixtime(timestamp/1000), temperature
from    temperature.example
limit   10

select  device,
        date_trunc('hour', from_unixtime(timestamp/1000)) as truncated_time,
        count(*) as num_readings, 
        avg(temperature) as average_temperature
from    temperature.example 
group by 1, 2
order by 1, 2
