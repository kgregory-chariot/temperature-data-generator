select  src,
        device,
        count(*) as num_readings, 
        avg(temperature) as average_temperature
from    temperature.partitioned 
group by 1, 2
order by 1, 2

