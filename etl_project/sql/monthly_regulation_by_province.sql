select 
	t1.province_id as province_id
	, province_name as province_name
	, cast(extract(year from date_time) as int) as year 
	, cast(extract(month from date_time) as int) as month
	, avg(air_quality_index) as avg_air_quality_index
	, max(air_quality_index) as highest_air_quality_index
	, min(air_quality_index) as lowest_air_quality_index
from 
	air_pollution  t1
LEFT JOIN 
	province  t2 on t1.province_id = t2.province_id  
group by 1,2,3,4
order by 1
