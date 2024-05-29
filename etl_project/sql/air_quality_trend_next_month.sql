select 
	 air_quality_index
	, province_id  
	, lag(air_quality_index, 1) over (partition by province_id order by year, month asc) as air_quality_prev_month
	, month 
	, year 
from 
	(select 
		 province_id 
		, extract(month from date_time) as month 
		, extract(year from date_time) as year 
		, avg(air_quality_index) as air_quality_index 
	from 
		air_pollution 
	group by 1,2,3
	) t1