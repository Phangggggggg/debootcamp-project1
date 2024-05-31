select 
	 air_quality_index
	, province_id  
	, province_name
	, lag(air_quality_index, 1) over (partition by province_id order by year, month asc) as air_quality_prev_month
	, month 
	, year 
from 
	(select 
		 t1.province_id 
		 , province_name
		, extract(month from date_time) as month 
		, extract(year from date_time) as year 
		, avg(air_quality_index) as air_quality_index 
	from 
		air_pollution t1
	left join 
		province t2 on t1.province_id = t2.province_id 
	group by 1,2,3,4
	) t1
order by 6,5,2
