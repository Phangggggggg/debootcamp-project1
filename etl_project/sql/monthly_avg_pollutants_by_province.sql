select 
	t1.province_id as province_id
	, province_name as province_name
	, cast(extract(year from date_time) as int) as year 
	, cast(extract(month from date_time) as int) as month
	, avg(air_quality_index) as avg_air_quality_index
	, avg(co) as avg_co
	, avg(no) as avg_no
	, avg(no2) as avg_no2
	, avg(o3) as avg_o3
	, avg(so2) as avg_so2
	, avg(pm2_5) as avg_pm2_5
	, avg(nh3) as avg_nh3
	, avg(pm10) as avg_pm10
from 
	air_pollution  t1
LEFT JOIN 
	province  t2 on t1.province_id = t2.province_id  
group by 1,2,3,4
order by 3,4,1
