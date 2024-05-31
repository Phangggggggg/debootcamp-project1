select 
	t1.province_id as province_id
	, num_population as total_population
	, cast(extract(year from date_time) as int) as year 
	, cast(extract(month from date_time) as int) as month
	, avg(co)/num_population as avg_co
	, avg(no)/num_population as avg_no
	, avg(no2)/num_population as avg_no2
	, avg(o3)/num_population as avg_o3
	, avg(so2)/num_population  as avg_so2
	, avg(pm2_5)/num_population as avg_pm2_5
	, avg(nh3)/num_population as avg_nh3
	, avg(pm10)/num_population as avg_pm10
from 
	air_pollution  t1
LEFT JOIN 
	population  t2 on t1.province_id = t2.province_id  
where 
	t2.year = (select max(year) from population)
group by 1,2,3,4
order by 3,4,1
