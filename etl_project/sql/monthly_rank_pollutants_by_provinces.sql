
select 
    province_id
    , province_name
    , dense_rank() OVER (PARTITION BY year, month ORDER BY total_co DESC) AS rank_co
    , total_co
    , dense_rank() OVER (PARTITION BY  year, month ORDER BY total_no DESC) AS rank_no
    , total_no
    , dense_rank() OVER (PARTITION BY year, month ORDER BY total_no2 DESC) AS rank_no2
    , total_no2
    , dense_rank() OVER (PARTITION BY year, month ORDER BY total_o3 DESC) AS rank_o3
    , total_o3
    , dense_rank() OVER (PARTITION BY  year, month ORDER BY total_so2 DESC) AS rank_so2
    , total_so2
    , dense_rank() OVER (PARTITION BY year, month ORDER BY total_pm2_5 DESC) AS rank_pm2_5
    , total_pm2_5
    , dense_rank() OVER (PARTITION BY  year, month ORDER BY total_nh3 DESC) AS rank_nh3
    , total_nh3
    , dense_rank() OVER (PARTITION BY  year, month ORDER BY total_pm10 DESC) AS rank_pm10
    , total_pm10
from 
	(select 
		 t1.province_id 
		 , province_name 
	    , cast(extract(year from date_time) as int) as year 
	    , cast(extract(month from date_time) as int) as month
		 , sum(co) as total_co
		 , sum(no) as total_no
		 , sum(no2) as total_no2
		 , sum(o3) as total_o3
		 , sum(so2) as total_so2
		 , sum(pm2_5) as total_pm2_5
		 , sum(nh3) as total_nh3
		 , sum(pm10) as total_pm10
	from 
		air_pollution t1
	left join 
		province t2 on t1.province_id = t2.province_id 
	group by 1,2,3,4
	) t1
order by 1
