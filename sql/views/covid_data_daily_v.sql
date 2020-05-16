create or replace view covid_data_daily_v as
select
	report_dt,
	county,
	sum(total_cases) as total_cases,
	sum(total_tests) as total_tests
from
	covid_data_mapped_to_county_v
where
	county in ('LOUDOUN', 'PRINCE WILLIAM', 'FAIRFAX', 'ARLINGTON', 'ALEXANDRIA')
group by
	report_dt, county
union
select * 
from 
	covid_by_county 
where 
	report_dt < '20200516'
order by
	report_dt;