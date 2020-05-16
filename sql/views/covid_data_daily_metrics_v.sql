create or replace view covid_data_daily_metrics_v as
select 
	report_dt,
	county,
	total_cases,
	total_tests,
	100 * cast(total_cases as float) / cast(total_tests as float) as infection_rate
from
	covid_data_daily_v;
	