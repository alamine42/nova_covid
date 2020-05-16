create or replace view covid_data_v as
select 
	report_dt,
	cast(zip_code as int),
	cast(total_cases as int),
	cast(total_tests as int)
from 
	covid_source_data
where
	total_cases <> 'Suppressed*'
	and zip_code <> 'Not Reported'
	and zip_code <> 'Out-of-State';