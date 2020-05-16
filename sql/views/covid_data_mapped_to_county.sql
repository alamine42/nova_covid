create or replace view covid_data_mapped_to_county_v as
select
	cov.report_dt,
	zip.county,
	cov.total_cases,
	cov.total_tests
from
	covid_data_v cov
	left join
	zip_codes_by_county zip
	on cov.zip_code = zip.zip_code;