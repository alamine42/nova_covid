create or replace view covid_by_county_v as
select
	report_dt,
	county,
	cases,
	tests,
	100 * cast(cases as float) / cast(tests as float) as infection_rate
from
	covid_by_county;