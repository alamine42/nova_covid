create or replace view covid_by_county_transpose_v as
select
	lou.report_dt,
	round(cast(lou.infection_rate as numeric), 3) as loudoun,
	round(cast(arl.infection_rate as numeric), 3) as arlington,
	round(cast(alx.infection_rate as numeric), 3) as alexandria,
	round(cast(ffx.infection_rate as numeric), 3) as fairfax,
	round(cast(pw.infection_rate as numeric), 3) as prince_william
from
	(select * from covid_by_county_v where county = 'LOUDOUN') lou
left join
	(select * from covid_by_county_v where county = 'ARLINGTON') arl
	on lou.report_dt = arl.report_dt
left join
	(select * from covid_by_county_v where county = 'ALEXANDRIA') alx
	on lou.report_dt = alx.report_dt
left join
	(select * from covid_by_county_v where county = 'FAIRFAX') ffx
	on lou.report_dt = ffx.report_dt
left join
	(select * from covid_by_county_v where county = 'PRINCE WILLIAM') pw
	on lou.report_dt = pw.report_dt;