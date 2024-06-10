with department_hired_cnt as
(
	select 
		d.id as department_id,
		d.department as department_name,
		dd.year_of_date,
		COUNT(*) as hired_rwn_cnt
	from hired_employees as he
	inner join department as d
		on he.department_id = d.id

	inner join dim_date as dd
		on CAST(he.datetime as date) = CAST(dd.event_id as date)

	group by 
		d.id, 
		d.department,
		dd.year_of_date
),

avg_year_2021 as
(
	select AVG(hired_rwn_cnt) as avg_hired_2021
	from department_hired_cnt
	where year_of_date = 2021
),

most_hired_by_department as
(
	select 
		department_id,
		department_name,
		hired_rwn_cnt
	from department_hired_cnt
	where hired_rwn_cnt >= (
							select 
								avg_hired_2021
							from avg_year_2021
							)
	and year_of_date > 2021
)
select *
from most_hired_by_department