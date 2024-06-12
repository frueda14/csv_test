-- List of ids, name and number of employees hired of each department that hired more employees than the mean of employees hired in 2021 for all the departments, ordered by the number of employees hired (descending).
-- drop view reports.most_hired_employees_by_department;
create view reports.most_hired_employees_by_department as
with department_hired_cnt as
(
	select 
		d.id as department_id,
		d.department as department_name,
		dd.year_number,
		COUNT(*) as hired_rwn_cnt
	from csv_files.hired_employees as he
	inner join csv_files.departments as d
		on he.department_id = d.id

	inner join common.dim_date as dd
		on CAST(he.hire_datetime as date) = CAST(dd.date_day as date)

	group by 
		d.id, 
		d.department,
		dd.year_number
),

avg_year_2021 as
(
	select AVG(hired_rwn_cnt) as avg_hired_2021
	from department_hired_cnt
	where year_number = 2021
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
	and year_number >= 2021
)
select *
from most_hired_by_department
order by hired_rwn_cnt desc