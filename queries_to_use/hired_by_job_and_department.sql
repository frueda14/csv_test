-- Query to obtain Number of employees hired for each job and department in 2021 divided by quarter. The table must be ordered alphabetically by department and job.
with joined_emp_dept_job as
(
	select 
		he.id as employee_id,
		he.datetime as employee_hired_date,
		d.department as department_name,
		j.job as job_name
	from hired_employees as he
	inner join department as d
		on he.department_id = d.id

	inner join jobs as j
		on he.job_id = j.id
)
select *
from joined_emp_dept_job