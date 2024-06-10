-- Query to obtain Number of employees hired for each job and department in 2021 divided by quarter. The table must be ordered alphabetically by department and job.
with joined_emp_dept_job as
(
	select 
		he.id as employee_id,
		he.datetime as employee_hired_date,
		d.department as department_name,
		j.job as job_name,
		dd.date_by_quarter
	from hired_employees as he
	inner join department as d
		on he.department_id = d.id

	inner join jobs as j
		on he.job_id = j.id

	inner join dim_date as dd
		on CAST(he.datetime as date) = CAST(dd.event_id as date)

	where dd.year_of_date = '2021'
),

data_grouped as 
(
	select 
		department_name,
		job_name,
		date_by_quarter,
		count(*) as hired_rwn_cnt
	from joined_emp_dept_job
	group by department_name,
			 job_name,
			 date_by_quarter
),

pivoted_dataset as
(
	select 
		department_name,
		job_name,

		case 
			when date_by_quarter = 'Q1' then
				MAX(hired_rwn_cnt)
		end as quarter_1,

		case
			when date_by_quarter = 'Q2' then
				MAX(hired_rwn_cnt)
		end as quarter_2,
			
		case	
			when date_by_quarter = 'Q3' then
				MAX(hired_rwn_cnt)
		end as quarter_3,
		
		case
			when date_by_quarter = 'Q4' then
				MAX(hired_rwn_cnt)
		end as quarter_4
	from  data_grouped
	group by department_name, job_name
)
select *
from pivoted_dataset