-- Query to obtain Number of employees hired for each job and department in 2021 divided by quarter. The table must be ordered alphabetically by department and job.
with joined_emp_dept_job as
(
	select 
		he.id as employee_id,
		he.hire_datetime as employee_hired_date,
		d.department as department_name,
		j.job as job_name,
		'Q' || dd.quarter_of_year as quarter_of_year
	from csv_files.hired_employees as he
	inner join csv_files.departments as d
		on he.department_id = d.id

	inner join csv_files.jobs as j
		on he.job_id = j.id

	inner join common.dim_date as dd
		on CAST(he.hire_datetime as date) = CAST(dd.date_day as date)

	where dd.year_number = 2021
),

data_grouped as 
(
	select 
		department_name,
		job_name,
		quarter_of_year,
		count(*) as hired_rwn_cnt
	from joined_emp_dept_job
	group by department_name,
			 job_name,
			 quarter_of_year
),

pivoted_dataset as
(
	select 
		department_name,
		job_name,

		sum(
				case 
					when quarter_of_year = 'Q1' then
						coalesce(hired_rwn_cnt,0)
				end 
			) as quarter_1,

		sum(
				case 
					when quarter_of_year = 'Q2' then
						coalesce(hired_rwn_cnt,0)
				end 
			) as quarter_2,
			
		sum(
				case 
					when quarter_of_year = 'Q3' then
						coalesce(hired_rwn_cnt,0)
				end 
			) as quarter_3,
		
		sum(
				case 
					when quarter_of_year = 'Q4' then
						coalesce(hired_rwn_cnt,0)
				end 
			) as quarter_4

	from  data_grouped
	group by department_name, job_name
),

final as
(
	select 
		department_name,
		job_name,

		coalesce(quarter_1,0) as quarter_1,
		coalesce(quarter_2,0) as quarter_2,
		coalesce(quarter_3,0) as quarter_3,
		coalesce(quarter_4,0) as quarter_4
	from pivoted_dataset
)
select *
from final