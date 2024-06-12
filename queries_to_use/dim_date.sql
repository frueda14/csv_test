WITH base_dates AS (
  WITH date_spine AS (
    WITH rawdata AS (
      WITH p AS (
        SELECT 0 AS generated_number
        UNION ALL
        SELECT 1
      ),
      unioned AS (
        SELECT 
          p0.generated_number * power(2, 0) + 
          p1.generated_number * power(2, 1) + 
          p2.generated_number * power(2, 2) + 
          p3.generated_number * power(2, 3) + 
          p4.generated_number * power(2, 4) + 
          p5.generated_number * power(2, 5) + 
          p6.generated_number * power(2, 6) + 
          p7.generated_number * power(2, 7) + 
          p8.generated_number * power(2, 8) + 
          p9.generated_number * power(2, 9) + 
          p10.generated_number * power(2, 10) + 
          p11.generated_number * power(2, 11) + 
          p12.generated_number * power(2, 12) + 1 AS generated_number
        FROM p AS p0
        CROSS JOIN p AS p1
        CROSS JOIN p AS p2
        CROSS JOIN p AS p3
        CROSS JOIN p AS p4
        CROSS JOIN p AS p5
        CROSS JOIN p AS p6
        CROSS JOIN p AS p7
        CROSS JOIN p AS p8
        CROSS JOIN p AS p9
        CROSS JOIN p AS p10
        CROSS JOIN p AS p11
        CROSS JOIN p AS p12
      )
      SELECT * FROM unioned WHERE generated_number <= 7305 ORDER BY generated_number
    ),
    all_periods AS (
      SELECT
        (DATE '2012-01-01' + INTERVAL '1 day' * (row_number() OVER (ORDER BY 1) - 1))::timestamp AS date_day
      FROM rawdata
    ),
    filtered AS (
      SELECT * FROM all_periods WHERE date_day <= '2032-01-01'::timestamp
    )
    SELECT * FROM filtered
  )
  SELECT d.date_day::timestamp AS date_day FROM date_spine d
),
dates_with_prior_year_dates AS (
  SELECT
    d.date_day::date AS date_day,
    (d.date_day - INTERVAL '1 year')::date AS prior_year_date_day,
    (d.date_day - INTERVAL '364 days')::date AS prior_year_over_year_date_day
  FROM base_dates d
)
SELECT
  d.date_day,
  (d.date_day - INTERVAL '1 day')::date AS prior_date_day,
  (d.date_day + INTERVAL '1 day')::date AS next_date_day,
  d.prior_year_date_day,
  d.prior_year_over_year_date_day,
  CASE WHEN extract(dow FROM d.date_day) = 0 THEN 7 ELSE extract(dow FROM d.date_day) END AS day_of_week,
  extract(isodow FROM d.date_day) AS day_of_week_iso,
  CASE to_char(d.date_day, 'Dy')
    WHEN 'Mon' THEN 'Monday'
    WHEN 'Tue' THEN 'Tuesday'
    WHEN 'Wed' THEN 'Wednesday'
    WHEN 'Thu' THEN 'Thursday'
    WHEN 'Fri' THEN 'Friday'
    WHEN 'Sat' THEN 'Saturday'
    WHEN 'Sun' THEN 'Sunday'
  END AS day_of_week_name,
  to_char(d.date_day, 'Dy') AS day_of_week_name_short,
  extract(day FROM d.date_day) AS day_of_month,
  extract(doy FROM d.date_day) AS day_of_year,
  (d.date_day - INTERVAL '1 day' * (CASE WHEN extract(dow FROM d.date_day) = 0 THEN 7 ELSE extract(dow FROM d.date_day) END - 1))::date AS week_start_date,
  ((d.date_day - INTERVAL '1 day' * (CASE WHEN extract(dow FROM d.date_day) = 0 THEN 7 ELSE extract(dow FROM d.date_day) END - 1)) + INTERVAL '6 days')::date AS week_end_date,
  ((d.prior_year_over_year_date_day - INTERVAL '1 day' * (CASE WHEN extract(dow FROM d.prior_year_over_year_date_day) = 0 THEN 7 ELSE extract(dow FROM d.prior_year_over_year_date_day) END - 1))::date) AS prior_year_week_start_date,
  (((d.prior_year_over_year_date_day - INTERVAL '1 day' * (CASE WHEN extract(dow FROM d.prior_year_over_year_date_day) = 0 THEN 7 ELSE extract(dow FROM d.prior_year_over_year_date_day) END - 1))::date) + INTERVAL '6 days')::date AS prior_year_week_end_date,
  extract(week FROM d.date_day)::int AS week_of_year,
  date_trunc('week', d.date_day)::date AS iso_week_start_date,
  (date_trunc('week', d.date_day)::date + INTERVAL '6 days')::date AS iso_week_end_date,
  date_trunc('week', d.prior_year_over_year_date_day)::date AS prior_year_iso_week_start_date,
  (date_trunc('week', d.prior_year_over_year_date_day)::date + INTERVAL '6 days')::date AS prior_year_iso_week_end_date,
  extract(week FROM d.date_day)::int AS iso_week_of_year,
  extract(week FROM d.prior_year_over_year_date_day)::int AS prior_year_week_of_year,
  extract(week FROM d.prior_year_over_year_date_day)::int AS prior_year_iso_week_of_year,
  extract(month FROM d.date_day)::int AS month_of_year,
  to_char(d.date_day, 'Month') AS month_name,
  to_char(d.date_day, 'Mon') AS month_name_short,
  date_trunc('month', d.date_day)::date AS month_start_date,
  (date_trunc('month', d.date_day)::date + INTERVAL '1 month' - INTERVAL '1 day')::date AS month_end_date,
  date_trunc('month', d.prior_year_date_day)::date AS prior_year_month_start_date,
  (date_trunc('month', d.prior_year_date_day)::date + INTERVAL '1 month' - INTERVAL '1 day')::date AS prior_year_month_end_date,
  extract(quarter FROM d.date_day)::int AS quarter_of_year,
  date_trunc('quarter', d.date_day)::date AS quarter_start_date,
  (date_trunc('quarter', d.date_day)::date + INTERVAL '3 months' - INTERVAL '1 day')::date AS quarter_end_date,
  extract(year FROM d.date_day)::int AS year_number,
  date_trunc('year', d.date_day)::date AS year_start_date,
  (date_trunc('year', d.date_day)::date + INTERVAL '1 year' - INTERVAL '1 day')::date AS year_end_date
FROM dates_with_prior_year_dates d
ORDER BY 1;
