-- gold/reporting/jobseeker_opportunity_dashboard.sql
{{
  config(
    materialized='table',
    schema='gold_reporting',
    tags=['jobseeker_dashboard']
  )
}}

WITH 
-- Job type distribution
job_type_stats AS (
  SELECT
    job_type,
    job_count,
    percentage_of_total_jobs,
    RANK() OVER (ORDER BY job_count DESC) AS type_rank
  FROM {{ ref('kpi_job_offers_by_type') }}
),

-- Gender-specific opportunities
gender_role_stats AS (
  SELECT
    job_role,
    gender_requirement,
    job_count,
    ROUND(job_count * 100.0 / SUM(job_count) OVER (PARTITION BY job_role), 1) AS gender_dist_pct
  FROM {{ ref('kpi_job_role_distribution_by_gender') }}
),

-- Monthly hiring trends
trend_stats AS (
  SELECT
    DATE_TRUNC('month', month) AS trend_month,
    job_count,
    LAG(job_count, 1) OVER (ORDER BY month) AS prev_month_count,
    job_count - LAG(job_count, 1) OVER (ORDER BY month) AS month_change,
    ROUND((job_count - LAG(job_count, 1) OVER (ORDER BY month)) * 100.0 / 
          NULLIF(LAG(job_count, 1) OVER (ORDER BY month), 0), 1) AS mom_change_pct
  FROM {{ ref('kpi_job_vacancy_trends') }}
  WHERE month >= DATEADD('month', -6, CURRENT_DATE)
),

-- Location-based opportunities
location_stats AS (
  SELECT
    location,
    job_role,
    job_count,
    avg_salary,
    hiring_companies,
    RANK() OVER (PARTITION BY location ORDER BY job_count DESC) AS role_rank_in_location,
    RANK() OVER (PARTITION BY job_role ORDER BY avg_salary DESC) AS salary_rank_for_role
  FROM {{ ref('kpi_location_opportunities') }}
  WHERE job_count > (SELECT AVG(job_count) FROM {{ ref('kpi_location_opportunities') }})
),

-- Urgent openings
urgent_stats AS (
  SELECT
    job_role,
    location,
    salary_in_lpa,
    vacancies_count,
    days_remaining,
    CASE
      WHEN days_remaining <= 3 THEN 'Critical Priority'
      WHEN days_remaining <= 7 THEN 'High Priority'
      ELSE 'Medium Priority'
    END AS urgency_level
  FROM {{ ref('kpi_urgent_openings') }}
),

-- Skill demand analysis
skill_stats AS (
  SELECT
    skill,
    skill_demand,
    RANK() OVER (ORDER BY skill_demand DESC) AS demand_rank
  FROM {{ ref('kpi_skill_demand_analysis_jobseeker') }}
  WHERE skill_demand > (SELECT AVG(skill_demand) FROM {{ ref('kpi_skill_demand_analysis_jobseeker') }})
)

-- Main dashboard assembly
SELECT
  'Job Type Opportunities' AS insight_category,
  job_type AS dimension,
  job_count AS primary_metric,
  percentage_of_total_jobs AS secondary_metric,
  type_rank AS tertiary_metric,
  'Job Count' AS primary_label,
  'Percentage of Total' AS secondary_label,
  'Popularity Rank' AS tertiary_label
FROM job_type_stats

UNION ALL

SELECT
  'Role by Gender',
  job_role || ' - ' || gender_requirement,
  job_count,
  gender_dist_pct,
  NULL,
  'Job Count',
  'Gender Distribution %',
  NULL
FROM gender_role_stats
WHERE job_count > (SELECT AVG(job_count) FROM {{ ref('kpi_job_role_distribution_by_gender') }})

UNION ALL

SELECT
  'Hiring Trends',
  TO_CHAR(trend_month, 'YYYY-MM'),
  job_count,
  mom_change_pct,
  month_change,
  'Job Count',
  'MoM Change %',
  'Change from Previous'
FROM trend_stats

UNION ALL

SELECT
  'Location Opportunities',
  location || ' - ' || job_role,
  job_count,
  avg_salary,
  salary_rank_for_role,
  'Job Count',
  'Avg Salary (LPA)',
  'Salary Rank for Role'
FROM location_stats

UNION ALL

SELECT
  'Urgent Openings',
  job_role || ' in ' || location,
  vacancies_count,
  salary_in_lpa,
  days_remaining,
  'Vacancies',
  'Salary (LPA)',
  'Days Remaining'
FROM urgent_stats

UNION ALL

SELECT
  'Top Skills',
  skill,
  skill_demand,
  demand_rank,
  NULL,
  'Demand Count',
  'Demand Rank',
  NULL
FROM skill_stats
ORDER BY
  insight_category,
  CASE insight_category
    WHEN 'Job Type Opportunities' THEN -primary_metric
    WHEN 'Role by Gender' THEN -primary_metric
    WHEN 'Hiring Trends' THEN -trend_month
    WHEN 'Location Opportunities' THEN -primary_metric
    WHEN 'Urgent Openings' THEN -days_remaining
    WHEN 'Top Skills' THEN -primary_metric
  END