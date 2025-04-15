-- gold/reporting/hr_strategic_hiring_dashboard.sql
{{
  config(
    materialized='table',
    schema='gold_reporting',
    tags=['hr_dashboard']
  )
}}

WITH 
-- Sector experience analysis
sector_stats AS (
  SELECT 
    industry_sector,
    avg_experience_required,
    median_experience,
    job_count,
    ROUND(job_count * 100.0 / SUM(job_count) OVER (), 1) AS sector_distribution_pct
  FROM {{ ref('kpi_experience_demand') }}
),

-- Gender diversity metrics
gender_stats AS (
  SELECT 
    gender_requirement,
    job_count,
    potential_hires,
    requirement_distribution AS percentage_of_total
  FROM {{ ref('kpi_gender_diversity') }}
),

-- Location-based demand
location_stats AS (
  SELECT 
    location,
    total_vacancies,
    avg_salary,
    hiring_sectors,
    RANK() OVER (ORDER BY total_vacancies DESC) AS demand_rank
  FROM {{ ref('kpi_location_demand') }}
),

-- Urgent positions (using our fixed KPI)
urgent_stats AS (
  SELECT * FROM {{ ref('kpi_urgent_positions') }}
),

-- Company hiring activity
company_stats AS (
  SELECT 
    company,
    total_job_postings,
    total_vacancies,
    ROUND(total_vacancies / NULLIF(total_job_postings, 0), 1) AS avg_vacancies_per_posting
  FROM {{ ref('kpi_job_postings_by_company') }}
)

-- Main dashboard assembly
SELECT 
  'Sector Analysis' AS category,
  industry_sector AS dimension,
  avg_experience_required AS metric_value_1,
  median_experience AS metric_value_2,
  job_count AS metric_value_3,
  sector_distribution_pct AS metric_value_4,
  'Years of Experience' AS metric_label_1,
  'Median Experience' AS metric_label_2,
  'Job Count' AS metric_label_3,
  'Sector Distribution %' AS metric_label_4
FROM sector_stats

UNION ALL

SELECT 
  'Gender Diversity',
  gender_requirement,
  job_count,
  potential_hires,
  percentage_of_total,
  NULL,
  'Job Postings',
  'Potential Hires',
  'Percentage of Total',
  NULL
FROM gender_stats

UNION ALL

SELECT 
  'Location Demand',
  location,
  total_vacancies,
  avg_salary,
  demand_rank,
  NULL,
  'Total Vacancies',
  'Avg Salary (LPA)',
  'Demand Rank',
  NULL
FROM location_stats

UNION ALL

SELECT 
  'Urgent Needs',
  'All Positions',
  jobs_closing_soon,
  urgent_vacancies,
  avg_days_remaining,
  NULL,
  'Jobs Closing Soon',
  'Urgent Vacancies',
  'Avg Days Remaining',
  NULL
FROM urgent_stats

UNION ALL

SELECT 
  'Company Activity',
  company,
  total_job_postings,
  total_vacancies,
  avg_vacancies_per_posting,
  NULL,
  'Total Postings',
  'Total Vacancies',
  'Avg Vacancies/Posting',
  NULL
FROM company_stats

ORDER BY 
  category,
  CASE category
    WHEN 'Sector Analysis' THEN -job_count
    WHEN 'Gender Diversity' THEN -job_count
    WHEN 'Location Demand' THEN -total_vacancies
    WHEN 'Urgent Needs' THEN -jobs_closing_soon
    WHEN 'Company Activity' THEN -total_job_postings
  END