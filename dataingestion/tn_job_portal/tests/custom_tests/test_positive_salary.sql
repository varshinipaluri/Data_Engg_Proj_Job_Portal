-- Fails if any job has negative or zero salary
SELECT *
FROM {{ ref('fact_job_postings_final') }}
WHERE salary_in_lpa <= 0
