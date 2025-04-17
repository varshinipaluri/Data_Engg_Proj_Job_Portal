-- Fails if any vacancy count is null or 0
SELECT *
FROM {{ ref('fact_job_postings_final') }}
WHERE vacancies_count IS NULL OR vacancies_count = 0
