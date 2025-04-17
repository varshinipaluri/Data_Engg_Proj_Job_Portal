-- Fails if any job has a deadline in the past
SELECT *
FROM {{ ref('fact_job_postings_final') }}
WHERE TRY_TO_DATE(application_deadline) < CURRENT_DATE
