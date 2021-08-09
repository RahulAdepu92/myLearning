-- ONLY RUN ONCE!
-- sets the out_job_key to a non-null value
-- so that the jobs running after deployment
-- don't export all the records

UPDATE ahub_dw.accumulator_detail
SET out_job_key = 0
WHERE
  out_job_key is null
  -- excludes inbound records from CVS that have not been yet sent to BCI
  AND job_key not in (
      SELECT job_key
      FROM ahub_dw.job
      WHERE client_id = 2 -- CVS
      AND file_status = 'Not Available'
  )
;