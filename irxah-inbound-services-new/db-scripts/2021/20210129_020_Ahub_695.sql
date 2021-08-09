---drop unused 'level1_error_reporting_complete' column from job table (ahub_695)

alter table ahub_dw.job
DROP COLUMN level1_error_reporting_complete CASCADE;
