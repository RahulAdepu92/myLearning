/* Ahub_463: introduces a new column 'process_multiple_file' in client_files table to veirfy if we can process more than one file from a client on same day */

--add the column
ALTER TABLE ahub_dw.client_files
ADD COLUMN process_multiple_file BOOLEAN default FALSE;

--update THP error file to allow multiple file processing

UPDATE ahub_dw.client_files
set process_multiple_file = TRUE
where client_file_id in (19);  --inbound THP error

-- update total_files_per_day for CVS inbound. Run it in all regions.

UPDATE ahub_dw.file_schedule
set total_files_per_day = 12
where client_file_id in (3) --inbound CVS
and processing_type= 'INBOUND->AHUB';



--------- ##############  DONOT RUN below in PROD ##############---------

-- Run this in DEV/SIT/UAT only.
--update the value to TRUE for INBOUND files in DEV/SIT/UAT since multiple files processing is a daily routine here.
UPDATE ahub_dw.client_files
set process_multiple_file = TRUE
where file_type = 'INBOUND';