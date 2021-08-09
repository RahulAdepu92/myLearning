/* Ahub_623: rename job.file_name to job.inbound_file_name */

alter table ahub_dw.job
rename column file_name to inbound_file_name;
