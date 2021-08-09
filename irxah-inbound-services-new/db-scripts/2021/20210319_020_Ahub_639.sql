/* https://jira.ingenio-rx.com/browse/AHUB-639 */

--- insert new file_schedule_id for checking SLA of CVS consolidated file (file_schedule_id = 1010)

INSERT INTO ahub_dw.file_schedule
	(file_schedule_id,processing_type,client_file_id,frequency_type,frequency_count,total_files_per_day,grace_period,notification_sns,notification_timezone,file_category,file_description,is_active,is_running,aws_resource_name,last_poll_time,next_poll_time,environment_level,cron_expression,cron_description,created_by,updated_by,updated_timestamp,created_timestamp)
VALUES
	(1010, 'AHUB->CVS', 22, 'Daily', 1, 1, 60, 'irx_ahub_sla_notification', 'America/New_York', 'CVS Consolidated', 'CVS Consolidated File',
	true, false, NULL, '2021-03-05 08:00:00.000', '2021-03-05 08:30:00.000', 'SIT-SILO', 'utc: */30 * * * *',
	'Every 30 minutes check whether the eligible transactions loaded in past 30 min from current time are sent to CVS or not. Usually transactions of every inbound file are sent in nearest 30 min schedule. But this check is made providing additional 30 min grace period.',
	'AHUB ETL', 'AHUB ETL', '2021-03-05 00:00:00.000', '2021-03-05 00:00:00.000');

---update values for above created file_schedule_id

UPDATE ahub_dw.file_schedule
SET is_active = true,
last_poll_time = '3/19/2021 08:00:00.000',      --random time on 03/19 deployment (automatically the next run would adjust to proper time)
next_poll_time = '3/19/2021 08:30:00.000'       --random time on 03/19 deployment (automatically the next run would adjust to proper time)
WHERE file_schedule_id=1010;


-- RUN IN UAT

UPDATE ahub_dw.file_schedule SET environment_level ='PRE-PROD';

-- RUN IN PROD

UPDATE ahub_dw.file_schedule SET environment_level ='PROD';