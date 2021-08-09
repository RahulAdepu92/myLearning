/* https://jira.ingenio-rx.com/browse/AHUB-766 says Gila river shall have different SLA on MON-FRI (1am-5am MST) and SAT (1am-10am MST)*/

----insert new file_schedule_id for Gila (file_schedule_id = 500) to specific day- SATURDAY

INSERT INTO ahub_dw.file_schedule
	(file_schedule_id,processing_type,client_file_id,frequency_type,frequency_count,total_files_per_day,grace_period,notification_sns,notification_timezone,file_category,file_description,is_active,is_running,aws_resource_name,last_poll_time,next_poll_time,environment_level,cron_expression,cron_description,created_by,updated_by,updated_timestamp,created_timestamp)
VALUES
	(500, 'INBOUND->AHUB', 16, 'Daily', 1, 1, 540, 'irx_ahub_sla_notification', 'America/New_York', 'Gila River', 'Gila River Accumulation File',
	true, false, NULL, '2021-03-06 17:00:00.000', '2021-03-13 17:00:00.000', 'SIT', 'utc: 0 17 * * SAT',
	'Every Saturday at 12:00 PM EST, check whether Gila River transactions sent by Gila River has been received by AHUB or not. Expecting a file daily between 1am and 10am MST which is 3am to 12pm EST. Grace period is 540',
	'AHUB ETL', 'AHUB ETL', '2021-03-12 00:00:00.000', '2021-03-12 00:00:00.000');

---update values for above created file_schedule_id

UPDATE ahub_dw.file_schedule
SET is_active = true,
last_poll_time = '3/20/2021 17:00:00.000',      --previous saturday before 03/19 deployment
next_poll_time = '3/27/2021 17:00:00.000'       --next saturday before 03/19 deployment
WHERE file_schedule_id=500;

----update old file_schedule_id for Gila (file_schedule_id = 100) to specific days- MONDAY-FRIDAY

update ahub_dw.file_schedule
set
last_poll_time = '3/19/2021 12:00:00.000',  --previous friday before 03/19 deployment
next_poll_time = '3/22/2021 12:00:00.000',    --coming monday after 03/19 deployment
cron_expression = 'utc: 0 12 * * MON-FRI',
cron_description = 'Everyday(Monday-Friday) at 07:00 AM EST, check whether Gila River transactions sent by Gila River has been received by AHUB or not. Expecting a file daily between 1am and 5am MST which is 3am to 7am EST. Grace period is 240'
where
file_schedule_id = 100 --- Inbound Gila river with Mon-Fri schedule
and client_file_id = 16
and processing_type = 'INBOUND->AHUB';


-- RUN IN UAT

UPDATE ahub_dw.file_schedule SET environment_level ='PRE-PROD';

-- RUN IN PROD

UPDATE ahub_dw.file_schedule SET environment_level ='PROD';