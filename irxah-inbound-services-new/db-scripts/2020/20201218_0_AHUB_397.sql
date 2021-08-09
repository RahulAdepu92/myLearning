--- RUN ACROSS ALL ENVRIONMENT 
-- If drop table runs in to error , it is okay because you may not have the table yet..
DROP TABLE IF EXISTS ahub_dw.file_schedule;


-- Creates a table that supports the file schedule
CREATE TABLE IF NOT EXISTS ahub_dw.file_schedule
(
	file_schedule_id INTEGER NOT NULL  ENCODE az64
	,processing_type VARCHAR(50) NOT NULL  ENCODE lzo
	,client_file_id INTEGER NOT NULL  ENCODE az64
	,frequency_type VARCHAR(15) NOT NULL  ENCODE lzo
	,frequency_count INTEGER NOT NULL  ENCODE az64
	,total_files_per_day INTEGER NOT NULL  ENCODE az64
	,grace_period INTEGER NOT NULL  ENCODE az64
	,notification_sns VARCHAR(200) NOT NULL  ENCODE lzo
	,notification_timezone VARCHAR(50) NOT NULL  ENCODE lzo
	,file_category VARCHAR(50) NOT NULL  ENCODE lzo
	,file_description VARCHAR(255) NOT NULL  ENCODE lzo
	,is_active BOOLEAN NOT NULL DEFAULT false 
	,is_running BOOLEAN NOT NULL DEFAULT false 
	,aws_resource_name VARCHAR(255)   ENCODE lzo
	,last_poll_time TIMESTAMP WITHOUT TIME ZONE NOT NULL  ENCODE az64
	,next_poll_time TIMESTAMP WITHOUT TIME ZONE NOT NULL  ENCODE az64
	,environment_level VARCHAR(100) NOT NULL  ENCODE lzo
	,cron_expression VARCHAR(25) NOT NULL  ENCODE lzo
	,cron_description VARCHAR(1000) NOT NULL  ENCODE lzo
	,created_by VARCHAR(100) NOT NULL DEFAULT 'AHUB ETL'::character varying ENCODE lzo
	,updated_by VARCHAR(100)   ENCODE lzo
	,updated_timestamp TIMESTAMP WITHOUT TIME ZONE NOT NULL  ENCODE az64
	,created_timestamp TIMESTAMP WITHOUT TIME ZONE NOT NULL DEFAULT ('now'::character varying)::timestamp without time zone ENCODE az64
	,PRIMARY KEY (file_schedule_id)
)
DISTSTYLE KEY
DISTKEY (file_schedule_id)
;


/* Inserts Rows for existing SLAs*/


INSERT INTO ahub_dw.file_schedule
	(file_schedule_id,processing_type,client_file_id,frequency_type,frequency_count,total_files_per_day,grace_period,notification_sns,notification_timezone,file_category,file_description,is_active,is_running,aws_resource_name,last_poll_time,next_poll_time,environment_level,cron_expression,cron_description,created_by,updated_by,updated_timestamp,created_timestamp)
VALUES
	(10, 'INBOUND->AHUB', 1, 'Daily', 1, 1, 630, 'irx_ahub_sla_notification', 'America/New_York', 'Commercial', 'BCI Integrated Accumulation File', true, false, NULL, '2020-12-21 15:30:00.000', '2020-12-22 15:30:00.000', 'SIT [INBOUND->AHUB] FS_ID=1, CF_ID=1', 'local : 30 10 * * *', 'Everyday at 10:30 AM local , check whether  BCI commercial transaction sent by BCI has been received by AHUB or not.
Based on 630 minutes of grace period, the system checks whether the file was sent between 12:00 AM and 10:30 AM
', 'AHUB ETL', 'AHUB ETL (update_file_schedule_to_next_execution)', '2020-12-21 15:30:30.000', '2020-11-09 10:00:00.000'),
	(20, 'INBOUND->AHUB', 9, 'Daily', 1, 1, 630, 'irx_ahub_sla_notification', 'America/New_York', 'Government Programs', 'BCI GP Integrated File', true, false, NULL, '2020-12-21 15:30:00.000', '2020-12-22 15:30:00.000', 'SIT [INBOUND->AHUB] FS_ID=2, CF_ID=9', 'local : 30 10 * * *', 'Every day by 10:30 AM local time ( EST/EDT) AHUB is expecting a BCI GP Integrated File. At 10:30 AM , using 630 minutes grace period , AHUB checks whether we received a file between 12 AM and 10:30 AM ', 'AHUB ETL', 'AHUB ETL (update_file_schedule_to_next_execution)', '2020-12-21 15:30:31.000', '2020-03-26 14:36:35.000'),
	(30, 'INBOUND->AHUB', 7, 'Daily', 1, 1, 630, 'irx_ahub_sla_notification', 'America/New_York', 'BCI Error', 'BCI Error File', true, false, NULL, '2020-12-21 15:30:00.000', '2020-12-22 15:30:00.000', 'SIT [INBOUND->AHUB] FS_ID=7, CF_ID=7', 'local : 30 10 * * *', 'Every day by 10:30 AM local time ( EST/EDT) AHUB is expecting a BCI Error File. At 10:30 AM , using 630 minutes grace period , AHUB checks whether we received a file between 12 AM and 10:30 AM ', 'AHUB ETL', 'AHUB ETL (update_file_schedule_to_next_execution)', '2020-12-21 15:30:33.000', '2020-03-26 14:36:35.000'),
	(40, 'INBOUND->AHUB', 3, 'Daily', 1, 1, 30, 'irx_ahub_sla_notification', 'America/New_York', 'CVS', 'CVS Integrated Accumulation File', true, false, NULL, '2020-12-22 04:30:00.000', '2020-12-22 06:30:00.000', 'SIT [INBOUND->AHUB] FS_ID=8, CF_ID=3', 'utc: 30 0/2 * * *', 'Every 2 hour CVS is expected to send the file to AHUB such as by 2:30 AM, 4:30 AM.. Uses 30 minutes as a grace period, did we receive a file between 2 AM and 2:30 AM, 4 AM and 4:30 AM and so on..', 'AHUB ETL', 'AHUB ETL (update_file_schedule_to_next_execution)', '2020-12-22 04:30:40.000', '2020-03-26 14:36:35.000'),
	(50, 'INBOUND->AHUB', 6, 'Weekly', 1, 1, 720, 'irx_ahub_sla_notification', 'America/New_York', 'Reconciliation (Integrated) ', 'CVS Reconciliation (Integrated) File', true, false, NULL, '2020-12-20 16:00:00.000', '2020-12-27 16:00:00.000', 'SIT [INBOUND->AHUB] FS_ID=10, CF_ID=6', 'utc: 0 16 * * SUN', 'EVery Sunday by 12 PM AHUB expects a Integrated Reconciliation ( weekly) file from CVS.
The CVS integrated group does not follow daylight savings time. Hence, when covnerted to the local ( EST/EDT), you will see 1 hour off in the EST alerts.', 'AHUB ETL', 'AHUB ETL (update_file_schedule_to_next_execution)', '2020-12-20 16:00:30.000', '2020-03-26 14:36:35.000'),
	(60, 'INBOUND->AHUB', 10, 'Weekly', 1, 1, 720, 'irx_ahub_sla_notification', 'America/New_York', 'Reconciliation (Non-Integrated) ', 'CVS Reconciliation (Non-Integrated) File', false, false, NULL, '2020-12-13 17:00:00.000', '2020-12-20 17:00:00.000', 'SIT [INBOUND->AHUB] FS_ID=12, CF_ID=10', 'local : 0 12 * * SUN', 'Every Sunday by 12 PM , AHUB is expecting a  Weekly CVS Reconciliation Non Integrated file from CVS.
Plesae note that Weekly CVS Non Recon file follows daylight savings time.', 'AHUB ETL', 'AHUB ETL (update_file_schedule_to_next_execution)', '2020-12-06 23:30:30.000', '2020-03-26 14:36:35.000'),
	(70, 'INBOUND->AHUB', 11, 'Daily', 1, 1, 60, 'irx_ahub_sla_notification', 'America/New_York', 'Non-Integrated', 'Non-Integrated (Daily)  File', true, false, NULL, '2020-12-31 08:00:00.000', '2021-01-01 08:00:00.000', 'SIT [INBOUND->AHUB] FS_ID=13, CF_ID=11', 'local : 0 3 * * *', 'Everyday by 3 AM local, AHUB is expecting a CVS Non Integrated (Daily) file. Between 2 AM and 3 AM of local time', 'AHUB ETL', 'AHUB ETL (update_file_schedule_to_next_execution)', '2020-12-09 08:00:30.000', '2020-03-26 14:36:35.000'),
	(80, 'INBOUND->AHUB', 12, 'Bi-Weekly', 1, 1, 1560, 'irx_ahub_sla_notification', 'America/New_York', 'Aetna Melton', 'Aetna Melton Accumulation File', false, false, NULL, '2020-12-10 17:00:00.000', '2020-12-24 17:00:00.000', 'SIT [INBOUND->AHUB] FS_ID=17, CF_ID=12', 'local : 0 12 * * THU : 2', 'Every two weeks on Thursday at 12:00 PM local (if Thursday is holiday, send on previous business day), check whether Aetna transactions sent by Aetna Melton has been received by AHUB or not.', 'AHUB ETL', 'AHUB ETL', '2020-11-29 20:00:52.000', '2020-11-09 10:00:00.000'),
	(90, 'INBOUND->AHUB', 14, 'Daily', 1, 1, 150, 'irx_ahub_sla_notification', 'America/New_York', 'GPA Melton', 'GPA Melton Accumulation File', true, false, NULL, '2020-12-21 15:00:00.000', '2020-12-22 15:00:00.000', 'SIT [INBOUND->AHUB] FS_ID=19, CF_ID=14', 'local : 0 10 * * MON-FRI', 'Everyday (Monday-Friday) at 10:00 AM local, check whether Gpa transactions sent by GPA Melton has been received by AHUB or not. Every two weeks Thursday between 10:00 AM - 12:00 PM ET', 'AHUB ETL', 'AHUB ETL (update_file_schedule_to_next_execution)', '2020-12-21 15:00:31.000', '2020-11-09 10:00:00.000'),
	(100, 'INBOUND->AHUB', 16, 'Daily', 1, 1, 120, 'irx_ahub_sla_notification', 'America/New_York', 'Gila River', 'Gila River Accumulation File', false, false, NULL, '2020-12-21 12:00:00.000', '2020-12-22 12:00:00.000', 'SIT [INBOUND->AHUB] FS_ID=21, CF_ID=16', 'utc : 0 12 * * *', 'Everyday at 07:00 AM EST, check whether Gila River transactions sent by Gila River has been received by AHUB or not. Expecting a file daily between  between 3am and 5am MST.', 'AHUB ETL', 'AHUB ETL', '2020-11-29 20:00:52.000', '2020-11-09 10:00:00.000'),
	(110, 'INBOUND->AHUB', 18, 'Daily', 1, 1, 120, 'irx_ahub_sla_notification', 'America/New_York', 'The Health Plan Integrated', 'The Health Plan Integrated Accumulation File', false, false, NULL, '2020-12-16 11:00:00.000', '2020-12-17 11:00:00.000', 'SIT [INBOUND->AHUB] FS_ID=23, CF_ID=18', 'local : 0 6 * * *', 'Everyday at 06:00 AM local, check whether The Health Plan Integrated transactions sent by Gila River has been received by AHUB or not.', 'AHUB ETL', 'AHUB ETL', '2020-11-29 20:00:52.000', '2020-11-09 10:00:00.000'),
	(120, 'INBOUND->AHUB', 19, 'Daily', 1, 1, 120, 'irx_ahub_sla_notification', 'America/New_York', 'The Health Plan Error', 'The Health Plan Error Accumulation File', false, false, NULL, '2020-12-16 11:00:00.000', '2020-12-17 11:00:00.000', 'SIT [INBOUND->AHUB] FS_ID=25, CF_ID=19', 'local : 0 6 * * *', 'Everyday at 06:00 AM local, check whether The Health Plan Integrated transactions sent by Gila River has been received by AHUB or not.', 'AHUB ETL', 'AHUB ETL', '2020-11-29 20:00:52.000', '2020-11-09 10:00:00.000'),
	(200, 'AHUB->OUTBOUND', 4, 'Daily', 1, 1, 60, 'irx_ahub_sla_notification', 'America/New_York', 'Integrated File', 'BCI Integrated File', true, false, NULL, '2020-12-21 10:00:00.000', '2020-12-22 10:00:00.000', 'SIT [AHUB->OUTBOUND] FS_ID=9, CF_ID=4', 'local : 0 5 * * *', 'Everday at 05:00 AM check whether the outbound file to BCI  is generated and sent or not. Grace period is only 60 minutes. So basically checks whether a file was generated between 4 and 5 AM', 'AHUB ETL', 'AHUB ETL (update_file_schedule_to_next_execution)', '2020-12-21 10:00:30.000', '2020-03-26 14:36:35.000'),
	(210, 'AHUB->OUTBOUND', 5, 'Daily', 1, 1, 60, 'irx_ahub_sla_notification', 'America/New_York', 'DQ File', 'BCI Error File', true, true, NULL, '2020-12-18 10:00:00.000', '2020-12-19 10:00:00.000', 'SIT [AHUB->OUTBOUND] FS_ID=9, CF_ID=4', 'local : 0 5 * * *', 'Every day at 5 AM , check that whether AHUB has generated the outbound BCI Error file or not ? ', 'AHUB ETL', 'AHUB ETL (update_file_schedule_to_running)', '2020-12-18 10:00:33.000', '2020-03-26 14:36:35.000'),
	(220, 'AHUB->OUTBOUND', 15, 'Daily', 1, 1, 120, 'irx_ahub_sla_notification', 'America/New_York', 'GPA Melton', 'GPA Melton Accumulation File', true, false, NULL, '2020-12-21 12:00:00.000', '2020-12-22 12:00:00.000', 'SIT [AHUB->OUTBOUND] FS_ID=20, CF_ID=15', 'local : 0 7 * * MON-FRI', 'Everyday (Monday-Friday) at 07:00 AM local, check whether the outbound file to GPA is sent or not.', 'AHUB ETL', 'AHUB ETL (update_file_schedule_to_next_execution)', '2020-12-21 12:00:30.000', '2020-03-26 14:36:35.000'),
	(230, 'AHUB->OUTBOUND', 13, 'Bi-Weekly', 1, 1, 120, 'irx_ahub_sla_notification', 'America/New_York', 'Aetna Melton', 'Aetna Melton Accumulation File', true, false, NULL, '2020-12-12 12:00:00.000', '2020-12-25 12:00:00.000', 'SIT [AHUB->OUTBOUND] FS_ID=18, CF_ID=13', 'local : 0 7 * * MON : 2', 'Every two weeks on Monday at 07:00 AM local, check whether the outbound file to ATN is sent or not.', 'AHUB ETL', 'AHUB ETL', '2020-11-27 14:18:28.000', '2020-03-26 14:36:35.000'),
	(240, 'AHUB->OUTBOUND', 20, 'Daily', 1, 1, 120, 'irx_ahub_sla_notification', 'America/New_York', 'The Health Plan Integrated', 'The Health Plan Integrated Accumulation File', true, false, NULL, '2020-12-21 09:00:00.000', '2020-12-22 09:00:00.000', 'SIT [AHUB->OUTBOUND] FS_ID=24, CF_ID=20', 'local : 0 4 * * *', 'Everyday at 04:00 AM local, check whether the outbound file to The Health Plan Integrated is sent or not.', 'AHUB ETL', 'AHUB ETL (update_file_schedule_to_next_execution)', '2020-12-21 09:00:31.000', '2020-03-26 14:36:35.000'),
	(250, 'AHUB->OUTBOUND', 17, 'Daily', 1, 1, 120, 'irx_ahub_sla_notification', 'America/New_York', 'Gila River', 'Gila River Accumulation File', true, false, NULL, '2020-12-21 16:00:00.000', '2020-12-22 16:00:00.000', 'SIT [AHUB->OUTBOUND] FS_ID=22, CF_ID=17', 'utc : 0 11 * * *', 'Everyday at 06:00 AM, check whether the outbound file to Gila River is sent or not.', 'AHUB ETL', 'AHUB ETL (update_file_schedule_to_next_execution)', '2020-12-21 16:00:31.000', '2020-03-26 14:36:35.000'),
	(260, 'AHUB->OUTBOUND', 21, 'Daily', 1, 1, 120, 'irx_ahub_sla_notification', 'America/New_York', 'The Health Plan Error', 'The Health Plan Error Accumulation File', true, false, NULL, '2020-12-21 09:00:00.000', '2020-12-22 09:00:00.000', 'SIT [AHUB->OUTBOUND] FS_ID=26, CF_ID=21', 'local : 0 4 * * *', 'Everyday at 04:00 AM local, check whether the outbound file to The Health Plan Error is sent or not.', 'AHUB ETL', 'AHUB ETL (update_file_schedule_to_next_execution)', '2020-12-21 09:00:32.000', '2020-03-26 14:36:35.000'),
	(300, 'GLUE', 20, 'Daily', 1, 1, 0, 'irx_ahub_sla_notification', 'America/New_York', 'The Health Plan Integrated', 'The Health Plan Integrated Accumulation File', true, true, 'irxah_export_client_file', '2020-12-16 07:05:00.000', '2020-12-17 07:05:00.000', 'SIT [GLUE] FS_ID=30, CF_ID=20', 'local : 5 2 * * *', 'Everyday at 02:05 AM local, run a glue job that would send a The Health Plan Integrated file.', 'AHUB ETL', 'AHUB ETL (update_file_schedule_to_running)', '2020-12-22 04:30:41.000', '2020-11-09 10:00:00.000'),
	(310, 'GLUE', 21, 'Daily', 1, 1, 0, 'irx_ahub_sla_notification', 'America/New_York', 'The Health Plan Error', 'The Health Plan Error Accumulation File', true, true, 'irxah_export_client_file', '2020-12-16 07:05:00.000', '2020-12-17 07:05:00.000', 'SIT [GLUE] FS_ID=31, CF_ID=21', 'local : 5 2 * * *', 'Everyday at 02:05 AM local, run a glue job that would send a The Health Plan Error file.', 'AHUB ETL', 'AHUB ETL (update_file_schedule_to_running)', '2020-12-22 04:30:43.000', '2020-11-09 10:00:00.000'),
	(320, 'GLUE', 22, 'Daily', 1, 1, 0, 'irx_ahub_sla_notification', 'America/New_York', 'CVS Consolidated', 'CVS Consolidated File', true, true, 'irxah_export_client_file', '2020-12-18 10:00:00.000', '2020-12-19 10:00:00.000', 'SIT [GLUE] FS_ID=32, CF_ID=22', 'utc : */30 * * * *', 'Every 30 minutes , run a glue job that would generate the consolidated outbound CVS file if eligible transactions are found.', 'AHUB ETL', 'AHUB ETL (update_file_schedule_to_running)', '2020-12-22 04:30:44.000', '2020-11-09 10:00:00.000'),
	(330, 'GLUE', 17, 'Daily', 1, 1, 0, 'irx_ahub_sla_notification', 'America/New_York', 'Gila River', 'Gila River Accumulation File', true, false, 'irxah_export_client_file', '2020-12-17 09:00:00.000', '2020-12-22 09:00:00.000', 'SIT [GLUE] FS_ID=29, CF_ID=17', 'local : 0 4 * * *', 'Everyday at 04:00 AM local, run a glue job that would send a Gila River file.', 'AHUB ETL', 'AHUB ETL (update_file_schedule_to_next_execution)', '2020-12-22 04:32:09.000', '2020-11-09 10:00:00.000'),
	(340, 'GLUE', 15, 'Daily', 1, 1, 0, 'irx_ahub_sla_notification', 'America/New_York', 'GPA Melton', 'GPA Melton Accumulation File', true, false, 'irxah_export_client_file', '2020-12-31 15:30:00.000', '2021-01-01 15:30:00.000', 'SIT [GLUE] FS_ID=28, CF_ID=15', 'local : 0 5 * * *', 'Everyday at 05:00 AM local, run a glue job that would send a GPA Melton file.', 'AHUB ETL', 'AHUB ETL', '2020-11-29 20:00:52.000', '2020-11-09 10:00:00.000'),
	(350, 'GLUE', 13, 'Bi-Weekly', 1, 1, 0, 'irx_ahub_sla_notification', 'America/New_York', 'Aetna Melton', 'Aetna Melton Accumulation File', true, false, 'irxah_export_client_file', '2020-12-14 10:00:00.000', '2020-12-28 10:00:00.000', 'SIT [GLUE] FS_ID=27, CF_ID=13', 'local : 0 5 * * MON : 2', 'Every two weeks on Monday at 05:00 AM local, run a glue job that would send a Aetna Melton file.', 'AHUB ETL', 'AHUB ETL', '2020-11-29 20:00:52.000', '2020-11-09 10:00:00.000'),
	(360, 'GLUE', 5, 'Daily', 1, 1, 0, 'irx_ahub_sla_notification', 'America/New_York', 'Integrated DR File', 'BCI Error File', true, false, 'irxah_export_client_file', '2020-12-17 09:00:00.000', '2020-12-22 09:00:00.000', 'SIT [GLUE] FS_ID=16, CF_ID=5', 'local : 0 4 * * *', 'Everday at 04:00 AM run a glue job that would send a BCI Error file. Grace period is zero here.', 'AHUB ETL', 'AHUB ETL (update_file_schedule_to_next_execution)', '2020-12-22 04:32:00.000', '2020-03-26 14:36:35.000'),
	(370, 'GLUE', 4, 'Daily', 1, 1, 0, 'irx_ahub_sla_notification', 'America/New_York', 'Integrated DQ Filer', 'BCI Integrated File ', true, false, 'irxah_export_client_file', '2020-12-17 09:00:00.000', '2020-12-22 09:00:00.000', 'SIT [GLUE] FS_ID=15, CF_ID=4', 'local : 0 4 * * *', 'Everday at 04:00 AM run a glue job that would send a BCI DQ file. Grace period does not apply here.', 'AHUB ETL', 'AHUB ETL (update_file_schedule_to_next_execution)', '2020-12-22 04:32:09.000', '2020-03-26 14:36:35.000'),
	(1000, '12 FILES FROM CVS', 3, 'Daily', 1, 12, 1440, 'irx_ahub_sla_notification', 'America/New_York', '', 'CVS Integrated File(s)', true, false, NULL, '2020-12-21 06:00:00.000', '2020-12-22 06:00:00.000', 'SIT [12 FILES FROM CVS] FS_ID=14, CF_ID=3', 'utc: 0 6 * * *', 'Everday at 2 AM ( EDT) / 1 AM (EST)  AHUB checks that whether 12 files has been received from CVS in last 24 hours or not.
Please note that the Integrated CVS file does not follow daylight savings time.', 'AHUB ETL', 'AHUB ETL (update_file_schedule_to_next_execution)', '2020-12-22 04:30:50.000', '2020-03-26 14:36:35.000');




---******************************************************************************
--- FILE SCHEDULE ID =10 ( BCI Commercial File - Inbound)
---Everyday at 10:30 AM local , check whether  BCI commercial transaction sent by BCI has been received by AHUB or not. 
---  Based on 630 minutes of grace period, the system checks whether the file was sent between 12:00 AM and 10:30 AM
UPDATE ahub_dw.file_schedule
   SET last_poll_time = '2020-12-21 15:30:00.000',
       next_poll_time = '2020-12-22 15:30:00.000',
	   cron_expression ='local : 30 10 * * *',
	   grace_period = 630
WHERE file_schedule_id = 10;


--******************************************************************************
--- FILE SCHEDULE ID =20 ( Inbound BCI GP  File )
---Every day by 10:30 AM local time ( EST/EDT) AHUB is expecting a BCI GP Integrated File. At 10:30 AM , 
---using 630 minutes grace period , AHUB checks whether we received a file between 12 AM and 10:30 AM 
UPDATE ahub_dw.file_schedule
   SET last_poll_time = '2020-12-21 15:30:00.000',
       next_poll_time = '2020-12-22 15:30:00.000',
	   cron_expression ='local : 30 10 * * *',
	   grace_period= 630
WHERE file_schedule_id = 20;


--******************************************************************************
--- FILE SCHEDULE ID =30 (Inbound BCI Error File Inbound)
---Every day by 10:30 AM local time ( EST/EDT) AHUB is expecting a BCI GP Integrated File. At 10:30 AM , 
---using 630 minutes grace period , AHUB checks whether we received a file between 12 AM and 10:30 AM 
UPDATE ahub_dw.file_schedule
   SET last_poll_time = '2020-12-21 15:30:00.000',
       next_poll_time = '2020-12-22 15:30:00.000',
	   cron_expression ='local : 30 10 * * *',
	   grace_period = 630
WHERE file_schedule_id = 30;

--******************************************************************************
--- FILE SCHEDULE ID =40 ( Inbound CVS Integrated Accumulation File )
---Every 2 hour CVS is expected to send the file to AHUB such as by 2:30 AM, 4:30 AM.. 
--- Uses 30 minutes as a grace period, did we receive a file between 2 AM and 2:30 AM, 4 AM and 4:30 AM and so on.. 
UPDATE ahub_dw.file_schedule
   SET last_poll_time = '2020-12-21 02:30:00.000',
       next_poll_time = '2020-12-22 04:30:00.000',
	   cron_expression ='utc: 30 0/2 * * *',
	   grace_period = 30
WHERE file_schedule_id = 40;

--******************************************************************************
--- FILE SCHEDULE ID =50 ( Inbound Weekly Integrated Recon File from CVS)
--EVery Sunday by 12 PM AHUB expects a Integrated Reconciliation ( weekly) file from CVS.
--The CVS integrated group does not follow daylight savings time. Hence, when covnerted to the local ( EST/EDT), you will see 1 hour off in the EST alerts.
UPDATE ahub_dw.file_schedule
   SET last_poll_time = '2020-12-20 16:00:00.000',
       next_poll_time = '2020-12-27 16:00:00.000',
	   cron_expression ='utc: 0 16 * * SUN',
	   grace_period = 720
	   
WHERE file_schedule_id = 50;

--******************************************************************************
--- FILE SCHEDULE ID =60 ( Inbound CVS Reconciliation (Non-Integrated) File)
--Every Sunday by 12 PM , AHUB is expecting a  Weekly CVS Reconciliation Non Integrated file from CVS.
---Plesae note that Weekly CVS Non Recon file follows daylight savings time.---
UPDATE ahub_dw.file_schedule
   SET last_poll_time = '2020-12-20 17:00:00.000',
       next_poll_time = '2020-12-27 17:00:00.000',
	   cron_expression ='local : 0 12 * * SUN',
	   grace_period = 720
WHERE file_schedule_id = 60;

--******************************************************************************
--- FILE SCHEDULE ID =70 ( Inbound Non-Integrated (Daily)  File)
--Every day by 3 AM Local ,  AHUB is expecting a CVS Non Integrated (Daily) file.
UPDATE ahub_dw.file_schedule
   SET last_poll_time = '2020-12-21 08:00:00.000',
       next_poll_time = '2020-12-22 08:00:00.000',
	   cron_expression ='local : 0 3 * * *',
	   grace_period= 60
WHERE file_schedule_id = 70;

--******************************************************************************
--- FILE SCHEDULE ID =80 ( Inbound Aetna Melton Accumulation File File)
--Every two weeks on Thursday at 12:00 PM local (if Thursday is holiday, send on previous business day), 
-- check whether Aetna transactions sent by Aetna Melton has been received by AHUB or not.
UPDATE ahub_dw.file_schedule
   SET last_poll_time = '2020-12-10 17:00:00.000',
       next_poll_time = '2020-12-24 17:00:00.000',
	   cron_expression ='local : 0 12 * * THU : 2',
	   grace_period=  1560 -- having long grace period is intentional ( file can come on Wednesday too)
WHERE file_schedule_id = 80;

--******************************************************************************
--- FILE SCHEDULE ID =90 (Inbound GPA Melton Accumulation File)
--Everyday (Monday-Friday) at 10:00 AM local, check whether Gpa transactions sent by GPA Melton has been received by AHUB or not.

UPDATE ahub_dw.file_schedule
   SET last_poll_time = '2020-12-21 15:00:00.000',
       next_poll_time = '2020-12-22 15:00:00.000',
	   cron_expression ='local : 0 10 * * MON-FRI',
	   grace_period = 150

WHERE file_schedule_id = 90;

--******************************************************************************
--- FILE SCHEDULE ID =100 ( Inbound Gila River Accumulation File)
--Everyday at 07:00 AM, check whether Gila River transactions sent by Gila River has been received by AHUB or not.
-- Gila River does not follow day light savings time.
UPDATE ahub_dw.file_schedule
   SET last_poll_time = '2020-12-21 12:00:00.000',
       next_poll_time = '2020-12-22 12:00:00.000',
  	   cron_expression ='utc : 0 12 * * *',
	   grace_period = 120
WHERE file_schedule_id = 100;

--******************************************************************************
--- FILE SCHEDULE ID =110 ( Inbound The Health Plan Integrated Accumulation File)
--Everyday at 06:00 AM local, check whether The Health Plan Integrated transactions sent by Gila River has been received by AHUB or not.
-- Gila River does not follow day light savings time.
UPDATE ahub_dw.file_schedule
   SET last_poll_time = '2020-12-21 11:00:00.000',
       next_poll_time = '2020-12-22 11:00:00.000',
	   cron_expression ='local : 0 6 * * *',
	   grace_period = 120
WHERE file_schedule_id = 110;

--******************************************************************************
--- FILE SCHEDULE ID =120  ( Inbound The Health Plan Error Accumulation File)
--Everyday at 06:00 AM local, check whether The Health Plan Integrated transactions sent by Gila River has been received by AHUB or not.
-- Gila River does not follow day light savings time.
UPDATE ahub_dw.file_schedule
   SET last_poll_time = '2020-12-21 11:00:00.000',
       next_poll_time = '2020-12-22 11:00:00.000',
	   cron_expression ='local : 0 6 * * *',
	   grace_period = 120
WHERE file_schedule_id = 120;

--******************************************************************************
--- FILE SCHEDULE ID =200 ( Outbound BCI Integrated File)
--Everday at 05:00 AM check whether the outbound file to BCI  is generated and sent or not. Grace period is only 60 minutes.
-- So basically checks whether a file was generated between 4 and 5 AM
UPDATE ahub_dw.file_schedule
   SET last_poll_time = '2020-12-21 10:00:00.000',
       next_poll_time = '2020-12-22 10:00:00.000',
	   cron_expression ='local : 0 5 * * *',
	   grace_period = 60
	   WHERE file_schedule_id = 200;


--******************************************************************************
--- FILE SCHEDULE ID =210 ( Outbound  to BCI Error File)
--Every day at 5 AM local , check that whether AHUB has generated the outbound BCI Error file or not ?  Grace period is only 60 minutes.
-- So basically checks whether a file was generated between 4 and 5 AM
UPDATE ahub_dw.file_schedule
   SET last_poll_time = '2020-12-21 10:00:00.000',
       next_poll_time = '2020-12-22 10:00:00.000',
	   cron_expression ='local : 0 5 * * *',
	   grace_period = 60
	   WHERE file_schedule_id = 210;

--******************************************************************************
--- FILE SCHEDULE ID =220 ( Outbound to  GPA Melton Accumulation File)
--Attempt to generate a file at 5 AM ET, if no file is generated by  7 AM ET 
-- generate an alert


UPDATE ahub_dw.file_schedule
   SET last_poll_time = '2020-12-21 12:00:00.000',
       next_poll_time = '2020-12-22 12:00:00.000',
	   cron_expression ='local : 0 7 * * * ',
	   grace_period = 120
	   WHERE file_schedule_id = 220;

--******************************************************************************
--- FILE SCHEDULE ID =230 ( Outbound to Aetna Melton Accumulation File)
--Every two weeks on Monday at 07:00 AM local, check whether the outbound file to ATN is sent between
-- 5 AM and 7 AM or not

UPDATE ahub_dw.file_schedule
   SET last_poll_time = '2020-12-14 12:00:00.000',
       next_poll_time = '2020-12-28 12:00:00.000',
	   cron_expression ='local : 0 7 * * MON : 2',
	   grace_period = 120
	   WHERE file_schedule_id = 230;

--******************************************************************************
--- FILE SCHEDULE ID =240 ( Outbound The Health Plan Integrated Accumulation File)
--Everday at 5 AM check whether did you send a file between 2:30 AM and 4 AM


UPDATE ahub_dw.file_schedule
   SET last_poll_time = '2020-12-21 09:00:00.000',
       next_poll_time = '2020-12-22 09:00:00.000',
	   cron_expression ='local : 0 4 * * *',
	   grace_period = 90
	   WHERE file_schedule_id = 240;


--******************************************************************************
--- FILE SCHEDULE ID =250 ( Outbound Gila River Accumulation File)
-- If no file is generated by 6 AM Local ( 11:00 UTC) send alert
-- 5 AM and 7 AM or not

UPDATE ahub_dw.file_schedule
   SET last_poll_time = '2020-12-21 11:00:00.000',
       next_poll_time = '2020-12-22 11:00:00.000',
	   cron_expression ='utc : 0 11 * * *',
	   grace_period = 120
	   WHERE file_schedule_id = 250;


--******************************************************************************
--- FILE SCHEDULE ID =260 ( Outbound The Health Plan Error Accumulation File)
-- If no file is generated by 6 AM Local ( 11:00 UTC) send alert
-- 5 AM and 7 AM or not

UPDATE ahub_dw.file_schedule
    SET last_poll_time = '2020-12-21 09:00:00.000',
       next_poll_time = '2020-12-22 09:00:00.000',
	   cron_expression ='local : 0 4 * * *',
	   grace_period = 90
	   WHERE file_schedule_id = 260;

--******************************************************************************
--- FILE SCHEDULE ID =300 ( Run a Glue Job to Generate  The Health Plan Integrated Accumulation File)
-- Attempt to generate a file at 2:30  AM ET,


UPDATE ahub_dw.file_schedule
    SET last_poll_time = '2020-12-21 07:30:00.000',
       next_poll_time = '2020-12-22 07:30:00.000',
	   cron_expression ='local : 30 2 * * *',
	   grace_period = 0
	   WHERE file_schedule_id = 300;

--******************************************************************************
--- FILE SCHEDULE ID =310 ( Run a Glue Job to Generate  Generate The Health Plan Integrated Accumulation File)
-- Attempt to generate a file at 2:30 AM ET,


UPDATE ahub_dw.file_schedule
    SET last_poll_time = '2020-12-21 07:30:00.000',
       next_poll_time = '2020-12-22 07:30:00.000',
	   cron_expression ='local : 30 2 * * *',
	   grace_period = 0
	   WHERE file_schedule_id = 310;


--******************************************************************************
--- FILE SCHEDULE ID =320 ( Run a Glue Job to Generate  Generate the consolidated outbound CVS file)
-- Every 30 minutes , run a glue job that would generate the consolidated outbound CVS file if eligible transactions are found.


UPDATE ahub_dw.file_schedule
    SET last_poll_time = '2020-12-22 10:00:00.000',
       next_poll_time = '2020-12-22 10:30:00.000',
	   cron_expression ='utc : */30 * * * *',
	   grace_period = 0
	   WHERE file_schedule_id = 320;

--******************************************************************************
--- FILE SCHEDULE ID =330 ( Run a Glue Job to Generate  Generate The Gila River Accumulation File to send to Gila River)
-- Attempt to generate a file at 4 AM ET.


UPDATE ahub_dw.file_schedule
    SET last_poll_time = '2020-12-21 09:00:00.000',
       next_poll_time = '2020-12-22 09:00:00.000',
	   cron_expression ='local : 0 4 * * *',
	   grace_period = 0
	   WHERE file_schedule_id = 330;

--******************************************************************************
--- FILE SCHEDULE ID =340 ( Run a Glue Job to Generate  Generate The GPA Melton Accumulation File to GPA Melton)
-- Everyday at 05:00 AM local, run a glue job that would send a GPA Melton file.


UPDATE ahub_dw.file_schedule
    SET last_poll_time = '2020-12-21 10:00:00.000',
       next_poll_time = '2020-12-22 10:00:00.000',
	   cron_expression ='local : 0 5 * * *',
	   grace_period = 0
	   WHERE file_schedule_id = 340;

--******************************************************************************
--- FILE SCHEDULE ID =350 ( Run a Glue Job to Generate  Generate The Aetna Melton file to Aetna Melton)
-- Every two weeks on Monday at 05:00 AM local, run a glue job that would send a Aetna Melton file.


UPDATE ahub_dw.file_schedule
    SET last_poll_time = '2020-12-14 10:00:00.000',
       next_poll_time = '2020-12-28 10:00:00.000',
	   cron_expression ='local : 0 5 * * MON :2',
	   grace_period = 0
	   WHERE file_schedule_id = 350;

--******************************************************************************
--- FILE SCHEDULE ID =360  ( Run a Glue Job to Generate  Generate The BCI Error file to BCI)
-- Everday at 04:00 AM run a glue job that would send a BCI Error file. Grace period is zero here.


UPDATE ahub_dw.file_schedule
    SET last_poll_time = '2020-12-21 09:00:00.000',
       next_poll_time = '2020-12-22 09:00:00.000',
	   cron_expression ='local : 0 4 * * *',
	   grace_period = 0
	   WHERE file_schedule_id = 360;

--******************************************************************************
--- FILE SCHEDULE ID =370  ( Run a Glue Job to Generate   The BCI DQ File file to BCI)
-- Everday at 04:00 AM run a glue job that would send a BCI DQ  file to BCI. Grace period is zero here.


UPDATE ahub_dw.file_schedule
    SET last_poll_time = '2020-12-21 09:00:00.000',
       next_poll_time = '2020-12-22 09:00:00.000',
	   cron_expression ='local : 0 4 * * *',
	   grace_period = 0
	   WHERE file_schedule_id = 370;

--******************************************************************************
--- FILE SCHEDULE ID =1000  ( 12 File Requirement)
-- Everday at 02:00 AM check that did we receive 12 files from CVS ( in last 24 hours) using grace period of 1440.


UPDATE ahub_dw.file_schedule
    SET last_poll_time = '2020-12-21 06:00:00.000',
       next_poll_time = '2020-12-22 06:00:00.000',
	   cron_expression ='utc : 0 6 * * *',
	   grace_period = 1440
	   WHERE file_schedule_id = 1000;

commit;

-- Optional to Run...
-- This helps sets up the environment level - used in SIT
-- Run the outcome of this query to setup the enviornment level  ( used in debugging)
SELECT 'UPDATE ahub_dw.file_schedule SET environment_level = ''SIT [' || processing_type || '] FS_ID =' || file_schedule_id || ', CF_ID=' || client_file_id || ''' WHERE  FILE_SCHEDULE_ID = ' || FILE_SCHEDULE_ID || ';'
FROM ahub_dw.file_schedule
ORDER BY file_schedule_id;
--- End optiona to Run



-- RUN IN UAT

UPDATE ahub_dw.file_schedule SET environment_level ='UAT';

-- END RUN IN UAT

--BEGIN************12/25 PRODUCTION DEPLOYMENT SPECIFIC*********(ONLY IN PRODUCTION)**********************

-- 12/25 Deployment  - Sets up Daily in Production..

-- This drop column related statement has already been ran in the SIT and UAT , now we need to run it in PROD as well..

ALTER TABLE ahub_dw.client_files DROP COLUMN frequency_type
CASCADE;

ALTER TABLE ahub_dw.client_files DROP COLUMN frequency_count
CASCADE;

ALTER TABLE ahub_dw.client_files DROP COLUMN total_files_per_day
CASCADE;

ALTER TABLE ahub_dw.client_files DROP COLUMN grace_period
CASCADE;

ALTER TABLE ahub_dw.client_files DROP COLUMN poll_time_in_24_hour_format
CASCADE;

ALTER TABLE ahub_dw.client_files DROP COLUMN sla_notification_sns
CASCADE;

ALTER TABLE ahub_dw.client_files DROP COLUMN current_timezone_abbreviation
CASCADE;

-- Sets up the environment level

UPDATE ahub_dw.file_schedule SET environment_level ='PROD';


-- Ensures that last poll time is today ( 12/25/2020) and the next poll time is next day ( 12/26/2020)
-- If deployment day differs , please readjust this SQL
update ahub_dw.file_schedule set last_poll_time = ('2020-12-29' || ' '||  to_char(next_poll_time, 'HH24:MI:SS'))
::timestamp
, next_poll_time =
('2020-12-30' || ' '||  to_char
(next_poll_time, 'HH24:MI:SS'))::timestamp
where  frequency_type='Daily';

--******************************************************************************
--- FILE SCHEDULE ID =40 ( CVS Integrated Accumulation File)
---Every 2 hour CVS is expected to send the file to AHUB such as by 2:30 AM, 4:30 AM.. 
--- Uses 30 minutes as a grace period, did we receive a file between 2 AM and 2:30 AM, 4 AM and 4:30 AM and so on.. 
--- Review carefully below , as it would vary based on the time of the deployment.
UPDATE ahub_dw.file_schedule
   SET last_poll_time = '2020-12-30 02:30:00.000',
       next_poll_time = '2020-12-30 04:30:00.000',
	   cron_expression ='utc: 30 0/2 * * *',
	   grace_period = 30
WHERE file_schedule_id = 40;

-- Sets up weekly ..

--******************************************************************************
--- FILE SCHEDULE ID =50 ( Weekly Integrated Recon File from CVS)
--EVery Sunday by 12 PM AHUB expects a Integrated Reconciliation ( weekly) file from CVS.
--The CVS integrated group does not follow daylight savings time. Hence, when covnerted to the local ( EST/EDT), you will see 1 hour off in the EST alerts.
-- If deployment day differs the SQL below would differ , review carefully.
UPDATE ahub_dw.file_schedule
   SET last_poll_time = '2020-12-27 16:00:00.000',
       next_poll_time = '2021-01-03 16:00:00.000',
	   cron_expression ='utc: 0 16 * * SUN',
	   grace_period = 720
WHERE file_schedule_id = 50;

--******************************************************************************
--- FILE SCHEDULE ID =60 ( CVS Reconciliation (Non-Integrated) File)
--Every Sunday by 12 PM , AHUB is expecting a  Weekly CVS Reconciliation Non Integrated file from CVS.
---Plesae note that Weekly CVS Non Recon file follows daylight savings time.---
-- If deployment day differs the SQL below may differ , review carefully.
UPDATE ahub_dw.file_schedule
   SET last_poll_time = '2020-12-27 16:00:00.000',
       next_poll_time = '2021-01-03 16:00:00.000',
	   cron_expression ='local : 0 12 * * SUN',
	   grace_period = 720
WHERE file_schedule_id = 60;

--******************************************************************************
--- FILE SCHEDULE ID =320 ( Generate the consolidated outbound CVS file)
-- Every 30 minutes , run a glue job that would generate the consolidated outbound CVS file if eligible transactions are found.
-- Special setup for a scheduled job that runs at every 30 minutes ( is there anything to be sent to CVS ? )


UPDATE ahub_dw.file_schedule
    SET last_poll_time = '2020-12-30 04:00:00.000',
       next_poll_time = '2020-12-30 04:30:00.000',
	   cron_expression ='utc : */30 * * * *',
	   grace_period = 0
	   WHERE file_schedule_id = 320;

-- Disable all other schedules in production 

-- Before 1/1/2021 the client file ID listed below should remain inactive/disabled
UPDATE AHUB_DW.FILE_SCHEDULE SET IS_ACTIVE = False where client_file_id in (12,13,14,15,16,17,18,19,20,21);

-- Before 1/1/2021 the rest of the client file ID  should remain  active 
UPDATE AHUB_DW.FILE_SCHEDULE SET IS_ACTIVE = True where client_file_id not in (12,13,14,15,16,17,18,19,20,21);

-- Ensuring that BCI GP File related schedule remains disabled.
UPDATE AHUB_DW.FILE_SCHEDULE SET IS_ACTIVE = False where file_schedule_id in (20); -- Disable GP file schedule
 
--END**********12/25 PRODUCTION DEPLOYMENT SPECIFIC*********(ONLY IN PRODUCTION)**********************
 