-- delete from ahub_dw.client_files where client_file_id=22;

----insert generic CVS file into client_files table

INSERT INTO ahub_dw.client_files
(
 client_file_id,
 client_id,
 file_type,
 import_columns,
 data_error_threshold,
 process_duration_threshold,
 is_data_error_alert_active,
 file_description,
 long_description,
 outbound_successful_acknowledgement,
 archive_folder,
 error_folder,
 structure_specification_file,
 expected_line_length,
 s3_merge_output_path,
 outbound_transmission_type_column,
 input_sender_id,
 output_sender_id,
 input_receiver_id,
 output_receiver_id,
 file_processing_location,
 position_specification_file,
 process_name,
 process_description,
 reserved_variable,
 validate_file_structure,
 validate_file_detail_columns,
 generate_outbound_file,
 file_category,
 name_pattern,
 environment_level,
 s3_output_file_name_prefix,
 outbound_file_type_column,
 created_by,
 updated_by,
 updated_timestamp,
 file_timezone,
 created_timestamp,
 redshift_glue_iam_role_name,
 data_error_notification_sns,
 file_extraction_error_notification_sns,
 processing_notification_sns,
 outbound_file_generation_notification_sns,
 is_active,
 glue_job_name,
 zip_file,
 deliver_files_to_client,
 destination_bucket_cfx,
 destination_folder_cfx,
 carrier_id,
 active_date
)
VALUES
(
---below values are changed according to the incoming/outgoing file requirements
  22,
  2, -- client id
  'OUTBOUND',
  '', -- import columns
  0.01, -- data_error_threshold
  300, -- process_duration_threshold
  true, -- is_data_error_alert_active
  'CVS Generic Outbound File', -- file_description
  'Process: From database CVS Generic Outbound file is exported
Path : Database  to  CVS
File Pattern : ACCDLYINT_TST_INRXCVS_YYMMDDHHMMSS (UAT), ACCDLYINT_PRD_INRXCVS_YYMMDDHHMMSS ( PRD)
Frequency : Daily
Time : ',
  True, -- outbound_successful_acknowledgement
  'outbound/archive', -- archive_folder
  '', -- error_folder
  '', -- structure_specification_file
  0, -- expected_line_length
  'outbound/txtfiles', --s3_merge_output_path
  'T', -- outbound_transmission_type_column
  '',   ---setting blank input_sender_id as it is not required while exporting to CVS
  '00489INGENIORX', -- output_sender_id
  '',   ---setting blank input_receiver_id as it is not required while exporting to CVS
  '00990CAREMARK', -- output_receiver_id
  'outbound/temp', -- file_processing_location
  '', -- position_specification_file
  'Database to CVS Generic file', -- process_name
  'From database generate a generic CVS outbound file', -- process_description
  NULL, -- reserved_variable
  false, -- validate_file_structure
  false, -- validate_file_detail_columns
  false, -- generate_outbound_file
  '', -- file_category
---below values are initially inserted taking SIT as reference. They should be updated according to environment while running UPDATE statement in next step
  'ACCDLYINT_TST_INRXCVS_YYMMDDHHMMSS', -- name_pattern
  'SIT',  -- environment_level
  'ACCDLYINT_TST_INRXCVS_', -- s3_output_file_name_prefix
  'T', -- outbound_file_type_column
---below values remain constant for any incoming file. Need not overwrite them.
  'AHUB', -- created_by
  NULL, -- updated_by
  NULL, -- updated_timestamp
  'America/New_York', -- file_timezone
  '2020-12-04 01:00:00.000', -- created_timestamp
  'irx-accum-phi-redshift-glue', -- redshift_glue_iam_role_name
---below values may vary as per client. They must be changed if the distribution list (persons who would be notified in case of error, SLA failure ) is different.
  'irx_ahub_error_notification', -- data_error_notification_sns
  'irx_ahub_incoming_file_extraction_error_notification', -- file_extraction_error_notification_sns
  'irx_ahub_processing_notification', -- processing_notification_sns
  'irx_ahub_outbound_file_generation_notification', -- outbound_file_generation_notification_sns
---below values are for lambda functions and vary as per environment. For example, delivers_to_client is FALSE in lower envs and TRUE in higher envs.
  True, -- is_active
  'irxah_export_client_file', -- glue_job_name
  True, -- zip_file
  True, -- delivers_to_client
  'irx-nonprod-cvs-east-1-sftp-app-outbound', -- destination_bucket_cfx
  'CDH_Accumulations/', --destination_folder_cfx
  '', -- carrier_id
  '01/01/2021' -- active_date
);


------------- Step 2 : RUN below UPDATE statements as per environment (UAT/PROD) or requirement -----------

-- Run this in UAT  Only

UPDATE ahub_dw.client_files
   SET
-- Following values would change per environment
       environment_level = 'PRE-PROD',
       name_pattern = 'ACCDLYINT_TST_INRXCVS_YYMMDDHHMMSS',
       s3_output_file_name_prefix = 'ACCDLYINT_TST_INRXCVS_',
       outbound_file_type_column = 'T',
       destination_bucket_cfx = 'irx-prod-cvs-cte-east-1-sftp-app-outbound'
       WHERE client_file_id IN (22);

-- Run this in PROD only

UPDATE ahub_dw.client_files
   SET
-- Following values would change per environment
       environment_level = 'PROD',
       name_pattern = 'ACCDLYINT_PRD_INRXCVS_YYMMDDHHMMSS',
       s3_output_file_name_prefix = 'ACCDLYINT_PRD_INRXCVS_',
       outbound_file_type_column = 'P',
       destination_bucket_cfx = 'irx-prod-cvs-east-1-sftp-app-outbound'
       WHERE client_file_id IN (22);