/* this whole script intends to introduce new Melton-Aetna file with
client_file_id = 12 (ATN → Database) and client_file_id = 13 (Database → ATN) */


------ ################## Insert client_file_id = 12  ( Aetna Melton --> AHUB ) ##################

------------- Step 1  -------------
-- Insert a new record in the ahub_dw.client_files
-- RUN this global INSERT statement which is set as per SIT in any environment SIT/UAT/PROD initially.
-- later you will update specific columns as per environment in Step 2

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
  12,
  3,
  'INBOUND',
  'processor_routing_identification,record_type,transmission_file_type,version_release_number,sender_identifier,receiver_identifier,submission_number,transaction_response_status,reject_code,record_length,reserved_1,transmission_date,transmission_time,date_of_service,service_provider_identifier_qualifier,service_provider_identifier,document_reference_identifier_qualifier,document_reference_identifier,transmission_identifier,benefit_type,in_network_indicator,formulary_status,accumulator_action_code,sender_reference_number,insurance_code,accumulator_balance_benefit_type,benefit_effective_date,benefit_termination_date,accumulator_change_source_code,transaction_identifier,transaction_identifier_cross_reference,adjustment_reason_code,accumulator_reference_time_stamp,reserved_2,cardholder_identifier,group_identifier,patient_first_name,middle_initial,patient_last_name,patient_relationship_code,date_of_birth,patient_gender_code,patient_state_province_address,cardholder_last_name,carrier_number,contract_number,client_pass_through,family_identifier_number,cardholder_identifier_alternate,group_identifier_alternate,patient_identifier,person_code,reserved_3,accumulator_balance_count,accumulator_specific_category_type,reserved_4,accumulator_1_balance_qualifier,accumulator_1_network_indicator,accumulator_1_applied_amount,accumulator_1_applied_amount_action_code,accumulator_1_benefit_period_amount,accumulator_1_benefit_period_amount_action_code,accumulator_1_remaining_balance,accumulator_1_remaining_balance_action_code,accumulator_2_balance_qualifier,accumulator_2_network_indicator,accumulator_2_applied_amount,accumulator_2_applied_amount_action_code,accumulator_2_benefit_period_amount,accumulator_2_benefit_period_amount_action_code,accumulator_2_remaining_balance,accumulator_2_remaining_balance_action_code,accumulator_3_balance_qualifier,accumulator_3_network_indicator,accumulator_3_applied_amount,accumulator_3_applied_amount_action_code,accumulator_3_benefit_period_amount,accumulator_3_benefit_period_amount_action_code,accumulator_3_remaining_balance,accumulator_3_remaining_balance_action_code,accumulator_4_balance_qualifier,accumulator_4_network_indicator,accumulator_4_applied_amount,accumulator_4_applied_amount_action_code,accumulator_4_benefit_period_amount,accumulator_4_benefit_period_amount_action_code,accumulator_4_remaining_balance,accumulator_4_remaining_balance_action_code,reserved_5,line_number,job_key',
  0.01,
  300,
  true,
  'Aetna Melton Accumulation File',
  'Process: Incoming file from Aetna Melton is stored in database
Path : Aetna Melton  to  Database
File Pattern : ACCDLYINT_TST_ATNINRX_YYMMDDHHMMSS (UAT), ACCDLYINT_PRD_ATNINRX_YYMMDDHHMMSS (PRD)
Frequency : Every 2 weeks (On Thursdays)
Time : 10:00 AM - 12:00 PM EST',
  True,
  'inbound/archive',
  'inbound/error',
  'inbound/specifications/aetna_melton_structure_specification.csv',
  1700,
  '',
  '',   ---for INBOUND outbound_transmission_type_column is not required
  '20600AETNA',
  '00489INGENIORX',
  '00489INGENIORX',
  '00990CAREMARK',
  'inbound/temp',
  'inbound/specifications/position_specification.txt',
  'Aetna Melton file to Database',
  'Validate inbound Aetna Melton file, split, load into redshift database',
  NULL,
  true,
  true,
  false,
  '',
---below values are initially inserted taking SIT as reference. They should be updated according to environment while running UPDATE statement in next step
  'ACCDLYINT_TST_ATNINRX_YYMMDDHHMMSS',
  'SIT',
  '',
  '',
---below values remain constant for any incoming file. Need not overwrite them.
  'AHUB',
  NULL,
  NULL,
  'America/New_York',
  '2020-12-04 01:00:00.000',
  'irx-accum-phi-redshift-glue',
---below values may vary as per client. They must be changed if the distribution list (persons who would be notified in case of error, SLA failure ) is different.
  'irx_ahub_error_notification',
  'irx_ahub_incoming_file_extraction_error_notification',
  'irx_ahub_processing_notification',
  'irx_ahub_outbound_file_generation_notification',
---below values are for lambda functions and vary as per environment. For example, delivers_to_client is FALSE in lower envs and TRUE in higher envs.
  True,
  'irxah_process_incoming_file',
  False,
  False,
  '',
  '',
  '8491',
  '01/01/2021'
);


------------- Step 2 -------------
-- RUN below UPDATE statements as per environment (UAT/PROD) for client_file_id = 12  ( Aetna Melton --> AHUB )
-- By this we are updating the selective columns that satisfy the environmental level requirements.
-- For example: name_pattern varies between UAT and PROD.

-- Run this in UAT  Only

UPDATE ahub_dw.client_files
   SET
-- Following values would change per environment
       environment_level = 'PRE-PROD',
       name_pattern = 'ACCDLYINT_TST_ATNINRX_YYMMDDHHMMSS',
       deliver_files_to_client = False
       WHERE client_file_id IN (12);

-- Run this in PROD only

UPDATE ahub_dw.client_files
   SET
-- Following values would change per environment
       environment_level = 'PROD',
       name_pattern = 'ACCDLYINT_PRD_ATNINRX_YYMMDDHHMMSS',
       deliver_files_to_client = True
       WHERE client_file_id IN (12);


------------- Step 3  -------------
-- Insert file columns records in the ahub_dw.file_columns for client_file_id = 12  ( Aetna Melton --> AHUB )
-- Here we are copying the columnar metadata of client_file_id=1 to the client_file_id=12
-- The reason we did the + 12000 in above SQL because we wanted to make sure that file_column_id is unique and it is identifiable by the first number itself that it belongs to the client_file_id = 12

INSERT INTO ahub_dw.file_columns
(
  file_column_id,
  client_file_id,
  file_column_name,
  column_position,
  table_column_name,
  is_accumulator,
  created_by
)
(SELECT (file_column_id +120000),
       12,
       file_column_name,
       column_position,
       table_column_name,
       is_accumulator,
       created_by
FROM ahub_dw.file_columns
WHERE client_file_id = 1);


------------- Step 4 -------------
-- Insert column rules records in the ahub_dw.column_rules  for client_file_id = 12  ( Aetna Melton --> AHUB )
-- Here we are copying the column level validation rules of client_file_id=1 to the client_file_id=12

INSERT INTO ahub_dw.column_rules
(
  column_rules_id,
  file_column_id,
  priority,
  validation_type,
  equal_to,
  python_formula,
  list_of_values,
  error_code,
  error_level,
  is_active,
  created_by
)
(SELECT (cr.column_rules_id +120000),   --put 120k because 12k series has been already occupied by cvs_non_integrated file
       (fc.file_column_id +120000),
       cr.priority,
       cr.validation_type,
       cr.equal_to,
       cr.python_formula,
       cr.list_of_values,
       cr.error_code,
       cr.error_level,
       cr.is_active,
       cr.created_by
FROM ahub_dw.file_columns fc
  INNER JOIN ahub_dw.column_rules cr ON fc.file_column_id = cr.file_column_id
WHERE fc.client_file_id = 1
ORDER BY fc.file_column_id,
         cr.column_rules_id);

------------- Step 5 -------------
-- UPDATE records in 'ahub_dw.column_rules' for client_file_id = 12  ( Aetna Melton --> AHUB )
-- updation of column rules vary from client to client.
-- For example, Client Pass Through and  Sender ID are different from BCI (rules what we imported). So update those particular fields as below:

---Updating Client Pass Through value for Aetna in column rules table
UPDATE ahub_dw.column_rules
SET equal_to = 'INGENIORXMELTON00489'
WHERE validation_type = 'EQUALTO'
AND file_column_id IN (SELECT file_column_id
FROM ahub_dw.file_columns
WHERE client_file_id = 12
AND file_column_name = 'Client Pass Through');

--Updating Sender ID value for Aetna in column rules table
UPDATE ahub_dw.column_rules
SET equal_to = '20600AETNA'
WHERE validation_type = 'EQUALTO'
AND file_column_id IN (SELECT file_column_id
FROM ahub_dw.file_columns
WHERE client_file_id = 12
AND file_column_name = 'Sender ID');

--- Aetna supports 12 accumulators. Hence making 5-12 accumulators (which are imported of BCI will have 'N') active ( is_active = 'Y' ).
UPDATE ahub_dw.column_rules
SET is_active = 'Y'     ---allow 12 accumulators to get validated
WHERE file_column_id IN (SELECT file_column_id
FROM ahub_dw.file_columns
WHERE client_file_id = 12
AND file_column_name in ('Accumulator Balance Qualifier 5','Accumulator Network Indicator 5','Accumulator Applied Amount 5','Action Code 5',
'Accumulator Balance Qualifier 6','Accumulator Network Indicator 6','Accumulator Applied Amount 6','Action Code 6',
'Accumulator Balance Qualifier 7','Accumulator Network Indicator 7','Accumulator Applied Amount 7','Action Code 7',
'Accumulator Balance Qualifier 8','Accumulator Network Indicator 8','Accumulator Applied Amount 8','Action Code 8',
'Accumulator Balance Qualifier 9','Accumulator Network Indicator 9','Accumulator Applied Amount 9','Action Code 9',
'Accumulator Balance Qualifier 10','Accumulator Network Indicator 10','Accumulator Applied Amount 10','Action Code 10',
'Accumulator Balance Qualifier 11','Accumulator Network Indicator 11','Accumulator Applied Amount 11','Action Code 11',
'Accumulator Balance Qualifier 12','Accumulator Network Indicator 12','Accumulator Applied Amount 12','Action Code 12'));

---update list_of_values to validate upto 12 accumulators for Aetna
UPDATE ahub_dw.column_rules
SET list_of_values = '01,02,03,04,05,06,07,08,09,10,11,12'
WHERE validation_type = 'RANGE'
AND file_column_id IN (SELECT file_column_id
FROM ahub_dw.file_columns
WHERE client_file_id = 12
AND file_column_name like 'Accumulator Balance Count%');



------- ################## Insert client_file_id = 13 ( AHUB --> Aetna Melton ) ##################

------------- Step 1  -------------
-- Insert a new record in the ahub_dw.client_files
-- RUN this global INSERT statement which is set as per SIT in any environment SIT/UAT/PROD initially.
-- later you will update specific columns as per environment in Step 2

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
  13,
  3,
  'OUTBOUND',
  '',
  1.00,
  300,
  true,
  'Aetna Melton Accumulation File',
  'Process: From database Outbound file is generated and exported to Aetna Melton
Path : Database to Aetna Melton
File Pattern : ACCDLYINT_TST_INRXATN_YYMMDDHHMMSS (UAT), ACCDLYINT_PRD_INRXATN_YYMMDDHHMMSS (PRD)
Frequency : Every 2 weeks (On Mondays)
Time : 05:00 AM - 07:00 AM EST',
  True,
  'outbound/archive',
  '',
  '',
  0,
  'outbound/txtfiles',
  'T',      ---for OUTBOUND outbound_transmission_type_column is required and is defaulted to 'T' for any INT file, for error file it will be 'R'
  '00990CAREMARK',
  '00489INGENIORX',
  '00489INGENIORX',
  '20600AETNA',
  'outbound/temp',
  '',
  'Database to Aetna Melton file',
  'From database export outbound file to Aetna Melton',
  NULL,
  false,
  false,
  false,
  '',
---below values are initially inserted taking SIT as reference. They should be updated according to environment while running UPDATE statement in next step
  'ACCDLYINT_TST_INRXATN_YYMMDDHHMMSS',
  'SIT',
  'ACCDLYINT_TST_INRXATN_',
  'T',
---below values remain constant for any incoming file. Need not overwrite them.
  'AHUB',
  NULL,
  NULL,
  'America/New_York',
  '2020-12-04 01:00:00.000',
  'irx-accum-phi-redshift-glue',
---below values may vary as per client. They must be changed if the distribution list (persons who would be notified in case of error, SLA failure ) is different.
  'irx_ahub_error_notification',
  'irx_ahub_incoming_file_extraction_error_notification',
  'irx_ahub_processing_notification',
  'irx_ahub_outbound_file_generation_notification',
---below values are for lambda functions and vary as per environment. For example, delivers_to_client is FALSE in lower envs and TRUE in higher envs.
  True,
  'irxah_export_client_file',
  False,
  False,
  'irx-nonprod-melton-aetna-east-1-sftp-app-outbound',
  'sit/accums/',
  '8491',
  '01/01/2021'
);


------------- Step 2 -------------
-- RUN below UPDATE statements as per environment (UAT/PROD) for client_file_id = 12  ( AHUB --> Aetna Melton )
-- By this we are updating the selective columns that satisfy the environmental level requirements.
-- For example: name_pattern varies between UAT and PROD.


-- Run this in UAT  Only

UPDATE ahub_dw.client_files
   SET
-- Following values would change per environment
       environment_level = 'PRE-PROD',
       name_pattern = 'ACCDLYINT_TST_INRXATN_YYMMDDHHMMSS',
       s3_output_file_name_prefix = 'ACCDLYINT_TST_INRXATN_',
       outbound_file_type_column = 'T',
       destination_bucket_cfx = 'irx-prod-melton-aetna-east-1-sftp-app-outbound',
       destination_folder_cfx = 'preprod/accums/',
       deliver_files_to_client = False
       WHERE client_file_id IN (13);

-- Run this in PROD only

UPDATE ahub_dw.client_files
   SET
-- Following values would change per environment
       environment_level = 'PROD',
       name_pattern = 'ACCDLYINT_PRD_INRXATN_YYMMDDHHMMSS',
       s3_output_file_name_prefix = 'ACCDLYINT_PRD_INRXATN_',
       outbound_file_type_column = 'P',
       destination_bucket_cfx = 'irx-prod-melton-aetna-east-1-sftp-app-outbound',
       destination_folder_cfx = 'prod/accums/',
       deliver_files_to_client = True
       WHERE client_file_id IN (13);


-------------   THE END   ---------------