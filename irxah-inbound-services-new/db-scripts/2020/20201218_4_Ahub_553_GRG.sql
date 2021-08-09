/* this whole script intends to introduce new Gila River file with
client_file_id = 16 (Custom file to Standard file), client_file_id = 23 (GRG → Database) and client_file_id = 17 (Database → GRG) */


------ ################## Insert client_file_id = 16  ( Custom Gila River file to Standard Gila River file ) ##################

------------- Step 1  -------------
-- Insert a new record in the ahub_dw.client_files
-- RUN this global INSERT statement which is set as per SIT in any environment SIT/UAT/PROD initially.
-- later you will update specific columns as per environment in Step 2
-- Gila River Custom file transformation to a standard file.
-- Nothing gets stored in the database, the process is all about converting custom to standard file.
-- The standard file gets generated in the s3_merge_output_path folder. So basically, we get the file in the inbound\txtfiles folder and then generated file ( a standard file) goes back to the inbound\txtfiles folder with a different naming pattern ( pointing to client file ID = 23)

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
  16,
  5,
  'INBOUND',
  '',
  0.01,
  300,
  true,
  'Custom Gila River Accumulation File',
  'Process: Transform Custom Gila River file to AHUB Generated Gila River Standard File
Path : Gila River Custom file transformation to a standard file.
Nothing gets stored in the database, the process is all about converting custom to standard file.
The standard file gets generated in the s3_merge_output_path folder. So basically, we get the file in the inbound\txtfiles folder and
then generated file ( a standard file) goes back to the inbound\txtfiles folder with a different naming pattern ( pointing to client file ID = 23)
File Pattern : ACCDLYINT_BCBSAZ_T_YYMMDDHHMMSS (UAT), ACCDLYINT_BCBSAZ_P_YYMMDDHHMMSS (PRD)
Frequency : Daily
Time : 03:00 AM - 05:00 AM MST',
  false,
  'inbound/archive',
  'inbound/error',
  '',
  1700,
  'inbound/txtfiles',
  '',
  '20800BCBSAZ',
  '00489INGENIORX',
  '00489INGENIORX',
  '00990CAREMARK',
  'inbound/temp',
  '',
  'Custom Gila River file to AHUB Generated Gila River Standard File',
  'Transform Custom Gila River file to AHUB Generated Gila River Standard File and Trigger Incoming Process Glue Job',
  NULL,
  false,
  false,
  false,
  '',
---below values are initially inserted taking SIT as reference. They should be updated according to environment while running UPDATE statement in next step
  'ACCDLYINT_BCBSAZ_T_YYMMDDHHMMSS',
  'SIT',
  'ACCDLYINT_GRGINRX_T_',
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
  'irxah_process_incoming_custom_file',
  False,
  False,
  '',
  '',
  '',
  '01/01/2021'
);


------------- Step 2 -------------
-- RUN below UPDATE statements as per environment (UAT/PROD) for client_file_id = 16  ( Custom Gila River file to Standard Gila River file )
-- By this we are updating the selective columns that satisfy the environmental level requirements.
-- For example: name_pattern varies between UAT and PROD.

-- Run this in UAT  Only

UPDATE ahub_dw.client_files
   SET
-- Following values would change per environment
       environment_level = 'PRE-PROD',
       name_pattern = 'ACCDLYINT_BCBSAZ_T_YYMMDDHHMMSS',
       s3_output_file_name_prefix = 'ACCDLYINT_GRGINRX_T_',
       deliver_files_to_client = False
       WHERE client_file_id IN (16);

-- Run this in PROD only

UPDATE ahub_dw.client_files
   SET
-- Following values would change per environment
       environment_level = 'PROD',
       name_pattern = 'ACCDLYINT_BCBSAZ_P_YYMMDDHHMMSS',
       s3_output_file_name_prefix = 'ACCDLYINT_GRGINRX_P_',
       deliver_files_to_client = True
       WHERE client_file_id IN (16);



------ ################## Insert client_file_id = 23   ( AHUB Standard Gila River --> AHUB ) ##################

------------- Step 1  -------------
-- Insert a new record in the ahub_dw.client_files
-- RUN this global INSERT statement which is set as per SIT in any environment SIT/UAT/PROD initially.
-- later you will update specific columns as per environment in Step 2
-- Here, AHUB Generated Standard File for Gila River is ingested in the database.

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
  23,
  5,
  'INBOUND',
  'processor_routing_identification,record_type,transmission_file_type,version_release_number,sender_identifier,receiver_identifier,submission_number,transaction_response_status,reject_code,record_length,reserved_1,transmission_date,transmission_time,date_of_service,service_provider_identifier_qualifier,service_provider_identifier,document_reference_identifier_qualifier,document_reference_identifier,transmission_identifier,benefit_type,in_network_indicator,formulary_status,accumulator_action_code,sender_reference_number,insurance_code,accumulator_balance_benefit_type,benefit_effective_date,benefit_termination_date,accumulator_change_source_code,transaction_identifier,transaction_identifier_cross_reference,adjustment_reason_code,accumulator_reference_time_stamp,reserved_2,cardholder_identifier,group_identifier,patient_first_name,middle_initial,patient_last_name,patient_relationship_code,date_of_birth,patient_gender_code,patient_state_province_address,cardholder_last_name,carrier_number,contract_number,client_pass_through,family_identifier_number,cardholder_identifier_alternate,group_identifier_alternate,patient_identifier,person_code,reserved_3,accumulator_balance_count,accumulator_specific_category_type,reserved_4,accumulator_1_balance_qualifier,accumulator_1_network_indicator,accumulator_1_applied_amount,accumulator_1_applied_amount_action_code,accumulator_1_benefit_period_amount,accumulator_1_benefit_period_amount_action_code,accumulator_1_remaining_balance,accumulator_1_remaining_balance_action_code,accumulator_2_balance_qualifier,accumulator_2_network_indicator,accumulator_2_applied_amount,accumulator_2_applied_amount_action_code,accumulator_2_benefit_period_amount,accumulator_2_benefit_period_amount_action_code,accumulator_2_remaining_balance,accumulator_2_remaining_balance_action_code,accumulator_3_balance_qualifier,accumulator_3_network_indicator,accumulator_3_applied_amount,accumulator_3_applied_amount_action_code,accumulator_3_benefit_period_amount,accumulator_3_benefit_period_amount_action_code,accumulator_3_remaining_balance,accumulator_3_remaining_balance_action_code,accumulator_4_balance_qualifier,accumulator_4_network_indicator,accumulator_4_applied_amount,accumulator_4_applied_amount_action_code,accumulator_4_benefit_period_amount,accumulator_4_benefit_period_amount_action_code,accumulator_4_remaining_balance,accumulator_4_remaining_balance_action_code,reserved_5,line_number,job_key',
  0.01,
  300,
  true,
  'AHUB Generated GILA River Standard File',
  'Process: AHUB Generated Standard File for Gila River is ingested in the database.
Path : AHUB Generated Standard File to Database
File Pattern : ACCDLYINT_GRGINRX_T_YYMMDDHHMMSS (UAT), ACCDLYINT_GRGINRX_P_YYMMDDHHMMSS (PRD)
Frequency : Daily
Time : 03:00 AM - 05:00 AM MST',
  true,
  'inbound/archive',
  'inbound/error',
  'inbound/specifications/gila_river_structure_specification.csv',
  1700,
  '',
  '',
  '20800BCBSAZ',
  '00489INGENIORX',
  '00489INGENIORX',
  '00990CAREMARK',
  'inbound/temp',
  'inbound/specifications/position_specification.txt',
  'AHUB Generated GILA River Standard file to database',
  'Validate the AHUB generated standard file ( originated from a custom file) and store in the database',
  NULL,
  true,
  true,
  false,
  '',
---below values are initially inserted taking SIT as reference. They should be updated according to environment while running UPDATE statement in next step
  'ACCDLYINT_GRGINRX_T_YYMMDDHHMMSS',
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
  'irxah_process_incoming_file_load_in_database',
  False,
  False,
  '',
  '',
  '',
  '01/01/2021'
);


------------- Step 2 -------------
-- RUN below UPDATE statements as per environment (UAT/PROD) for client_file_id = 23  ( Standard Gila River file to AHUB )
-- By this we are updating the selective columns that satisfy the environmental level requirements.
-- For example: name_pattern varies between UAT and PROD.

-- Run this in UAT  Only

UPDATE ahub_dw.client_files
   SET
-- Following values would change per environment
       environment_level = 'PRE-PROD',
       name_pattern = 'ACCDLYINT_GRGINRX_T_YYMMDDHHMMSS',
       deliver_files_to_client = False
       WHERE client_file_id IN (23);

-- Run this in PROD only

UPDATE ahub_dw.client_files
   SET
-- Following values would change per environment
       environment_level = 'PROD',
       name_pattern = 'ACCDLYINT_GRGINRX_P_YYMMDDHHMMSS',
       deliver_files_to_client = True
       WHERE client_file_id IN (23);



------------- Step 3  -------------
-- Insert file columns records in the ahub_dw.file_columns for client_file_id = 23  ( Standard Gila River file to AHUB )
-- Here we are copying the columnar metadata of client_file_id=1 to the client_file_id=23
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
(SELECT (file_column_id +23000),
       23,
       file_column_name,
       column_position,
       table_column_name,
       is_accumulator,
       created_by
FROM ahub_dw.file_columns
WHERE client_file_id = 1);


------------- Step 4 -------------
-- Insert column rules records in the ahub_dw.column_rules  for client_file_id = 23  ( Standard Gila River file to AHUB )
-- Here we are copying the column level validation rules of client_file_id=1 to the client_file_id=23

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
(SELECT (cr.column_rules_id +23000),
       (fc.file_column_id +23000),
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
-- UPDATE records in 'ahub_dw.column_rules' for client_file_id = 23  ( Standard Gila River file to AHUB )
-- updation of column rules vary from client to client.
-- For example, Client Pass Through and  Sender ID are different from BCI (rules what we imported). So update those particular fields as below:

---Updating Client Pass Through value for Gila in column rules table
UPDATE ahub_dw.column_rules
SET equal_to = 'INGENIORXGILA00489'
WHERE validation_type = 'EQUALTO'
AND file_column_id IN (SELECT file_column_id
FROM ahub_dw.file_columns
WHERE client_file_id = 23
AND file_column_name = 'Client Pass Through');

--Updating Sender ID value for Gila in column rules table
UPDATE ahub_dw.column_rules
SET equal_to = '20800BCBSAZ'
WHERE validation_type = 'EQUALTO'
AND file_column_id IN (SELECT file_column_id
FROM ahub_dw.file_columns
WHERE client_file_id = 23
AND file_column_name = 'Sender ID');

--- Gila River supports only 1 accumulator hence remaining 11 accumulators should be made inactive ( is_active = 'N' ).
--- As we borrow 12 accumulators from BCI (1-4 are 'Y' and 5-12 are 'N'), the query below assigns the Accumulator 2 to 4 to 'N'.
--- Hence, 1st accumulator would have 'Y' and accumulators 2 to 12 will have 'N'
UPDATE ahub_dw.column_rules
SET is_active = 'N'
WHERE file_column_id IN (SELECT file_column_id
FROM ahub_dw.file_columns
WHERE client_file_id = 23
AND file_column_name in ('Accumulator Balance Qualifier 2','Accumulator Network Indicator 2','Accumulator Applied Amount 2','Action Code 2',
'Accumulator Balance Qualifier 3','Accumulator Network Indicator 3','Accumulator Applied Amount 3','Action Code 3',
'Accumulator Balance Qualifier 4','Accumulator Network Indicator 4','Accumulator Applied Amount 4','Action Code 4'));


---update list_of_values to validate to 01 as only ONE accumulator is supported by Gila
UPDATE ahub_dw.column_rules
SET list_of_values = '01'
WHERE validation_type = 'RANGE'
AND file_column_id IN (SELECT file_column_id
FROM ahub_dw.file_columns
WHERE client_file_id = 23
AND file_column_name like 'Accumulator Balance Count%');



------- ################## Insert client_file_id = 17  ( AHUB --> Gila River ) ##################

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
  17,
  5,
  'OUTBOUND',
  '',
  1.00,
  300,
  true,
  'Gila River Accumulation File',
  'Process: From database Outbound file is generated and exported to Gila River
Path : Database to Gila River
File Pattern : ACCDLYINT_TST_INRXBCBSAZ_YYMMDDHHMMSS (UAT), ACCDLYINT_PRD_INRXBCBSAZ_YYMMDDHHMMSS (PRD)
Frequency : Daily
Time : 05:00 AM - 07:00 AM EST',
  True,
  'outbound/archive',
  '',
  '',
  0,
  'outbound/txtfiles',
  'T',
  '00990CAREMARK',
  '00489INGENIORX',
  '00489INGENIORX',
  '20800BCBSAZ',
  'outbound/temp',
  '',
  'Database to Gila River file',
  'From database export outbound file to Gila River',
  NULL,
  false,
  false,
  false,
  '',
---below values are initially inserted taking SIT as reference. They should be updated according to environment while running UPDATE statement in next step
  'ACCDLYINT_TST_INRXBCBSAZ_YYMMDDHHMMSS',
  'SIT',
  'ACCDLYINT_TST_INRXBCBSAZ_',
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
  'irx-nonprod-gila-east-1-sftp-app-outbound',
  'sit/accums/',
  '',
  '01/01/2021'
);

------------------- Final Step  -------------
-- RUN below UPDATE statements as per environment (UAT/PROD) for client_file_id = 16 and 23  to ensure all the fields are set properly.

--- update client_file_id= 16
-- Gila River Custom file transformation to a standard file.
-- Nothing gets stored in the database, the process is all about converting custom to standard file.
-- The standard file gets generated in the s3_merge_output_path folder. So basically, we get the file in the inbound\txtfiles folder and then generated file ( a standard file) goes back to the inbound\txtfiles folder with a different naming pattern ( pointing to client file ID = 23)


UPDATE ahub_dw.client_files
SET glue_job_name = 'irxah_process_incoming_custom_file',
structure_specification_file = '', -- There is no standard structure to follow for the custom file
position_specification_file ='', -- Therre is no standard position to parse , since this is a custom file
s3_merge_output_path = 'inbound/txtfiles', -- generated file gets copied here..
s3_output_file_name_prefix = 'ACCDLYINT_GRGINRX_T_', --generated file follows this pattern
extract_folder = 'inbound/txtfiles',
archive_folder = 'inbound/archive',
error_folder = 'inbound/error'
WHERE CLIENT_FILE_ID IN (16);


-- update client_file_id= 23
-- Here, AHUB Generated Standard File for Gila River is ingested in the database

UPDATE ahub_dw.client_files
SET long_description = 'Process: AHUB Generated Standard File for Gila River is ingested in the database.
Path : AHUB Generated Standard File to Database
File Pattern : ACCDLYINT_GRGINRX_T_YYMMDDHHMMSS (UAT), ACCDLYINT_GRGINRX_P_YYMMDDHHMMSS (PRD)
Frequency : Daily
Time : 03:00 AM - 05:00 AM MST
Incoming file is stored in database',
extract_folder = 'inbound/txtfiles',
archive_folder = 'inbound/archive',
error_folder = 'inbound/error',
structure_specification_file = 'inbound/specifications/gila_river_structure_specification.csv',
-- Gila River Specific
position_specification_file = 'inbound/specifications/position_specification.txt',
-- Standard NCPDP
glue_job_name = 'irxah_process_incoming_file_load_in_database'
WHERE client_file_id = 23;
-- Use the output of the query below to run the update statement..
-- Column Rule ID is not known in advance, it is based on the how column rules are setup ..
-- The query below detects it run - time..
-- You need to run the output of this query ( the update statement)
-- DO NOT RUN THE UPDATE BELOW IF YOU HAVE NOT CONFIGURED THE COLUMN_RULES FOR THE RESPECTIVE INBOUND CLIENTS..

SELECT --cr.column_rules_id, fc.column_position, c.client_file_id, c.client_id,
' UPDATE AHUB_DW.CLIENT_FILES SET column_rule_id_for_transmission_id_validation= ' || cr.column_rules_id || ' where CLIENT_FILE_ID =' || c.client_file_id || ';'
FROM ahub_dw.column_rules cr
INNER JOIN ahub_dw.file_columns fc
ON cr.file_column_id = fc.file_column_id
INNER JOIN ahub_dw.client_files c
ON fc.client_file_id = c.client_file_id
WHERE c.file_type = 'INBOUND'
AND fc.column_position = '357:407'
AND cr.validation_type = 'EXTERNAL'
AND c.client_file_id in (23);


------------- Step 2 -------------
-- RUN below UPDATE statements as per environment (UAT/PROD) for client_file_id = 17  (  AHUB --> Gila River )
-- By this we are updating the selective columns that satisfy the environmental level requirements.
-- For example: name_pattern varies between UAT and PROD.


-- Run this in UAT  Only

UPDATE ahub_dw.client_files
   SET
-- Following values would change per environment
       environment_level = 'PRE-PROD',
       name_pattern = 'ACCDLYINT_TST_INRXBCBSAZ_YYMMDDHHMMSS',
       s3_output_file_name_prefix = 'ACCDLYINT_TST_INRXBCBSAZ_',
       outbound_file_type_column = 'T',
       destination_bucket_cfx = 'irx-prod-gila-uat-east-1-sftp-app-outbound',
       destination_folder_cfx = 'preprod/accums/',
       deliver_files_to_client = False
       WHERE client_file_id IN (17);

-- Run this in PROD only

UPDATE ahub_dw.client_files
   SET
-- Following values would change per environment
       environment_level = 'PROD',
       name_pattern = 'ACCDLYINT_PRD_INRXBCBSAZ_YYMMDDHHMMSS',
       s3_output_file_name_prefix = 'ACCDLYINT_PRD_INRXBCBSAZ_',
       outbound_file_type_column = 'P',
       destination_bucket_cfx = 'irx-prod-gila-east-1-sftp-app-outbound',
       destination_folder_cfx = 'prod/accums/',
       deliver_files_to_client = True
       WHERE client_file_id IN (17);


-------------   THE END   ---------------