####### this whole script intends to introduce new Recon Non Integrated file with client_file_id = 10 ######

--Step 1 : Insert a new record in the ahub_dw.client_files

-- RUN this global INSERT statement in any environment SIT/UAT/PROD

INSERT INTO ahub_dw.client_files
(
 client_file_id,
 client_id,
 name_pattern,
 import_columns,
 data_error_threshold,
 process_duration_threshold,
 is_data_error_alert_active,
 frequency_type,
 frequency_count,
 total_files_per_day,
 grace_period,
 poll_time_in_24_hour_format,
 file_description,
 long_description,
 outbound_successful_acknowledgement,
 current_timezone_abbreviation,
 archive_folder,
 error_folder,
 structure_specification_file,
 expected_line_length,
 s3_merge_output_path,
 outbound_file_type_column,
 outbound_transmission_type_column,
 input_sender_id,
 output_sender_id,
 input_receiver_id,
 output_receiver_id,
 s3_output_file_name_prefix,
 file_processing_location,
 position_specification_file,
 process_name,
 process_description,
 reserved_variable,
 validate_file_structure,
 validate_file_detail_columns,
 generate_outbound_file,
 file_category,
---below fields remain constant for any incoming file. Need not change values for them.
 file_type,
 created_by,
 updated_by,
 updated_timestamp,
 file_timezone,
 created_timestamp,
 data_error_notification_sns,
 sla_notification_sns,
 processing_notification_sns,
 outbound_file_generation_notification_sns,
 redshift_glue_iam_role_name,
 environment_level
)
VALUES
(
 10,
 2,
 'RECACCNON_TST_CVSINRX_MMDDYY.HHMMSS',
 'record_type,unique_record_identifier,data_file_sender_identifier,data_file_sender_name,cdh_client_identifier,patient_identifier,patient_date_of_birth,patient_first_name,patient_middle_initial,patient_last_name,patient_gender,carrier,account,group_name,accumulation_benefit_begin_date,accumulation_benefit_end_date,accumulator_segment_count,accumulator_1_balance_qualifier,accumulator_1_specific_category_type,accumulator_1_network_indicator,medical_claims_accumulation_1_balance,pharmacy_claims_accumulation_1_balance,total_medical_pharmacy_claims_accumulation_1_balance,accumulator_2_balance_qualifier,accumulator_2_specific_category_type,accumulator_2_network_indicator,medical_claims_accumulation_2_balance,pharmacy_claims_accumulation_2_balance,total_medical_pharmacy_claims_accumulation_2_balance,accumulator_3_balance_qualifier,accumulator_3_specific_category_type,accumulator_3_network_indicator,medical_claims_accumulation_3_balance,pharmacy_claims_accumulation_3_balance,total_medical_pharmacy_claims_accumulation_3_balance,accumulator_4_balance_qualifier,accumulator_4_specific_category_type,accumulator_4_network_indicator,medical_claims_accumulation_4_balance,pharmacy_claims_accumulation_4_balance,total_medical_pharmacy_claims_accumulation_4_balance,accumulator_5_balance_qualifier,accumulator_5_specific_category_type,accumulator_5_network_indicator,medical_claims_accumulation_5_balance,pharmacy_claims_accumulation_5_balance,total_medical_pharmacy_claims_accumulation_5_balance,accumulator_6_balance_qualifier,accumulator_6_specific_category_type,accumulator_6_network_indicator,medical_claims_accumulation_6_balance,pharmacy_claims_accumulation_6_balance,total_medical_pharmacy_claims_accumulation_6_balance,accumulator_7_balance_qualifier,accumulator_7_specific_category_type,accumulator_7_network_indicator,medical_claims_accumulation_7_balance,pharmacy_claims_accumulation_7_balance,total_medical_pharmacy_claims_accumulation_7_balance,accumulator_8_balance_qualifier,accumulator_8_specific_category_type,accumulator_8_network_indicator,medical_claims_accumulation_8_balance,pharmacy_claims_accumulation_8_balance,total_medical_pharmacy_claims_accumulation_8_balance,accumulator_9_balance_qualifier,accumulator_9_specific_category_type,accumulator_9_network_indicator,medical_claims_accumulation_9_balance,pharmacy_claims_accumulation_9_balance,total_medical_pharmacy_claims_accumulation_9_balance,accumulator_10_balance_qualifier,accumulator_10_specific_category_type,accumulator_10_network_indicator,medical_claims_accumulation_10_balance,pharmacy_claims_accumulation_10_balance,total_medical_pharmacy_claims_accumulation_10_balance,accumulator_11_balance_qualifier,accumulator_11_specific_category_type,accumulator_11_network_indicator,medical_claims_accumulation_11_balance,pharmacy_claims_accumulation_11_balance,total_medical_pharmacy_claims_accumulation_11_balance,accumulator_12_balance_qualifier,accumulator_12_specific_category_type,accumulator_12_network_indicator,medical_claims_accumulation_12_balance,pharmacy_claims_accumulation_12_balance,total_medical_pharmacy_claims_accumulation_12_balance,filler_space,line_number,job_key',
 2.00,
 300,
 true,
 'Weekly',
 1,
 1,
 720,
 '16:00',
 'Reconciliation Non Integrated File',
 'CVS Recon File Path : CVS → AHUB File
 Pattern : RECACCNON_TST_CVSINRX_MMDDYY.HHMMSS.txt (UAT) RECACCNON_PRD_CVSINRX_MMDDYY.HHMMSS ( PRD)
 Frequency : Weekly, Every Sunday
 Time : 06:00 AM
 Stored in database, no outbound file generated',
 false,
 'EDT',
 'inbound/cvs/archive',
 'inbound/cvs/error',
 'inbound/cvs/specification/reconpositions.csv',
 800,
 '',
 '',
 '',
 '00990CAREMARK',
 '',
 '00489INGENIORX',
 '',
 '',
 'inbound/cvs/temp',
 'inbound/cvs/specification/recon_range.txt',
 'CVS Non Integrated Recon file to Database',
 'Validate Inbound Recon Non Integrated file structure, Split it and Load into Redshift database',
 '',
 TRUE,
 FALSE,
 FALSE,
 'Non Integrated',
---below values remain constant for any incoming file. Need not overwrite them.
 'INBOUND',
 'AHUB',
 NULL,
 NULL,
 'America/New_York',
 '2020-10-08 16:11:19.058',
 'irx_ahub_error_notification',
 'irx_ahub_sla_notification',
 'irx_ahub_processing_notification',
 'irx_ahub_outbound_file_generation_notification',
 'irx-accum-phi-redshift-glue',
 ''
);

--Step 2 : RUN below UPDATE statements as per environment

-- Run this in SIT Only

UPDATE ahub_dw.client_files
   SET
-- Following values would change per environment
       environment_level = 'SIT',
       name_pattern = 'RECACCNON_TST_CVSINRX_MMDDYY.HHMMSS'
       WHERE client_file_id IN (10);

-- Run this in UAT  Only

UPDATE ahub_dw.client_files
   SET
-- Following values would change per environment
       environment_level = 'PRE-PROD',
       name_pattern = 'RECACCNON_TST_CVSINRX_MMDDYY.HHMMSS'
       WHERE client_file_id IN (10);

-- Run this in PROD only

UPDATE ahub_dw.client_files
   SET
-- Following values would change per environment
       environment_level = 'PROD',
       name_pattern = 'RECACCNON_PRD_CVSINRX_MMDDYY.HHMMSS'
       WHERE client_file_id IN (10);


------ Script END  ------