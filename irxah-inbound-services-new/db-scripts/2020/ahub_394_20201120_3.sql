####### this whole script intends to introduce new CVS Non Integrated file with client_file_id = 11 ######

-- Following statement ( commented out below) was used to generate the query below
--select 'UPDATE ahub_dw.file_columns  SET table_column_name=''' || fc.table_column_name || ''' where file_column_name =''' || fc.file_column_name || ''' and client_file_id=3; '
--c.client_file_id,fc.file_column_name, fc.table_column_name ,fc.*
--from ahub_dw.file_columns fc
--where fc.client_file_id=1
--order by fc.file_column_id

-- This query fixes the CVS table_column_name for CVS
-- Where to run SIT/UAT and on day of deployment in PROD
UPDATE ahub_dw.file_columns  SET table_column_name='ACCUMULATOR_DETAIL.PROCESSOR_ROUTING_IDENTIFICATION' where file_column_name ='Processor Routing Identification' and client_file_id=3;
UPDATE ahub_dw.file_columns  SET table_column_name='ACCUMULATOR_DETAIL.RECORD_TYPE' where file_column_name ='Record Type ' and client_file_id=3;
UPDATE ahub_dw.file_columns  SET table_column_name='ACCUMULATOR_DETAIL.TRANSMISSION_FILE_TYPE' where file_column_name ='Transmission File Type ' and client_file_id=3;
UPDATE ahub_dw.file_columns  SET table_column_name='ACCUMULATOR_DETAIL.SENDER_IDENTIFIER' where file_column_name ='Sender ID ' and client_file_id=3;
UPDATE ahub_dw.file_columns  SET table_column_name='ACCUMULATOR_DETAIL.RECEIVER_IDENTIFIER' where file_column_name ='Receiver ID ' and client_file_id=3;
UPDATE ahub_dw.file_columns  SET table_column_name='ACCUMULATOR_DETAIL.SUBMISSION_NUMBER' where file_column_name ='Submission Number ' and client_file_id=3;
UPDATE ahub_dw.file_columns  SET table_column_name='ACCUMULATOR_DETAIL.TRANSACTION_RESPONSE_STATUS' where file_column_name ='Transaction Response Status ' and client_file_id=3;
UPDATE ahub_dw.file_columns  SET table_column_name='ACCUMULATOR_DETAIL.REJECT_CODE' where file_column_name ='Reject Code' and client_file_id=3;
UPDATE ahub_dw.file_columns  SET table_column_name='ACCUMULATOR_DETAIL.TRANSMISSION_DATE' where file_column_name ='Transmission Date ' and client_file_id=3;
UPDATE ahub_dw.file_columns  SET table_column_name='ACCUMULATOR_DETAIL.TRANSMISSION_TIME' where file_column_name ='Transmission Time ' and client_file_id=3;
UPDATE ahub_dw.file_columns  SET table_column_name='ACCUMULATOR_DETAIL.DATE_OF_SERVICE' where file_column_name ='Date of Service ' and client_file_id=3;
UPDATE ahub_dw.file_columns  SET table_column_name='ACCUMULATOR_DETAIL.TRANSMISSION_IDENTIFIER' where file_column_name ='Transmission ID ' and client_file_id=3;
UPDATE ahub_dw.file_columns  SET table_column_name='ACCUMULATOR_DETAIL.BENEFIT_TYPE' where file_column_name ='Benefit Type ' and client_file_id=3;
UPDATE ahub_dw.file_columns  SET table_column_name='ACCUMULATOR_DETAIL.ACCUMULATOR_ACTION_CODE' where file_column_name ='Accumulator Action Code ' and client_file_id=3;
UPDATE ahub_dw.file_columns  SET table_column_name='ACCUMULATOR_DETAIL.BENEFIT_EFFECTIVE_DATE' where file_column_name ='Benefit Effective Date ' and client_file_id=3;
UPDATE ahub_dw.file_columns  SET table_column_name='ACCUMULATOR_DETAIL.BENEFIT_TERMINATION_DATE' where file_column_name ='Benefit Termination Date ' and client_file_id=3;
UPDATE ahub_dw.file_columns  SET table_column_name='ACCUMULATOR_DETAIL.TRANSACTION_IDENTIFIER' where file_column_name ='Transaction ID' and client_file_id=3;
UPDATE ahub_dw.file_columns  SET table_column_name='ACCUMULATOR_DETAIL.PATIENT_FIRST_NAME' where file_column_name ='Patient First Name ' and client_file_id=3;
UPDATE ahub_dw.file_columns  SET table_column_name='ACCUMULATOR_DETAIL.PATIENT_LAST_NAME' where file_column_name ='Patient Last Name ' and client_file_id=3;
UPDATE ahub_dw.file_columns  SET table_column_name='ACCUMULATOR_DETAIL.PATIENT_RELATIONSHIP_CODE' where file_column_name ='Patient Relationship Code ' and client_file_id=3;
UPDATE ahub_dw.file_columns  SET table_column_name='ACCUMULATOR_DETAIL.DATE_OF_BIRTH' where file_column_name ='Date of Birth ' and client_file_id=3;
UPDATE ahub_dw.file_columns  SET table_column_name='ACCUMULATOR_DETAIL.PATIENT_GENDER_CODE' where file_column_name ='Patient Gender Code ' and client_file_id=3;
UPDATE ahub_dw.file_columns  SET table_column_name='ACCUMULATOR_DETAIL.CARRIER_NUMBER' where file_column_name ='Carrier Number ' and client_file_id=3;
UPDATE ahub_dw.file_columns  SET table_column_name='ACCUMULATOR_DETAIL.CONTRACT_NUMBER' where file_column_name ='Contract Number ' and client_file_id=3;
UPDATE ahub_dw.file_columns  SET table_column_name='ACCUMULATOR_DETAIL.CLIENT_PASS_THROUGH' where file_column_name ='Client Pass Through ' and client_file_id=3;
UPDATE ahub_dw.file_columns  SET table_column_name='ACCUMULATOR_DETAIL.PATIENT_IDENTIFIER' where file_column_name ='Patient ID ' and client_file_id=3;
UPDATE ahub_dw.file_columns  SET table_column_name='ACCUMULATOR_DETAIL.ACCUMULATOR_BALANCE_COUNT' where file_column_name ='Accumulator Balance Count ' and client_file_id=3;
UPDATE ahub_dw.file_columns  SET table_column_name='ACCUMULATOR_DETAIL.ACCUMULATOR_SPECIFIC_CATEGORY_TYPE' where file_column_name ='Accumulator Specific Category Type' and client_file_id=3;
UPDATE ahub_dw.file_columns  SET table_column_name='ACCUMULATOR_DETAIL.ACCUMULATOR_1_BALANCE_QUALIFIER' where file_column_name ='Accumulator Balance Qualifier 1' and client_file_id=3;
UPDATE ahub_dw.file_columns  SET table_column_name='ACCUMULATOR_DETAIL.ACCUMULATOR_1_NETWORK_INDICATOR' where file_column_name ='Accumulator Network Indicator 1' and client_file_id=3;
UPDATE ahub_dw.file_columns  SET table_column_name='ACCUMULATOR_DETAIL.ACCUMULATOR_1_APPLIED_AMOUNT' where file_column_name ='Accumulator Applied Amount 1' and client_file_id=3;
UPDATE ahub_dw.file_columns  SET table_column_name='ACCUMULATOR_DETAIL.ACCUMULATOR_1_APPLIED_AMOUNT_ACTION_CODE' where file_column_name ='Action Code 1' and client_file_id=3;
UPDATE ahub_dw.file_columns  SET table_column_name='ACCUMULATOR_DETAIL.ACCUMULATOR_2_BALANCE_QUALIFIER' where file_column_name ='Accumulator Balance Qualifier 2' and client_file_id=3;
UPDATE ahub_dw.file_columns  SET table_column_name='ACCUMULATOR_DETAIL.ACCUMULATOR_2_NETWORK_INDICATOR' where file_column_name ='Accumulator Network Indicator 2' and client_file_id=3;
UPDATE ahub_dw.file_columns  SET table_column_name='ACCUMULATOR_DETAIL.ACCUMULATOR_2_APPLIED_AMOUNT' where file_column_name ='Accumulator Applied Amount 2' and client_file_id=3;
UPDATE ahub_dw.file_columns  SET table_column_name='ACCUMULATOR_DETAIL.ACCUMULATOR_2_APPLIED_AMOUNT_ACTION_CODE' where file_column_name ='Action Code 2' and client_file_id=3;
UPDATE ahub_dw.file_columns  SET table_column_name='ACCUMULATOR_DETAIL.ACCUMULATOR_3_BALANCE_QUALIFIER' where file_column_name ='Accumulator Balance Qualifier 3' and client_file_id=3;
UPDATE ahub_dw.file_columns  SET table_column_name='ACCUMULATOR_DETAIL.ACCUMULATOR_3_NETWORK_INDICATOR' where file_column_name ='Accumulator Network Indicator 3' and client_file_id=3;
UPDATE ahub_dw.file_columns  SET table_column_name='ACCUMULATOR_DETAIL.ACCUMULATOR_3_APPLIED_AMOUNT' where file_column_name ='Accumulator Applied Amount 3' and client_file_id=3;
UPDATE ahub_dw.file_columns  SET table_column_name='ACCUMULATOR_DETAIL.ACCUMULATOR_3_APPLIED_AMOUNT_ACTION_CODE' where file_column_name ='Action Code 3' and client_file_id=3;
UPDATE ahub_dw.file_columns  SET table_column_name='ACCUMULATOR_DETAIL.ACCUMULATOR_4_BALANCE_QUALIFIER' where file_column_name ='Accumulator Balance Qualifier 4' and client_file_id=3;
UPDATE ahub_dw.file_columns  SET table_column_name='ACCUMULATOR_DETAIL.ACCUMULATOR_4_NETWORK_INDICATOR' where file_column_name ='Accumulator Network Indicator 4' and client_file_id=3;
UPDATE ahub_dw.file_columns  SET table_column_name='ACCUMULATOR_DETAIL.ACCUMULATOR_4_APPLIED_AMOUNT' where file_column_name ='Accumulator Applied Amount 4' and client_file_id=3;
UPDATE ahub_dw.file_columns  SET table_column_name='ACCUMULATOR_DETAIL.ACCUMULATOR_4_APPLIED_AMOUNT_ACTION_CODE' where file_column_name ='Action Code 4' and client_file_id=3;
UPDATE ahub_dw.file_columns  SET table_column_name='ACCUMULATOR_DETAIL.ACCUMULATOR_5_BALANCE_QUALIFIER' where file_column_name ='Accumulator Balance Qualifier 5' and client_file_id=3;
UPDATE ahub_dw.file_columns  SET table_column_name='ACCUMULATOR_DETAIL.ACCUMULATOR_5_NETWORK_INDICATOR' where file_column_name ='Accumulator Network Indicator 5' and client_file_id=3;
UPDATE ahub_dw.file_columns  SET table_column_name='ACCUMULATOR_DETAIL.ACCUMULATOR_5_APPLIED_AMOUNT' where file_column_name ='Accumulator Applied Amount 5' and client_file_id=3;
UPDATE ahub_dw.file_columns  SET table_column_name='ACCUMULATOR_DETAIL.ACCUMULATOR_5_APPLIED_AMOUNT_ACTION_CODE' where file_column_name ='Action Code 5' and client_file_id=3;
UPDATE ahub_dw.file_columns  SET table_column_name='ACCUMULATOR_DETAIL.ACCUMULATOR_6_BALANCE_QUALIFIER' where file_column_name ='Accumulator Balance Qualifier 6' and client_file_id=3;
UPDATE ahub_dw.file_columns  SET table_column_name='ACCUMULATOR_DETAIL.ACCUMULATOR_6_NETWORK_INDICATOR' where file_column_name ='Accumulator Network Indicator 6' and client_file_id=3;
UPDATE ahub_dw.file_columns  SET table_column_name='ACCUMULATOR_DETAIL.ACCUMULATOR_6_APPLIED_AMOUNT' where file_column_name ='Accumulator Applied Amount 6' and client_file_id=3;
UPDATE ahub_dw.file_columns  SET table_column_name='ACCUMULATOR_DETAIL.ACCUMULATOR_6_APPLIED_AMOUNT_ACTION_CODE' where file_column_name ='Action Code 6' and client_file_id=3;
UPDATE ahub_dw.file_columns  SET table_column_name='ACCUMULATOR_DETAIL.ACCUMULATOR_7_BALANCE_QUALIFIER' where file_column_name ='Accumulator Balance Qualifier 7' and client_file_id=3;
UPDATE ahub_dw.file_columns  SET table_column_name='ACCUMULATOR_DETAIL.ACCUMULATOR_7_NETWORK_INDICATOR' where file_column_name ='Accumulator Network Indicator 7' and client_file_id=3;
UPDATE ahub_dw.file_columns  SET table_column_name='ACCUMULATOR_DETAIL.ACCUMULATOR_7_APPLIED_AMOUNT' where file_column_name ='Accumulator Applied Amount 7' and client_file_id=3;
UPDATE ahub_dw.file_columns  SET table_column_name='ACCUMULATOR_DETAIL.ACCUMULATOR_7_APPLIED_AMOUNT_ACTION_CODE' where file_column_name ='Action Code 7' and client_file_id=3;
UPDATE ahub_dw.file_columns  SET table_column_name='ACCUMULATOR_DETAIL.ACCUMULATOR_8_BALANCE_QUALIFIER' where file_column_name ='Accumulator Balance Qualifier 8' and client_file_id=3;
UPDATE ahub_dw.file_columns  SET table_column_name='ACCUMULATOR_DETAIL.ACCUMULATOR_8_NETWORK_INDICATOR' where file_column_name ='Accumulator Network Indicator 8' and client_file_id=3;
UPDATE ahub_dw.file_columns  SET table_column_name='ACCUMULATOR_DETAIL.ACCUMULATOR_8_APPLIED_AMOUNT' where file_column_name ='Accumulator Applied Amount 8' and client_file_id=3;
UPDATE ahub_dw.file_columns  SET table_column_name='ACCUMULATOR_DETAIL.ACCUMULATOR_8_APPLIED_AMOUNT_ACTION_CODE' where file_column_name ='Action Code 8' and client_file_id=3;
UPDATE ahub_dw.file_columns  SET table_column_name='ACCUMULATOR_DETAIL.ACCUMULATOR_9_BALANCE_QUALIFIER' where file_column_name ='Accumulator Balance Qualifier 9' and client_file_id=3;
UPDATE ahub_dw.file_columns  SET table_column_name='ACCUMULATOR_DETAIL.ACCUMULATOR_9_NETWORK_INDICATOR' where file_column_name ='Accumulator Network Indicator 9' and client_file_id=3;
UPDATE ahub_dw.file_columns  SET table_column_name='ACCUMULATOR_DETAIL.ACCUMULATOR_9_APPLIED_AMOUNT' where file_column_name ='Accumulator Applied Amount 9' and client_file_id=3;
UPDATE ahub_dw.file_columns  SET table_column_name='ACCUMULATOR_DETAIL.ACCUMULATOR_9_APPLIED_AMOUNT_ACTION_CODE' where file_column_name ='Action Code 9' and client_file_id=3;
UPDATE ahub_dw.file_columns  SET table_column_name='ACCUMULATOR_DETAIL.ACCUMULATOR_10_BALANCE_QUALIFIER' where file_column_name ='Accumulator Balance Qualifier 10' and client_file_id=3;
UPDATE ahub_dw.file_columns  SET table_column_name='ACCUMULATOR_DETAIL.ACCUMULATOR_10_NETWORK_INDICATOR' where file_column_name ='Accumulator Network Indicator 10' and client_file_id=3;
UPDATE ahub_dw.file_columns  SET table_column_name='ACCUMULATOR_DETAIL.ACCUMULATOR_10_APPLIED_AMOUNT' where file_column_name ='Accumulator Applied Amount 10' and client_file_id=3;
UPDATE ahub_dw.file_columns  SET table_column_name='ACCUMULATOR_DETAIL.ACCUMULATOR_10_APPLIED_AMOUNT_ACTION_CODE' where file_column_name ='Action Code 10' and client_file_id=3;
UPDATE ahub_dw.file_columns  SET table_column_name='ACCUMULATOR_DETAIL.ACCUMULATOR_11_BALANCE_QUALIFIER' where file_column_name ='Accumulator Balance Qualifier 11' and client_file_id=3;
UPDATE ahub_dw.file_columns  SET table_column_name='ACCUMULATOR_DETAIL.ACCUMULATOR_11_NETWORK_INDICATOR' where file_column_name ='Accumulator Network Indicator 11' and client_file_id=3;
UPDATE ahub_dw.file_columns  SET table_column_name='ACCUMULATOR_DETAIL.ACCUMULATOR_11_APPLIED_AMOUNT' where file_column_name ='Accumulator Applied Amount 11' and client_file_id=3;
UPDATE ahub_dw.file_columns  SET table_column_name='ACCUMULATOR_DETAIL.ACCUMULATOR_11_APPLIED_AMOUNT_ACTION_CODE' where file_column_name ='Action Code 11' and client_file_id=3;
UPDATE ahub_dw.file_columns  SET table_column_name='ACCUMULATOR_DETAIL.ACCUMULATOR_12_BALANCE_QUALIFIER' where file_column_name ='Accumulator Balance Qualifier 12' and client_file_id=3;
UPDATE ahub_dw.file_columns  SET table_column_name='ACCUMULATOR_DETAIL.ACCUMULATOR_12_NETWORK_INDICATOR' where file_column_name ='Accumulator Network Indicator 12' and client_file_id=3;
UPDATE ahub_dw.file_columns  SET table_column_name='ACCUMULATOR_DETAIL.ACCUMULATOR_12_APPLIED_AMOUNT' where file_column_name ='Accumulator Applied Amount 12' and client_file_id=3;
UPDATE ahub_dw.file_columns  SET table_column_name='ACCUMULATOR_DETAIL.ACCUMULATOR_12_APPLIED_AMOUNT_ACTION_CODE' where file_column_name ='Action Code 12' and client_file_id=3;


--Step 1 : Insert a new record in the ahub_dw.client_files

-- RUN this global INSERT statement in any environment SIT/UAT/PROD

INSERT INTO ahub_dw.client_files
(
---below field values change as per choice of incoming file
 client_file_id,
 client_id,
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
---below field values change as per environment if applicable
 name_pattern,
 environment_level,
 s3_output_file_name_prefix,
 outbound_file_type_column, --its P in Prod and rest all envs T. It is required only for client_file_ids where outbound file is generated.
---below field values remain constant for any incoming file. Need not change values for them.
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
 redshift_glue_iam_role_name
)
VALUES
(
---below values are changed according to the incoming file requirements
  11,
  2,
  'processor_routing_identification, record_type, transmission_file_type, version_release_number, sender_identifier, receiver_identifier, submission_number, transaction_response_status, reject_code, record_length, reserved_1, transmission_date, transmission_time, date_of_service, service_provider_identifier_qualifier, service_provider_identifier, document_reference_identifier_qualifier, document_reference_identifier, transmission_identifier, benefit_type, in_network_indicator, formulary_status, accumulator_action_code, sender_reference_number, insurance_code, accumulator_balance_benefit_type, benefit_effective_date, benefit_termination_date, accumulator_change_source_code, transaction_identifier, transaction_identifier_cross_reference, adjustment_reason_code, accumulator_reference_time_stamp, reserved_2, cardholder_identifier, group_identifier, patient_first_name, middle_initial, patient_last_name, patient_relationship_code, date_of_birth, patient_gender_code, patient_state_province_address, cardholder_last_name, carrier_number, contract_number, client_pass_through, family_identifier_number, cardholder_identifier_alternate, group_identifier_alternate, patient_identifier, person_code, reserved_3, accumulator_balance_count, accumulator_specific_category_type, reserved_4, accumulator_1_balance_qualifier, accumulator_1_network_indicator, accumulator_1_applied_amount, accumulator_1_applied_amount_action_code, accumulator_1_benefit_period_amount, accumulator_1_benefit_period_amount_action_code, accumulator_1_remaining_balance, accumulator_1_remaining_balance_action_code, accumulator_2_balance_qualifier, accumulator_2_network_indicator, accumulator_2_applied_amount, accumulator_2_applied_amount_action_code, accumulator_2_benefit_period_amount, accumulator_2_benefit_period_amount_action_code, accumulator_2_remaining_balance, accumulator_2_remaining_balance_action_code, accumulator_3_balance_qualifier, accumulator_3_network_indicator, accumulator_3_applied_amount, accumulator_3_applied_amount_action_code, accumulator_3_benefit_period_amount, accumulator_3_benefit_period_amount_action_code, accumulator_3_remaining_balance, accumulator_3_remaining_balance_action_code, accumulator_4_balance_qualifier, accumulator_4_network_indicator, accumulator_4_applied_amount, accumulator_4_applied_amount_action_code, accumulator_4_benefit_period_amount, accumulator_4_benefit_period_amount_action_code, accumulator_4_remaining_balance, accumulator_4_remaining_balance_action_code, accumulator_5_balance_qualifier, accumulator_5_network_indicator, accumulator_5_applied_amount, accumulator_5_applied_amount_action_code, accumulator_5_benefit_period_amount, accumulator_5_benefit_period_amount_action_code, accumulator_5_remaining_balance, accumulator_5_remaining_balance_action_code, accumulator_6_balance_qualifier, accumulator_6_network_indicator, accumulator_6_applied_amount, accumulator_6_applied_amount_action_code, accumulator_6_benefit_period_amount, accumulator_6_benefit_period_amount_action_code, accumulator_6_remaining_balance, accumulator_6_remaining_balance_action_code, reserved_5, accumulator_7_balance_qualifier, accumulator_7_network_indicator, accumulator_7_applied_amount, accumulator_7_applied_amount_action_code, accumulator_7_benefit_period_amount, accumulator_7_benefit_period_amount_action_code, accumulator_7_remaining_balance, accumulator_7_remaining_balance_action_code, accumulator_8_balance_qualifier, accumulator_8_network_indicator, accumulator_8_applied_amount, accumulator_8_applied_amount_action_code, accumulator_8_benefit_period_amount, accumulator_8_benefit_period_amount_action_code, accumulator_8_remaining_balance, accumulator_8_remaining_balance_action_code, accumulator_9_balance_qualifier, accumulator_9_network_indicator, accumulator_9_applied_amount, accumulator_9_applied_amount_action_code, accumulator_9_benefit_period_amount, accumulator_9_benefit_period_amount_action_code, accumulator_9_remaining_balance, accumulator_9_remaining_balance_action_code, accumulator_10_balance_qualifier, accumulator_10_network_indicator, accumulator_10_applied_amount, accumulator_10_applied_amount_action_code, accumulator_10_benefit_period_amount, accumulator_10_benefit_period_amount_action_code, accumulator_10_remaining_balance, accumulator_10_remaining_balance_action_code, accumulator_11_balance_qualifier, accumulator_11_network_indicator, accumulator_11_applied_amount, accumulator_11_applied_amount_action_code, accumulator_11_benefit_period_amount, accumulator_11_benefit_period_amount_action_code, accumulator_11_remaining_balance, accumulator_11_remaining_balance_action_code, accumulator_12_balance_qualifier, accumulator_12_network_indicator, accumulator_12_applied_amount, accumulator_12_applied_amount_action_code, accumulator_12_benefit_period_amount, accumulator_12_benefit_period_amount_action_code, accumulator_12_remaining_balance, accumulator_12_remaining_balance_action_code, optional_data_indicator, total_amount_paid, total_amount_paid_action_code, amount_of_copay, amount_of_copay_action_code, patient_pay_amount, patient_pay_amount_action_code, amount_attributed_to_product_selection_brand, amount_attributed_to_product_selection_brand_action_code, amount_attributed_to_sales_tax, amount_attributed_to_sales_tax_action_code, amount_attributed_to_processor_fee, amount_attributed_to_processor_fee_action_code, gross_amount_due, gross_amount_due_action_code, invoiced_amount, invoiced_amount_action_code, penalty_amount, penalty_amount_action_code, reserved_6, product_service_identifier_qualifier, product_service_identifier, days_supply, quantity_dispensed, product_service_name, brand_generic_indicator, therapeutic_class_code_qualifier, therapeutic_class_code, dispensed_as_written, reserved_7, line_number,job_key',
  1.00,
  300,
  true,
  'Daily',
  1,
  1,
  60,
  '07:00',
  'CVS Non Integrated Accumulation File',
  'CVS Non Integrated File
Path : CVS  to  AHUB
File Pattern : ACCDLYNON_TST_CVSINRX_MMDDYY.HHMMSS (UAT)
ACCDLYNON_PRD_CVSINRX_MMDDYY.HHMMSS( PRD)
Frequency : Daily
Time : 02:00 AM to 03:00 AM
Stored in database, no outbound file generated ( as of now)',
  false,
  'EDT',
  'inbound/cvs/archive',
  'inbound/cvs/error',
  'inbound/cvs/specification/cvspositions.csv',
  1700,
  '',
  '',
  '00990CAREMARK',
  '',
  '00489INGENIORX',
  '',
  'inbound/cvs/temp',
  'inbound/cvs/specification/cvs_range.txt',
  'CVS Non Integrated file to Database',
  'Validate inbound CVS Non Integrated file, split, load into redshift database',
  NULL,
  true,
  true,
  false,
  'Non Integrated',
---below values are initially inserted taking SIT as reference. They should be updated according to environment while running UPDATE statement in next step
  'ACCDLYNON_TST_CVSINRX_MMDDYY.HHMMSS',
  'SIT',
  '',
  'T',
---below values remain constant for any incoming file. Need not overwrite them.
  'INBOUND',
  'AHUB',
  NULL,
  NULL,
  'America/New_York',
  '2020-03-26 14:36:12.360',
  'irx_ahub_error_notification',
  'irx_ahub_sla_notification',
  'irx_ahub_processing_notification',
  'irx_ahub_outbound_file_generation_notification',
  'irx-accum-phi-redshift-glue'
  ''
);

--Step 2 : RUN below UPDATE statements as per environment

-- Run this in SIT Only

UPDATE ahub_dw.client_files
   SET
-- Following values would change per environment
       environment_level = 'SIT',
       name_pattern = 'ACCDLYNON_TST_CVSINRX_MMDDYY.HHMMSS'
       WHERE client_file_id IN (11);

-- Run this in UAT  Only

UPDATE ahub_dw.client_files
   SET
-- Following values would change per environment
       environment_level = 'PRE-PROD',
       name_pattern = 'ACCDLYNON_TST_CVSINRX_MMDDYY.HHMMSS'
       WHERE client_file_id IN (11);

-- Run this in PROD only

UPDATE ahub_dw.client_files
   SET
-- Following values would change per environment
       environment_level = 'PROD',
       name_pattern = 'ACCDLYNON_PRD_CVSINRX_MMDDYY.HHMMSS'
       WHERE client_file_id IN (11);

--Step 3 : Insert file columns records in the ahub_dw.file_columns

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
(SELECT (file_column_id +11000),
       11,
       file_column_name,
       column_position,
       table_column_name,
       is_accumulator,
       created_by
FROM ahub_dw.file_columns
WHERE client_file_id = 3);

--Step 4 : Insert column rules records in the ahub_dw.column_rules

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
(SELECT (cr.column_rules_id +11000),
       (fc.file_column_id +11000),
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
WHERE fc.client_file_id = 3
ORDER BY fc.file_column_id,
         cr.column_rules_id);


------ Script END  ------