-- Run in UAT/SIT/PROD
-- This is a harmless update.

UPDATE ahub_dw.export_setting
   SET header_query = '


UNLOAD (''select ''''{routing_id}'''' AS PRCS_ROUT_ID,

''''HD'''' AS RECD_TYPE,

''''T'''' AS TRANSMISSION_FILE_TYP,

to_char(sysdate, ''''YYYYMMDD'''') AS SRC_CREATE_DT,

to_char(sysdate, ''''HHMI'''') AS SRC_CREATE_TS,

''''00489INGENIORX'''' as SENDER_IDENTIFIER,
 

cf.output_receiver_id as RECEIVER_IDENTIFIER,

''''0000001'''' AS BATCH_NBR,

cf.outbound_file_type_column as FILE_TYP,

''''10'''' AS VER_RELEASE_NBR,

'''' '''' AS RESERVED_SP

from ahub_dw.client_files cf

where client_file_id=22'')

TO ''s3://{s3_out_bucket}/{s3_file_path}'' iam_role ''{iam_role}''

            FIXEDWIDTH

            ''0:200,1:2,2:1,3:8,4:4,5:30,6:30,7:7,8:1,9:2,10:1415''

            ALLOWOVERWRITE

            parallel off;
'
WHERE client_file_id = 22;

UPDATE ahub_dw.client_files
SET import_columns = 'processor_routing_identification, record_type, transmission_file_type, version_release_number, sender_identifier, receiver_identifier, submission_number, transaction_response_status, reject_code, record_length, reserved_1, transmission_date, transmission_time, date_of_service, service_provider_identifier_qualifier, service_provider_identifier, document_reference_identifier_qualifier, document_reference_identifier, transmission_identifier, benefit_type, in_network_indicator, formulary_status, accumulator_action_code, sender_reference_number, insurance_code, accumulator_balance_benefit_type, benefit_effective_date, benefit_termination_date, accumulator_change_source_code, transaction_identifier, transaction_identifier_cross_reference, adjustment_reason_code, accumulator_reference_time_stamp, reserved_2, cardholder_identifier, group_identifier, patient_first_name, middle_initial, patient_last_name, patient_relationship_code, date_of_birth, patient_gender_code, patient_state_province_address, cardholder_last_name, carrier_number, contract_number, client_pass_through, family_identifier_number, cardholder_identifier_alternate, group_identifier_alternate, patient_identifier, person_code, reserved_3, accumulator_balance_count, accumulator_specific_category_type, reserved_4, accumulator_1_balance_qualifier, accumulator_1_network_indicator, accumulator_1_applied_amount, accumulator_1_applied_amount_action_code, accumulator_1_benefit_period_amount, accumulator_1_benefit_period_amount_action_code, accumulator_1_remaining_balance, accumulator_1_remaining_balance_action_code, accumulator_2_balance_qualifier, accumulator_2_network_indicator, accumulator_2_applied_amount, accumulator_2_applied_amount_action_code, accumulator_2_benefit_period_amount, accumulator_2_benefit_period_amount_action_code, accumulator_2_remaining_balance, accumulator_2_remaining_balance_action_code, accumulator_3_balance_qualifier, accumulator_3_network_indicator, accumulator_3_applied_amount, accumulator_3_applied_amount_action_code, accumulator_3_benefit_period_amount, accumulator_3_benefit_period_amount_action_code, accumulator_3_remaining_balance, accumulator_3_remaining_balance_action_code, accumulator_4_balance_qualifier, accumulator_4_network_indicator, accumulator_4_applied_amount, accumulator_4_applied_amount_action_code, accumulator_4_benefit_period_amount, accumulator_4_benefit_period_amount_action_code, accumulator_4_remaining_balance, accumulator_4_remaining_balance_action_code, accumulator_5_balance_qualifier, accumulator_5_network_indicator, accumulator_5_applied_amount, accumulator_5_applied_amount_action_code, accumulator_5_benefit_period_amount, accumulator_5_benefit_period_amount_action_code, accumulator_5_remaining_balance, accumulator_5_remaining_balance_action_code, accumulator_6_balance_qualifier, accumulator_6_network_indicator, accumulator_6_applied_amount, accumulator_6_applied_amount_action_code, accumulator_6_benefit_period_amount, accumulator_6_benefit_period_amount_action_code, accumulator_6_remaining_balance, accumulator_6_remaining_balance_action_code, reserved_5, accumulator_7_balance_qualifier, accumulator_7_network_indicator, accumulator_7_applied_amount, accumulator_7_applied_amount_action_code, accumulator_7_benefit_period_amount, accumulator_7_benefit_period_amount_action_code, accumulator_7_remaining_balance, accumulator_7_remaining_balance_action_code, accumulator_8_balance_qualifier, accumulator_8_network_indicator, accumulator_8_applied_amount, accumulator_8_applied_amount_action_code, accumulator_8_benefit_period_amount, accumulator_8_benefit_period_amount_action_code, accumulator_8_remaining_balance, accumulator_8_remaining_balance_action_code, accumulator_9_balance_qualifier, accumulator_9_network_indicator, accumulator_9_applied_amount, accumulator_9_applied_amount_action_code, accumulator_9_benefit_period_amount, accumulator_9_benefit_period_amount_action_code, accumulator_9_remaining_balance, accumulator_9_remaining_balance_action_code, accumulator_10_balance_qualifier, accumulator_10_network_indicator, accumulator_10_applied_amount, accumulator_10_applied_amount_action_code, accumulator_10_benefit_period_amount, accumulator_10_benefit_period_amount_action_code, accumulator_10_remaining_balance, accumulator_10_remaining_balance_action_code, accumulator_11_balance_qualifier, accumulator_11_network_indicator, accumulator_11_applied_amount, accumulator_11_applied_amount_action_code, accumulator_11_benefit_period_amount, accumulator_11_benefit_period_amount_action_code, accumulator_11_remaining_balance, accumulator_11_remaining_balance_action_code, accumulator_12_balance_qualifier, accumulator_12_network_indicator, accumulator_12_applied_amount, accumulator_12_applied_amount_action_code, accumulator_12_benefit_period_amount, accumulator_12_benefit_period_amount_action_code, accumulator_12_remaining_balance, accumulator_12_remaining_balance_action_code, optional_data_indicator, total_amount_paid, total_amount_paid_action_code, amount_of_copay, amount_of_copay_action_code, patient_pay_amount, patient_pay_amount_action_code, amount_attributed_to_product_selection_brand, amount_attributed_to_product_selection_brand_action_code, amount_attributed_to_sales_tax, amount_attributed_to_sales_tax_action_code, amount_attributed_to_processor_fee, amount_attributed_to_processor_fee_action_code, gross_amount_due, gross_amount_due_action_code, invoiced_amount, invoiced_amount_action_code, penalty_amount, penalty_amount_action_code, reserved_6, product_service_identifier_qualifier, product_service_identifier, days_supply, quantity_dispensed, product_service_name, brand_generic_indicator, therapeutic_class_code_qualifier, therapeutic_class_code, dispensed_as_written, reserved_7, line_number,job_key',
position_specification_file = 'inbound/specifications/cvs_position_specification.txt'
WHERE CLIENT_FILE_ID IN (12,14,18,19,23);



-- Update already ran - No harm done if ran in production
UPDATE AHUB_DW.CLIENT_FILES 
 SET destination_bucket_cfx ='irx-prod-melton-aetna-east-1-sftp-app-outbound' ,  
     destination_folder_cfx ='AetnaAccumulatorDollar_Reports/toAetna/'
Where client_file_id=13;

-- Update already ran - No harm done if ran in production
-- Gila River will send inbound files to Accum only from Monday to Saturday
UPDATE ahub_dw.file_schedule
   SET cron_expression = 'utc: 0 12 * * MON-SAT'
WHERE file_schedule_id = 100;