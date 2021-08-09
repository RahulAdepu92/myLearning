####### this whole script intends to introduce new BCI GP file with client_file_id = 9 ######

------insert records to accommodate the column level validation requirement for client_file_id = 9 in file_columns table

## Step 1 :  Insert  records in ahub_dw.file_columns

INSERT INTO ahub_dw.file_columns
(file_column_id,client_file_id,file_column_name,column_position,table_column_name,is_accumulator,created_by)
(select (file_column_id+9000),9,file_column_name,column_position,table_column_name,is_accumulator,created_by
from ahub_dw.file_columns
where client_file_id=1);

## Step 2 : Insert column rules records in the ahub_dw.column_rules

INSERT INTO ahub_dw.column_rules
(column_rules_id,file_column_id,priority,validation_type,equal_to,python_formula,list_of_values,error_code,error_level,is_active,created_by)
(Select  (cr.column_rules_id+9000), (fc.file_column_id+9000), cr.priority , cr.validation_type, cr.equal_to, cr.python_formula, cr.list_of_values, cr.error_code, cr.error_level, cr.is_active,cr.created_by
From ahub_dw.file_columns fc
Inner Join ahub_dw.column_rules cr
On fc.file_column_id=cr.file_column_id
Where fc.client_file_id=1
Order by fc.file_column_id, cr.column_rules_id);


## STOP HERE AND READ BELOW.
################# search by key word GOTO<env> to directly search the sqls that to be run as per environment (SIT,UAT,PROD) ###################

## GOTOSIT (to run sqls in SIT environment only)

------ inserting new record to accommodate BCI GP Integrated file with client_file_id = 9 (name_pattern varies for prod) in client_files table

INSERT INTO ahub_dw.client_files (client_file_id,client_id,name_pattern,file_type,created_by,created_timestamp,updated_by,updated_timestamp,import_columns,alert_notification_arn,alert_threshold,is_alert_active,frequency_type,frequency_count,total_files_per_day,grace_period,poll_time_in_24_hour_format,sla_notification_arn,processing_notification_arn,file_timezone,process_duration_threshold,file_description,long_description,outbound_file_generation_notification_arn,outbound_successful_acknowledgement,current_timezone) VALUES
(9,1,'ACCDLYINT_TST_BCIGPINRX_YYMMDDHHMMSS','INBOUND','AHUB','2020-09-28 10:36:35.412',NULL,NULL,'processor_routing_identification,record_type,transmission_file_type,version_release_number,sender_identifier,receiver_identifier,submission_number,transaction_response_status,reject_code,record_length,reserved_1,transmission_date,transmission_time,date_of_service,service_provider_identifier_qualifier,service_provider_identifier,document_reference_identifier_qualifier,document_reference_identifier,transmission_identifier,benefit_type,in_network_indicator,formulary_status,accumulator_action_code,sender_reference_number,insurance_code,accumulator_balance_benefit_type,benefit_effective_date,benefit_termination_date,accumulator_change_source_code,transaction_identifier,transaction_identifier_cross_reference,adjustment_reason_code,accumulator_reference_time_stamp,reserved_2,cardholder_identifier,group_identifier,patient_first_name,middle_initial,patient_last_name,patient_relationship_code,date_of_birth,patient_gender_code,patient_state_province_address,cardholder_last_name,carrier_number,contract_number,client_pass_through,family_identifier_number,cardholder_identifier_alternate,group_identifier_alternate,patient_identifier,person_code,reserved_3,accumulator_balance_count,accumulator_specific_category_type,reserved_4,accumulator_1_balance_qualifier,accumulator_1_network_indicator,accumulator_1_applied_amount,accumulator_1_applied_amount_action_code,accumulator_1_benefit_period_amount,accumulator_1_benefit_period_amount_action_code,accumulator_1_remaining_balance,accumulator_1_remaining_balance_action_code,accumulator_2_balance_qualifier,accumulator_2_network_indicator,accumulator_2_applied_amount,accumulator_2_applied_amount_action_code,accumulator_2_benefit_period_amount,accumulator_2_benefit_period_amount_action_code,accumulator_2_remaining_balance,accumulator_2_remaining_balance_action_code,accumulator_3_balance_qualifier,accumulator_3_network_indicator,accumulator_3_applied_amount,accumulator_3_applied_amount_action_code,accumulator_3_benefit_period_amount,accumulator_3_benefit_period_amount_action_code,accumulator_3_remaining_balance,accumulator_3_remaining_balance_action_code,accumulator_4_balance_qualifier,accumulator_4_network_indicator,accumulator_4_applied_amount,accumulator_4_applied_amount_action_code,accumulator_4_benefit_period_amount,accumulator_4_benefit_period_amount_action_code,accumulator_4_remaining_balance,accumulator_4_remaining_balance_action_code,reserved_5,line_number,job_key','795038802291:irx_ahub_error_notification',0.01,true,'Daily',1,1,630,'14:30','795038802291:irx_ahub_sla_notification','795038802291:irx_ahub_processing_notification','America/New_York',300,'BCI GP Integrated File','Path : BCI → CVS
File Pattern : ACCDLYINT_TST_BCIGPINRX_YYMMDDHHMMSS (UAT)
ACCDLYINT_PRD_BCIGPINRX_YYMMDDHHMMSS (PROD)
Remarks : Output file is also generated and sent to CVS','795038802291:irx_ahub_outbound_file_generation_notification',true,'EDT');

-----update newly added fields for client_file_id = 9 in client_files table

update ahub_dw.client_files
set
environment_level = 'SIT'				--varies as per env
,archive_folder = 'inbound/bci/archive'
,error_folder = 'inbound/bci/error'
,structure_specification_file = 'inbound/bci/specification/bcipositions.csv'
,expected_line_length = 1700
,iam_arn = 'arn:aws:iam::795038802291:role/irx-accum-phi-redshift-glue' 	--varies as per env
,s3_merge_output_path = 'outbound/cvs/txtfiles'
,file_type_column_in_outbound_file = 'T'  --varies as per env, its P in Prod and rest all envs T
,transmission_file_type = 'T'			--remains constant for all flows and envs
,input_sender_id = '20500BCIDAHO'
,output_sender_id = '00489INGENIORX'
,input_receiver_id = '00489INGENIORX'
,output_receiver_id = '00990CAREMARK'
,s3_output_file_name_prefix = 'ACCDLYINT_TST_INRXBCI_'			--varies as per env
,file_processing_location = 'inbound/bci/temp'
,position_specification_file = 'inbound/bci/specification/bci_range.txt'
,validate_file_string = 'TRUE'
,validate_detail_records_in_file_structure = ''				---this field id used by bci error file only, rest all flow have null value
,process_name = 'BCI GP Integrated file to CVS'
,process_description = 'Validate inbound BCI GP Integrated file, split, load into redshift database and generate a CVS outbound file'
where client_file_id in (9);



## GOTOUAT (to run sqls in UAT environment only)

------ inserting new record to accommodate BCI GP Integrated file with client_file_id = 9 (name_pattern varies for prod) in client_files table

INSERT INTO ahub_dw.client_files (client_file_id,client_id,name_pattern,file_type,created_by,created_timestamp,updated_by,updated_timestamp,import_columns,alert_notification_arn,alert_threshold,is_alert_active,frequency_type,frequency_count,total_files_per_day,grace_period,poll_time_in_24_hour_format,sla_notification_arn,processing_notification_arn,file_timezone,process_duration_threshold,file_description,long_description,outbound_file_generation_notification_arn,outbound_successful_acknowledgement,current_timezone) VALUES
(9,1,'ACCDLYINT_TST_BCIGPINRX_YYMMDDHHMMSS','INBOUND','AHUB','2020-09-28 10:36:35.412',NULL,NULL,'processor_routing_identification,record_type,transmission_file_type,version_release_number,sender_identifier,receiver_identifier,submission_number,transaction_response_status,reject_code,record_length,reserved_1,transmission_date,transmission_time,date_of_service,service_provider_identifier_qualifier,service_provider_identifier,document_reference_identifier_qualifier,document_reference_identifier,transmission_identifier,benefit_type,in_network_indicator,formulary_status,accumulator_action_code,sender_reference_number,insurance_code,accumulator_balance_benefit_type,benefit_effective_date,benefit_termination_date,accumulator_change_source_code,transaction_identifier,transaction_identifier_cross_reference,adjustment_reason_code,accumulator_reference_time_stamp,reserved_2,cardholder_identifier,group_identifier,patient_first_name,middle_initial,patient_last_name,patient_relationship_code,date_of_birth,patient_gender_code,patient_state_province_address,cardholder_last_name,carrier_number,contract_number,client_pass_through,family_identifier_number,cardholder_identifier_alternate,group_identifier_alternate,patient_identifier,person_code,reserved_3,accumulator_balance_count,accumulator_specific_category_type,reserved_4,accumulator_1_balance_qualifier,accumulator_1_network_indicator,accumulator_1_applied_amount,accumulator_1_applied_amount_action_code,accumulator_1_benefit_period_amount,accumulator_1_benefit_period_amount_action_code,accumulator_1_remaining_balance,accumulator_1_remaining_balance_action_code,accumulator_2_balance_qualifier,accumulator_2_network_indicator,accumulator_2_applied_amount,accumulator_2_applied_amount_action_code,accumulator_2_benefit_period_amount,accumulator_2_benefit_period_amount_action_code,accumulator_2_remaining_balance,accumulator_2_remaining_balance_action_code,accumulator_3_balance_qualifier,accumulator_3_network_indicator,accumulator_3_applied_amount,accumulator_3_applied_amount_action_code,accumulator_3_benefit_period_amount,accumulator_3_benefit_period_amount_action_code,accumulator_3_remaining_balance,accumulator_3_remaining_balance_action_code,accumulator_4_balance_qualifier,accumulator_4_network_indicator,accumulator_4_applied_amount,accumulator_4_applied_amount_action_code,accumulator_4_benefit_period_amount,accumulator_4_benefit_period_amount_action_code,accumulator_4_remaining_balance,accumulator_4_remaining_balance_action_code,reserved_5,line_number,job_key','974356221399:irx_ahub_error_notification',0.01,true,'Daily',1,1,630,'14:30','974356221399:irx_ahub_sla_notification','974356221399:irx_ahub_processing_notification','America/New_York',300,'BCI GP Integrated File','Path : BCI → CVS
File Pattern : ACCDLYINT_TST_BCIGPINRX_YYMMDDHHMMSS (UAT)
ACCDLYINT_PRD_BCIGPINRX_YYMMDDHHMMSS (PROD)
Remarks : Output file is also generated and sent to CVS','974356221399:irx_ahub_outbound_file_generation_notification',true,'EDT');

-----update newly added fields for client_file_id = 9 in client_files table

update ahub_dw.client_files
set
environment_level = 'PRE-PROD'				--varies as per env
,archive_folder = 'inbound/bci/archive'
,error_folder = 'inbound/bci/error'
,structure_specification_file = 'inbound/bci/specification/bcipositions.csv'
,expected_line_length = 1700
,iam_arn = 'arn:aws:iam::974356221399:role/irx-accum-phi-redshift-glue' 	--varies as per env
,s3_merge_output_path = 'outbound/cvs/txtfiles'
,file_type_column_in_outbound_file = 'T'  --varies as per env, its P in Prod and rest all envs T
,transmission_file_type = 'T'			--remains constant for all flows and envs
,input_sender_id = '20500BCIDAHO'
,output_sender_id = '00489INGENIORX'
,input_receiver_id = '00489INGENIORX'
,output_receiver_id = '00990CAREMARK'
,s3_output_file_name_prefix = 'ACCDLYINT_TST_INRXBCI_'			--varies as per env
,file_processing_location = 'inbound/bci/temp'
,position_specification_file = 'inbound/bci/specification/bci_range.txt'
,validate_file_string = 'TRUE'
,validate_detail_records_in_file_structure = ''				---this field id used by bci error file only, rest all flow have null value
,process_name = 'BCI GP Integrated file to CVS'
,process_description = 'Validate inbound BCI GP Integrated file, split, load into redshift database and generate a CVS outbound file'
where client_file_id in (9);



## GOTOPROD (to run sqls in PROD environment only)

------ inserting new record to accommodate BCI GP Integrated file with client_file_id = 9 (name_pattern varies for prod) in client_files table

INSERT INTO ahub_dw.client_files (client_file_id,client_id,name_pattern,file_type,created_by,created_timestamp,updated_by,updated_timestamp,import_columns,alert_notification_arn,alert_threshold,is_alert_active,frequency_type,frequency_count,total_files_per_day,grace_period,poll_time_in_24_hour_format,sla_notification_arn,processing_notification_arn,file_timezone,process_duration_threshold,file_description,long_description,outbound_file_generation_notification_arn,outbound_successful_acknowledgement,current_timezone) VALUES
(9,1,'ACCDLYINT_PRD_BCIGPINRX_YYMMDDHHMMSS','INBOUND','AHUB','2020-09-28 10:36:35.412',NULL,NULL,'processor_routing_identification,record_type,transmission_file_type,version_release_number,sender_identifier,receiver_identifier,submission_number,transaction_response_status,reject_code,record_length,reserved_1,transmission_date,transmission_time,date_of_service,service_provider_identifier_qualifier,service_provider_identifier,document_reference_identifier_qualifier,document_reference_identifier,transmission_identifier,benefit_type,in_network_indicator,formulary_status,accumulator_action_code,sender_reference_number,insurance_code,accumulator_balance_benefit_type,benefit_effective_date,benefit_termination_date,accumulator_change_source_code,transaction_identifier,transaction_identifier_cross_reference,adjustment_reason_code,accumulator_reference_time_stamp,reserved_2,cardholder_identifier,group_identifier,patient_first_name,middle_initial,patient_last_name,patient_relationship_code,date_of_birth,patient_gender_code,patient_state_province_address,cardholder_last_name,carrier_number,contract_number,client_pass_through,family_identifier_number,cardholder_identifier_alternate,group_identifier_alternate,patient_identifier,person_code,reserved_3,accumulator_balance_count,accumulator_specific_category_type,reserved_4,accumulator_1_balance_qualifier,accumulator_1_network_indicator,accumulator_1_applied_amount,accumulator_1_applied_amount_action_code,accumulator_1_benefit_period_amount,accumulator_1_benefit_period_amount_action_code,accumulator_1_remaining_balance,accumulator_1_remaining_balance_action_code,accumulator_2_balance_qualifier,accumulator_2_network_indicator,accumulator_2_applied_amount,accumulator_2_applied_amount_action_code,accumulator_2_benefit_period_amount,accumulator_2_benefit_period_amount_action_code,accumulator_2_remaining_balance,accumulator_2_remaining_balance_action_code,accumulator_3_balance_qualifier,accumulator_3_network_indicator,accumulator_3_applied_amount,accumulator_3_applied_amount_action_code,accumulator_3_benefit_period_amount,accumulator_3_benefit_period_amount_action_code,accumulator_3_remaining_balance,accumulator_3_remaining_balance_action_code,accumulator_4_balance_qualifier,accumulator_4_network_indicator,accumulator_4_applied_amount,accumulator_4_applied_amount_action_code,accumulator_4_benefit_period_amount,accumulator_4_benefit_period_amount_action_code,accumulator_4_remaining_balance,accumulator_4_remaining_balance_action_code,reserved_5,line_number,job_key','206388457288:irx_ahub_error_notification',0.01,true,'Daily',1,1,630,'14:30','206388457288:irx_ahub_sla_notification','206388457288:irx_ahub_processing_notification','America/New_York',300,'BCI GP Integrated File','Path : BCI → CVS
File Pattern : ACCDLYINT_TST_BCIGPINRX_YYMMDDHHMMSS (UAT)
ACCDLYINT_PRD_BCIGPINRX_YYMMDDHHMMSS (PROD)
Remarks : Output file is also generated and sent to CVS','206388457288:irx_ahub_outbound_file_generation_notification',true,'EDT');


-----update newly added fields for client_file_id = 9 in client_files table

update ahub_dw.client_files
set
environment_level = 'PROD'				--varies as per env
,archive_folder = 'inbound/bci/archive'
,error_folder = 'inbound/bci/error'
,structure_specification_file = 'inbound/bci/specification/bcipositions.csv'
,expected_line_length = 1700
,iam_arn = 'arn:aws:iam::206388457288:role/irx-accum-phi-redshift-glue' 	--varies as per env
,s3_merge_output_path = 'outbound/cvs/txtfiles'
,file_type_column_in_outbound_file = 'P'  --varies as per env, its P in Prod and rest all envs T
,transmission_file_type = 'T'			--remains constant for all flows and envs
,input_sender_id = '20500BCIDAHO'
,output_sender_id = '00489INGENIORX'
,input_receiver_id = '00489INGENIORX'
,output_receiver_id = '00990CAREMARK'
,s3_output_file_name_prefix = 'ACCDLYINT_PRD_INRXBCI_'			--varies as per env
,file_processing_location = 'inbound/bci/temp'
,position_specification_file = 'inbound/bci/specification/bci_range.txt'
,validate_file_string = 'TRUE'
,validate_detail_records_in_file_structure = ''				---this field id used by bci error file only, rest all flow have null value
,process_name = 'BCI GP Integrated file to CVS'
,process_description = 'Validate inbound BCI GP Integrated file, split, load into redshift database and generate a CVS outbound file'
where client_file_id in (9);

---**************************The following script configures the column_rule in the UAT/SIT/PROD
---************************** This query needs to be executed when GP program goes in the production.
---***********************PLEASE NOTE THAT GP PROGRAM IS NOT BEING DEPLOYED IN PRODUCTION********************

-- This query updates and sets the rule of the Accumulator Balance Qualifer for incoming BCI commercial file
-- earlier the list_of_values had 14 and 15.
-- Now the value of 14 and 15 is expected in a BCI GP file.
UPDATE ahub_dw.column_rules cr set list_of_values='02,04,05,06,07,08' where cr.column_rules_id in
(SELECT cr.column_rules_id
From ahub_dw.file_columns fc
Inner join ahub_dw.column_rules cr
On fc.file_column_id=cr.file_column_id 
Where fc.file_column_name like 'Accumulator Balance Qualifier%'
And cr.validation_type='REQUIRED_AND_RANGE'
And cr.is_active='Y'
And fc.client_file_id=1)


-- This query updates and sets the rule of the Accumulator Balance Qualifer for incoming BCI GP file
-- Allowed list_of_values is  14 and 15 only.
UPDATE ahub_dw.column_rules cr set list_of_values='14,15' where cr.column_rules_id in
(SELECT cr.column_rules_id
From ahub_dw.file_columns fc
Inner join ahub_dw.column_rules cr
On fc.file_column_id=cr.file_column_id 
Where fc.file_column_name like 'Accumulator Balance Qualifier%'
And cr.validation_type='REQUIRED_AND_RANGE'
And cr.is_active='Y'
And fc.client_file_id=9)

