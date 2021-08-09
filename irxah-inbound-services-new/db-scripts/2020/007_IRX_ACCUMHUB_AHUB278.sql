ALTER TABLE ahub_dw.client_files RENAME TO client_files_old
CREATE TABLE IF NOT EXISTS ahub_dw.client_files
(
	client_file_id INTEGER NOT NULL  ENCODE az64
	,client_id INTEGER NOT NULL  ENCODE az64
	,name_pattern VARCHAR(256) NOT NULL  ENCODE lzo
	,sender_identifier VARCHAR(30)   ENCODE lzo
	,receiver_identifier VARCHAR(30)   ENCODE lzo
	,file_type VARCHAR(50)   ENCODE lzo
	,created_by VARCHAR(100) NOT NULL DEFAULT 'AHUB ETL'::character varying ENCODE lzo	
	,created_timestamp TIMESTAMP WITHOUT TIME ZONE NOT NULL DEFAULT ('now'::text)::timestamp without time zone ENCODE az64
	,updated_by VARCHAR(100)   ENCODE lzo
	,updated_timestamp TIMESTAMP WITHOUT TIME ZONE   ENCODE az64
	,import_columns VARCHAR(8000)   ENCODE lzo
	,alert_notification_arn VARCHAR(200)   ENCODE lzo
	,alert_threshold NUMERIC(10,2)   ENCODE az64
	,is_alert_active BOOLEAN   ENCODE RAW
	,frequency_type VARCHAR(15)   ENCODE lzo
	,frequency_count INTEGER   ENCODE az64
	,total_files_per_day INTEGER   ENCODE az64
	,grace_period INTEGER   ENCODE az64
	,poll_time_in_24_hour_format VARCHAR(5)   ENCODE lzo
	,sla_notification_arn VARCHAR(200)   ENCODE lzo
	,processing_notification_arn VARCHAR(200)   ENCODE lzo
	,file_timezone VARCHAR(50)  NOT NULL ENCODE lzo
	,process_duration_threshold INTEGER   ENCODE az64
	,file_description VARCHAR(255)   ENCODE lzo
	,long_description VARCHAR(1000)   ENCODE lzo
	,PRIMARY KEY (client_file_id)
)
DISTSTYLE AUTO
;
ALTER TABLE ahub_dw.client_files owner to awsuser;


INSERT INTO ahub_dw.client_files (client_file_id,client_id,name_pattern,sender_identifier,receiver_identifier,file_type,created_by,created_timestamp,updated_by,updated_timestamp,import_columns,alert_notification_arn,alert_threshold,is_alert_active,frequency_type,frequency_count,total_files_per_day,grace_period,poll_time_in_24_hour_format,sla_notification_arn,processing_notification_arn,file_timezone,process_duration_threshold,file_description,long_description) VALUES 
(6,2,'RECACC_TST_CVSINRX_MMDDYYHHMMSS','00990CAREMARK','00489INGENIORX','INBOUND','AHUB','2020-04-23 16:11:19.058',NULL,NULL,'record_type,unique_record_identifier,data_file_sender_identifier,data_file_sender_name,cdh_client_identifier,patient_identifier,patient_date_of_birth,patient_first_name,patient_middle_initial,patient_last_name,patient_gender,carrier,account,group_name,accumulation_benefit_begin_date,accumulation_benefit_end_date,accumulator_segment_count,accumulator_1_balance_qualifier,accumulator_1_specific_category_type,accumulator_1_network_indicator,medical_claims_accumulation_1_balance,pharmacy_claims_accumulation_1_balance,total_medical_pharmacy_claims_accumulation_1_balance,accumulator_2_balance_qualifier,accumulator_2_specific_category_type,accumulator_2_network_indicator,medical_claims_accumulation_2_balance,pharmacy_claims_accumulation_2_balance,total_medical_pharmacy_claims_accumulation_2_balance,accumulator_3_balance_qualifier,accumulator_3_specific_category_type,accumulator_3_network_indicator,medical_claims_accumulation_3_balance,pharmacy_claims_accumulation_3_balance,total_medical_pharmacy_claims_accumulation_3_balance,accumulator_4_balance_qualifier,accumulator_4_specific_category_type,accumulator_4_network_indicator,medical_claims_accumulation_4_balance,pharmacy_claims_accumulation_4_balance,total_medical_pharmacy_claims_accumulation_4_balance,accumulator_5_balance_qualifier,accumulator_5_specific_category_type,accumulator_5_network_indicator,medical_claims_accumulation_5_balance,pharmacy_claims_accumulation_5_balance,total_medical_pharmacy_claims_accumulation_5_balance,accumulator_6_balance_qualifier,accumulator_6_specific_category_type,accumulator_6_network_indicator,medical_claims_accumulation_6_balance,pharmacy_claims_accumulation_6_balance,total_medical_pharmacy_claims_accumulation_6_balance,accumulator_7_balance_qualifier,accumulator_7_specific_category_type,accumulator_7_network_indicator,medical_claims_accumulation_7_balance,pharmacy_claims_accumulation_7_balance,total_medical_pharmacy_claims_accumulation_7_balance,accumulator_8_balance_qualifier,accumulator_8_specific_category_type,accumulator_8_network_indicator,medical_claims_accumulation_8_balance,pharmacy_claims_accumulation_8_balance,total_medical_pharmacy_claims_accumulation_8_balance,accumulator_9_balance_qualifier,accumulator_9_specific_category_type,accumulator_9_network_indicator,medical_claims_accumulation_9_balance,pharmacy_claims_accumulation_9_balance,total_medical_pharmacy_claims_accumulation_9_balance,accumulator_10_balance_qualifier,accumulator_10_specific_category_type,accumulator_10_network_indicator,medical_claims_accumulation_10_balance,pharmacy_claims_accumulation_10_balance,total_medical_pharmacy_claims_accumulation_10_balance,accumulator_11_balance_qualifier,accumulator_11_specific_category_type,accumulator_11_network_indicator,medical_claims_accumulation_11_balance,pharmacy_claims_accumulation_11_balance,total_medical_pharmacy_claims_accumulation_11_balance,accumulator_12_balance_qualifier,accumulator_12_specific_category_type,accumulator_12_network_indicator,medical_claims_accumulation_12_balance,pharmacy_claims_accumulation_12_balance,total_medical_pharmacy_claims_accumulation_12_balance,filler_space,line_number,job_key','arn:aws:sns:us-east-1:474156701944:irx_ahub_error_notification',2.00,true,'Weekly',1,0,30,'06:00','arn:aws:sns:us-east-1:474156701944:ahub_sla_notification_arn','arn:aws:sns:us-east-1:474156701944:irx_ahub_processing_notification','America/New_York',3,'Reconciliation File','CVS Recon File
Path : CVS → AHUB	
File Pattern : RECACC_TST_CVSINRX_MMDDYY.HHMMSS.txt (UAT)
RECACC_PRD_CVSINRX_MMDDYY.HHMMSS ( PRD)
Frequency : Weekly, Every Sunday
Time : 06:00 AM
Stored in database, no outbound file generated.')
,(2,1,'ACCDLYINT_TST_INRXBCI_YYMMDDHHMMSS','00489INGENIORX','00990CAREMARK','OUTBOUND','AHUB','2020-03-26 10:33:38.878',NULL,NULL,NULL,NULL,NULL,NULL,NULL,NULL,1,30,'NA','arn:aws:sns:us-east-1:474156701944:ahub_sla_notification_arn','arn:aws:sns:us-east-1:474156701944:irx_ahub_processing_notification','America/New_York',3,'CVS Outbound File','Path : BCI → AHUB 
File Pattern  : ACCDLYINT_TST_INRXBCI_YYMMDDHHMMSS (UAT)
ACCDLYINT_PRD_INRXBCI_YYMMDDHHMMSS ( PROD)
Frequency : Based on incoming BCI file
File is placed in outbound/CVS folder.
The file generation is based on the incomig file.
The file generation is immediate.')
,(3,2,'ACCDLYINT_TST_CVSINRX_YYMMDDHHMMSS','00990CAREMARK','00489INGENIORX','INBOUND','AHUB','2020-03-26 10:36:12.360',NULL,NULL,'processor_routing_identification, record_type, transmission_file_type, version_release_number, sender_identifier, receiver_identifier, submission_number, transaction_response_status, reject_code, record_length, reserved_1, transmission_date, transmission_time, date_of_service, service_provider_identifier_qualifier, service_provider_identifier, document_reference_identifier_qualifier, document_reference_identifier, transmission_identifier, benefit_type, in_network_indicator, formulary_status, accumulator_action_code, sender_reference_number, insurance_code, accumulator_balance_benefit_type, benefit_effective_date, benefit_termination_date, accumulator_change_source_code, transaction_identifier, transaction_identifier_cross_reference, adjustment_reason_code, accumulator_reference_time_stamp, reserved_2, cardholder_identifier, group_identifier, patient_first_name, middle_initial, patient_last_name, patient_relationship_code, date_of_birth, patient_gender_code, patient_state_province_address, cardholder_last_name, carrier_number, contract_number, client_pass_through, family_identifier_number, cardholder_identifier_alternate, group_identifier_alternate, patient_identifier, person_code, reserved_3, accumulator_balance_count, accumulator_specific_category_type, reserved_4, accumulator_1_balance_qualifier, accumulator_1_network_indicator, accumulator_1_applied_amount, accumulator_1_applied_amount_action_code, accumulator_1_benefit_period_amount, accumulator_1_benefit_period_amount_action_code, accumulator_1_remaining_balance, accumulator_1_remaining_balance_action_code, accumulator_2_balance_qualifier, accumulator_2_network_indicator, accumulator_2_applied_amount, accumulator_2_applied_amount_action_code, accumulator_2_benefit_period_amount, accumulator_2_benefit_period_amount_action_code, accumulator_2_remaining_balance, accumulator_2_remaining_balance_action_code, accumulator_3_balance_qualifier, accumulator_3_network_indicator, accumulator_3_applied_amount, accumulator_3_applied_amount_action_code, accumulator_3_benefit_period_amount, accumulator_3_benefit_period_amount_action_code, accumulator_3_remaining_balance, accumulator_3_remaining_balance_action_code, accumulator_4_balance_qualifier, accumulator_4_network_indicator, accumulator_4_applied_amount, accumulator_4_applied_amount_action_code, accumulator_4_benefit_period_amount, accumulator_4_benefit_period_amount_action_code, accumulator_4_remaining_balance, accumulator_4_remaining_balance_action_code, accumulator_5_balance_qualifier, accumulator_5_network_indicator, accumulator_5_applied_amount, accumulator_5_applied_amount_action_code, accumulator_5_benefit_period_amount, accumulator_5_benefit_period_amount_action_code, accumulator_5_remaining_balance, accumulator_5_remaining_balance_action_code, accumulator_6_balance_qualifier, accumulator_6_network_indicator, accumulator_6_applied_amount, accumulator_6_applied_amount_action_code, accumulator_6_benefit_period_amount, accumulator_6_benefit_period_amount_action_code, accumulator_6_remaining_balance, accumulator_6_remaining_balance_action_code, reserved_5, accumulator_7_balance_qualifier, accumulator_7_network_indicator, accumulator_7_applied_amount, accumulator_7_applied_amount_action_code, accumulator_7_benefit_period_amount, accumulator_7_benefit_period_amount_action_code, accumulator_7_remaining_balance, accumulator_7_remaining_balance_action_code, accumulator_8_balance_qualifier, accumulator_8_network_indicator, accumulator_8_applied_amount, accumulator_8_applied_amount_action_code, accumulator_8_benefit_period_amount, accumulator_8_benefit_period_amount_action_code, accumulator_8_remaining_balance, accumulator_8_remaining_balance_action_code, accumulator_9_balance_qualifier, accumulator_9_network_indicator, accumulator_9_applied_amount, accumulator_9_applied_amount_action_code, accumulator_9_benefit_period_amount, accumulator_9_benefit_period_amount_action_code, accumulator_9_remaining_balance, accumulator_9_remaining_balance_action_code, accumulator_10_balance_qualifier, accumulator_10_network_indicator, accumulator_10_applied_amount, accumulator_10_applied_amount_action_code, accumulator_10_benefit_period_amount, accumulator_10_benefit_period_amount_action_code, accumulator_10_remaining_balance, accumulator_10_remaining_balance_action_code, accumulator_11_balance_qualifier, accumulator_11_network_indicator, accumulator_11_applied_amount, accumulator_11_applied_amount_action_code, accumulator_11_benefit_period_amount, accumulator_11_benefit_period_amount_action_code, accumulator_11_remaining_balance, accumulator_11_remaining_balance_action_code, accumulator_12_balance_qualifier, accumulator_12_network_indicator, accumulator_12_applied_amount, accumulator_12_applied_amount_action_code, accumulator_12_benefit_period_amount, accumulator_12_benefit_period_amount_action_code, accumulator_12_remaining_balance, accumulator_12_remaining_balance_action_code, optional_data_indicator, total_amount_paid, total_amount_paid_action_code, amount_of_copay, amount_of_copay_action_code, patient_pay_amount, patient_pay_amount_action_code, amount_attributed_to_product_selection_brand, amount_attributed_to_product_selection_brand_action_code, amount_attributed_to_sales_tax, amount_attributed_to_sales_tax_action_code, amount_attributed_to_processor_fee, amount_attributed_to_processor_fee_action_code, gross_amount_due, gross_amount_due_action_code, invoiced_amount, invoiced_amount_action_code, penalty_amount, penalty_amount_action_code, reserved_6, product_service_identifier_qualifier, product_service_identifier, days_supply, quantity_dispensed, product_service_name, brand_generic_indicator, therapeutic_class_code_qualifier, therapeutic_class_code, dispensed_as_written, reserved_7, line_number,job_key','arn:aws:sns:us-east-1:474156701944:irx_ahub_error_notification',1.00,true,'Hourly',2,12,30,'02:00','arn:aws:sns:us-east-1:474156701944:ahub_sla_notification_arn','arn:aws:sns:us-east-1:474156701944:irx_ahub_processing_notification','America/New_York',3,'CVS Integrated Accumulation File','CVS 2 Hourly File
Path : CVS → AHUB	
File Pattern : ACCDLYINT_TST_CVSINRX_MMDDYY.HHMMSS (UAT)
ACCDLYINT_PRD_CVSINRX_MMDDYY.HHMMSS( PRD)
Frequency : Daily, Every 2 Hours
Time : 02:00 AM
Stored in database, no outbound file generated.')
,(5,2,'ACCDLYERR_TST_BCI_YYMMDDHHMMSS','00489INGENIORX','20500BCIDAHO','OUTBOUND','AHUB','2020-03-26 10:36:54.328',NULL,NULL,NULL,NULL,NULL,NULL,'Daily',1,1,30,'04:00','arn:aws:sns:us-east-1:474156701944:ahub_sla_notification_arn','arn:aws:sns:us-east-1:474156701944:irx_ahub_processing_notification','America/New_York',3,'BCI Error File','Path : AHUB → BCI	
File Pattern : ACCDLYERR_TST_BCI_YYMMDDHHMMSS (UAT)
ACCDLYERR_PRD_BCI_YYMMDDHHMMSS (PRD)
Frequency : Daily 4 AM
Generated via scheduled job.
')
,(1,1,'ACCDLYINT_TST_BCIINRX_YYMMDDHHMMSS','20500BCIDAHO','00489INGENIORX','INBOUND','AHUB','2020-03-26 10:33:18.415',NULL,NULL,'processor_routing_identification,record_type,transmission_file_type,version_release_number,sender_identifier,receiver_identifier,submission_number,transaction_response_status,reject_code,record_length,reserved_1,transmission_date,transmission_time,date_of_service,service_provider_identifier_qualifier,service_provider_identifier,document_reference_identifier_qualifier,document_reference_identifier,transmission_identifier,benefit_type,in_network_indicator,formulary_status,accumulator_action_code,sender_reference_number,insurance_code,accumulator_balance_benefit_type,benefit_effective_date,benefit_termination_date,accumulator_change_source_code,transaction_identifier,transaction_identifier_cross_reference,adjustment_reason_code,accumulator_reference_time_stamp,reserved_2,cardholder_identifier,group_identifier,patient_first_name,middle_initial,patient_last_name,patient_relationship_code,date_of_birth,patient_gender_code,patient_state_province_address,cardholder_last_name,carrier_number,contract_number,client_pass_through,family_identifier_number,cardholder_identifier_alternate,group_identifier_alternate,patient_identifier,person_code,reserved_3,accumulator_balance_count,accumulator_specific_category_type,reserved_4,accumulator_1_balance_qualifier,accumulator_1_network_indicator,accumulator_1_applied_amount,accumulator_1_applied_amount_action_code,accumulator_1_benefit_period_amount,accumulator_1_benefit_period_amount_action_code,accumulator_1_remaining_balance,accumulator_1_remaining_balance_action_code,accumulator_2_balance_qualifier,accumulator_2_network_indicator,accumulator_2_applied_amount,accumulator_2_applied_amount_action_code,accumulator_2_benefit_period_amount,accumulator_2_benefit_period_amount_action_code,accumulator_2_remaining_balance,accumulator_2_remaining_balance_action_code,accumulator_3_balance_qualifier,accumulator_3_network_indicator,accumulator_3_applied_amount,accumulator_3_applied_amount_action_code,accumulator_3_benefit_period_amount,accumulator_3_benefit_period_amount_action_code,accumulator_3_remaining_balance,accumulator_3_remaining_balance_action_code,accumulator_4_balance_qualifier,accumulator_4_network_indicator,accumulator_4_applied_amount,accumulator_4_applied_amount_action_code,accumulator_4_benefit_period_amount,accumulator_4_benefit_period_amount_action_code,accumulator_4_remaining_balance,accumulator_4_remaining_balance_action_code,reserved_5,line_number,job_key','arn:aws:sns:us-east-1:474156701944:irx_ahub_error_notification',0.01,true,'Daily',1,5,30,'05:00','arn:aws:sns:us-east-1:474156701944:ahub_sla_notification_arn','arn:aws:sns:us-east-1:474156701944:irx_ahub_processing_notification','America/New_York',3,'BCI Integrated Accumulation File','BCI Integrated Accumulation Files 
Path : BCI → AHUB
Pattern : AACCDLYINT_TST_BCIINRX_YYMMDDHHMMSS (UAT)
, ACCDLYINT_PRD_BCIINRX_YYMMDDHHMMSS(PROD)
Frequency : Daily 
Time : 05:00 AM
Remarks : Output file is also generated and sent to CVS')
,(7,1,'ACCDLYERR_TST_BCIINRX_YYMMDDHHMMSS','20500BCIDAHO','00489INGENIORX','INBOUND','AHUB','2020-03-26 10:36:35.412',NULL,NULL,'processor_routing_identification,record_type,transmission_file_type,version_release_number,sender_identifier,receiver_identifier,submission_number,transaction_response_status,reject_code,record_length,reserved_1,transmission_date,transmission_time,date_of_service,service_provider_identifier_qualifier,service_provider_identifier,document_reference_identifier_qualifier,document_reference_identifier,transmission_identifier,benefit_type,in_network_indicator,formulary_status,accumulator_action_code,sender_reference_number,insurance_code,accumulator_balance_benefit_type,benefit_effective_date,benefit_termination_date,accumulator_change_source_code,transaction_identifier,transaction_identifier_cross_reference,adjustment_reason_code,accumulator_reference_time_stamp,reserved_2,cardholder_identifier,group_identifier,patient_first_name,middle_initial,patient_last_name,patient_relationship_code,date_of_birth,patient_gender_code,patient_state_province_address,cardholder_last_name,carrier_number,contract_number,client_pass_through,family_identifier_number,cardholder_identifier_alternate,group_identifier_alternate,patient_identifier,person_code,reserved_3,accumulator_balance_count,accumulator_specific_category_type,reserved_4,accumulator_1_balance_qualifier,accumulator_1_network_indicator,accumulator_1_applied_amount,accumulator_1_applied_amount_action_code,accumulator_1_benefit_period_amount,accumulator_1_benefit_period_amount_action_code,accumulator_1_remaining_balance,accumulator_1_remaining_balance_action_code,accumulator_2_balance_qualifier,accumulator_2_network_indicator,accumulator_2_applied_amount,accumulator_2_applied_amount_action_code,accumulator_2_benefit_period_amount,accumulator_2_benefit_period_amount_action_code,accumulator_2_remaining_balance,accumulator_2_remaining_balance_action_code,accumulator_3_balance_qualifier,accumulator_3_network_indicator,accumulator_3_applied_amount,accumulator_3_applied_amount_action_code,accumulator_3_benefit_period_amount,accumulator_3_benefit_period_amount_action_code,accumulator_3_remaining_balance,accumulator_3_remaining_balance_action_code,accumulator_4_balance_qualifier,accumulator_4_network_indicator,accumulator_4_applied_amount,accumulator_4_applied_amount_action_code,accumulator_4_benefit_period_amount,accumulator_4_benefit_period_amount_action_code,accumulator_4_remaining_balance,accumulator_4_remaining_balance_action_code,reserved_5,line_number,job_key','arn:aws:sns:us-east-1:474156701944:irx_ahub_error_notification',2.00,true,'Daily',1,1,30,'04:00','arn:aws:sns:us-east-1:474156701944:ahub_sla_notification_arn','arn:aws:sns:us-east-1:474156701944:irx_ahub_processing_notification','America/New_York',3,'BCI Error File','BCI Error File
Path : BCI → AHUB
Pattern : ACCDLYERR_TST_BCIINRX_YYMMDDHHMMSS(UAT), ACCDLYERR_PRD_BCIINRX_YYMMDDHHMMSS(PROD)
Frequency : Daily  
Time : 4 AM
Remarks : Contains DR records. Will not be send to CVS. It is stored in a database.
No outbound file generated.')
,(4,2,'ACCDLYINT_TST_BCI_YYMMDDHHMMSS','00489INGENIORX','20500BCIDAHO','OUTBOUND','AHUB','2020-03-26 10:36:35.412',NULL,NULL,NULL,'arn:aws:sns:us-east-1:474156701944:Ahub_structural_error_notify',1.00,true,'Daily',1,1,30,'04:00','arn:aws:sns:us-east-1:474156701944:ahub_sla_notification_arn','arn:aws:sns:us-east-1:474156701944:irx_ahub_processing_notification','America/New_York',3,'BCI Integrated Accumulation File','Path : AHUB → BCI	
File Pattern : ACCDLYINT_TST_BCI_YYMMDDHHMMSS (UAT)
ACCDLYINT_PRD_BCI_YYMMDDHHMMSS ( PROD)
Frequency : Once per day at 4 AM ET. Contains DQ records

')

CREATE TABLE IF NOT EXISTS ahub_dw.file_maintenance_window
(
	client_file_id INTEGER NOT NULL  ENCODE az64
	,description VARCHAR(255)   ENCODE lzo
	,start_timestamp TIMESTAMP WITHOUT TIME ZONE NOT NULL  ENCODE az64
	,end_timestamp TIMESTAMP WITHOUT TIME ZONE NOT NULL  ENCODE az64
	,created_by VARCHAR(100) NOT NULL DEFAULT 'AHUB ETL'::character varying ENCODE lzo
	,created_timestamp TIMESTAMP WITHOUT TIME ZONE NOT NULL DEFAULT ('now'::text)::timestamp without time zone ENCODE az64
	,updated_by VARCHAR(100)   ENCODE lzo
	,updated_timestamp TIMESTAMP WITHOUT TIME ZONE   ENCODE az64
	,PRIMARY KEY (client_file_id, start_timestamp)
)
DISTSTYLE AUTO
;
ALTER TABLE ahub_dw.file_maintenance_window owner to awsuser;

/* Following is specific to PROD */
UPDATE ahub_dw.client_files SET name_pattern='RECACC_PRD_CVSINRX_MMDDYY.HHMMSS' WHERE client_file_id=6;
UPDATE ahub_dw.client_files SET name_pattern='ACCDLYINT_PRD_INRXBCI_YYMMDDHHMMSS' WHERE client_file_id=2;
UPDATE ahub_dw.client_files SET name_pattern='ACCDLYINT_PRD_CVSINRX_MMDDYY.HHMMSS' WHERE client_file_id=3;
UPDATE ahub_dw.client_files SET name_pattern='ACCDLYERR_PRD_BCI_YYMMDDHHMMSS' WHERE client_file_id=5;
UPDATE ahub_dw.client_files SET name_pattern='ACCDLYINT_PRD_BCIINRX_YYMMDDHHMMSS' WHERE client_file_id=1;
UPDATE ahub_dw.client_files SET name_pattern='ACCDLYERR_PRD_BCIINRX_YYMMDDHHMMSS' WHERE client_file_id=7;
UPDATE ahub_dw.client_files SET name_pattern='ACCDLYINT_PRD_BCI_YYMMDDHHMMSS' WHERE client_file_id=4;

UPDATE ahub_dw.client_files SET alert_notification_arn='arn:aws:sns:us-east-1:206388457288:irx_ahub_error_notification'
UPDATE ahub_dw.client_files SET sla_notification_arn='arn:aws:sns:us-east-1:206388457288:irx_ahub_sla_notification'
UPDATE ahub_dw.client_files SET processing_notification_arn='arn:aws:sns:us-east-1:206388457288:irx_ahub_processing_notification'

ALTER TABLE ahub_dw.job ADD file_status varchar(50) NULL;

/* End  Following is specific to PROD */

/* Following is specific to PreProd - UAT */

UPDATE ahub_dw.client_files SET alert_notification_arn='arn:aws:sns:us-east-1:974356221399:irx_ahub_error_notification'
UPDATE ahub_dw.client_files SET sla_notification_arn='arn:aws:sns:us-east-1:974356221399:irx_ahub_sla_notification'
UPDATE ahub_dw.client_files SET processing_notification_arn='arn:aws:sns:us-east-1:974356221399:irx_ahub_processing_notification'

ALTER TABLE ahub_dw.job ADD file_status varchar(50) NULL;

/* End  Following is specific to PROD */

/* Following is specific to  SIT */

	UPDATE ahub_dw.client_files SET alert_notification_arn='arn:aws:sns:us-east-1:795038802291:irx_ahub_error_notification'
	UPDATE ahub_dw.client_files SET sla_notification_arn='arn:aws:sns:us-east-1:795038802291:irx_ahub_sla_notification'
	UPDATE ahub_dw.client_files SET processing_notification_arn='arn:aws:sns:us-east-1:795038802291:irx_ahub_processing_notification'

ALTER TABLE ahub_dw.job ADD file_status varchar(50) NULL;

/* End  Following is specific to SIT */

/* Following script ensures that the proper setup is done for the schedules*/

/* Everyday at 05:30 AM Eastern ( 09:30 AM UTC) for BCI to CVS for Inbound and Outbound*/
UPDATE ahub_dw.client_files SET poll_time_in_24_hour_format='09:30' WHERE client_file_id=1
/*Every 2 hour starting 02:30 AM Eastern ( 06:30 AM UTC) for CVS 2 Hourly File for Inbound*/
UPDATE ahub_dw.client_files SET poll_time_in_24_hour_format='06:30' WHERE client_file_id=3
/*Every Sunday  starting 06:30 AM Eastern ( 10:30 AM UTC) for Inbound CVS Recon file*/
UPDATE ahub_dw.client_files SET poll_time_in_24_hour_format='10:30' WHERE client_file_id=6
/*Everyday at 05:30 AM Eastern ( 09:30 AM UTC) BCI  DQ and DR files for outbound BCI DQ and DR files*/
UPDATE ahub_dw.client_files SET poll_time_in_24_hour_format='09:30' WHERE client_file_id=4
UPDATE ahub_dw.client_files SET total_files_per_day=1 
UPDATE ahub_dw.client_files SET total_files_per_day=12  WHERE client_file_id=3
UPDATE ahub_dw.client_files SET grace_period=30 
UPDATE ahub_dw.client_files SET file_timezone='America/New_York'

DROP TABLE ahub_dw.client_files_old




