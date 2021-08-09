/*==============================================================*/
/* Script : IRX_ACCUMHUB_DDL.sql								*/
/* Author : Akashtha (ag65804)								    */
/* Project : AHUB (MVP2)                                        */
/* Production Release Date: 05/22/2020                          */
/* Purpose : Added new Schema AHUB_DW and all new objects       */
/*           such as new tables and views                       */
/*==============================================================*/

/*==============================================================*/
/* Schema: AHUB_DW									            */
/*==============================================================*/

CREATE SCHEMA IF NOT EXISTS AHUB_DW AUTHORIZATION awsuser;

/*==============================================================*/
/* Drop existing view in AHUB_DW (if any)			            */
/*==============================================================*/

DROP VIEW IF EXISTS AHUB_DW.VW_ACCUMULATOR_DETAIL_VALIDATION_RESULT; 
DROP VIEW IF EXISTS AHUB_DW.VW_ACCUMULATOR_DETAIL_DETOKENIZED; 
DROP VIEW IF EXISTS AHUB_DW.VW_ACCUMULATOR_RECONCILIATION_DETAIL_DETOKENIZED; 

/*==============================================================*/
/* Drop existing tables in AHUB_DW (if any)			            */
/*==============================================================*/

DROP TABLE IF EXISTS ahub_dw.ACCUMULATOR_DETAIL;
DROP TABLE IF EXISTS ahub_dw.ACCUMULATOR_RECONCILIATION_DETAIL;
DROP TABLE IF EXISTS ahub_dw.file_validation_result;
DROP TABLE IF EXISTS ahub_dw.job_detail;
DROP TABLE IF EXISTS ahub_dw.job;
DROP TABLE IF EXISTS ahub_dw.column_rules;
DROP TABLE IF EXISTS ahub_dw.column_error_codes;
DROP TABLE IF EXISTS ahub_dw.file_columns;
DROP TABLE IF EXISTS ahub_dw.client_files;
DROP TABLE IF EXISTS ahub_dw.client;

/*==============================================================*/
/* Table: CLIENT						    				    */
/*==============================================================*/

CREATE TABLE IF NOT EXISTS ahub_dw.client (
	client_id          integer NOT NULL,
	abbreviated_name   varchar(100) NOT NULL,
	"name"             varchar(256),
	created_by         varchar(100) DEFAULT 'AHUB ETL'::character varying NOT NULL,
	created_timestamp  timestamp DEFAULT convert_timezone(('US/Eastern'::character varying)::text, ('now'::character varying)::timestamp without time zone) NOT NULL,
	updated_by         varchar(100),
	updated_timestamp  timestamp,
	PRIMARY KEY (client_id)
)
distkey(client_id)
;

INSERT INTO ahub_dw.client (client_id,abbreviated_name,name) 
VALUES
  (1,'BCI','Blue Cross Idhao'),
  (2,'CVS','CVS');

/*==============================================================*/
/* Table: CLIENT_FILES						    			    */
/*==============================================================*/

CREATE TABLE IF NOT EXISTS ahub_dw.client_files (
	client_file_id       integer NOT NULL,
	client_id            integer NOT NULL,
	name_pattern         varchar(256) NOT NULL,
	sender_identifier    varchar(30),
	receiver_identifier  varchar(30),
	file_type            varchar(50),
	import_columns       varchar(8000),
	created_by           varchar(100) DEFAULT 'AHUB ETL'::character varying NOT NULL,
	created_timestamp    timestamp DEFAULT convert_timezone(('US/Eastern'::character varying)::text, ('now'::character varying)::timestamp without time zone) NOT NULL,   
	updated_by           varchar(100),
	updated_timestamp    timestamp,
	PRIMARY KEY (client_file_id),
	CONSTRAINT FK_CLIENT_FILES_CLIENT foreign key(client_id) references AHUB_DW.CLIENT(client_id)
) distkey(client_id);

INSERT INTO ahub_dw.client_files (client_file_id,client_id,name_pattern,sender_identifier,receiver_identifier,file_type,import_columns) 
VALUES
  (1,1,'ACCDLYINT_TST_BCIINRX_YYMMDDHHMMSS','20500BCIDAHO','00489INGENIORX','INBOUND','processor_routing_identification,record_type,transmission_file_type,version_release_number,sender_identifier,receiver_identifier,submission_number,transaction_response_status,reject_code,record_length,reserved_1,transmission_date,transmission_time,date_of_service,service_provider_identifier_qualifier,service_provider_identifier,document_reference_identifier_qualifier,document_reference_identifier,transmission_identifier,benefit_type,in_network_indicator,formulary_status,accumulator_action_code,sender_reference_number,insurance_code,accumulator_balance_benefit_type,benefit_effective_date,benefit_termination_date,accumulator_change_source_code,transaction_identifier,transaction_identifier_cross_reference,adjustment_reason_code,accumulator_reference_time_stamp,reserved_2,cardholder_identifier,group_identifier,patient_first_name,middle_initial,patient_last_name,patient_relationship_code,date_of_birth,patient_gender_code,patient_state_province_address,cardholder_last_name,carrier_number,contract_number,client_pass_through,family_identifier_number,cardholder_identifier_alternate,group_identifier_alternate,patient_identifier,person_code,reserved_3,accumulator_balance_count,accumulator_specific_category_type,reserved_4,accumulator_1_balance_qualifier,accumulator_1_network_indicator,accumulator_1_applied_amount,accumulator_1_applied_amount_action_code,accumulator_1_benefit_period_amount,accumulator_1_benefit_period_amount_action_code,accumulator_1_remaining_balance,accumulator_1_remaining_balance_action_code,accumulator_2_balance_qualifier,accumulator_2_network_indicator,accumulator_2_applied_amount,accumulator_2_applied_amount_action_code,accumulator_2_benefit_period_amount,accumulator_2_benefit_period_amount_action_code,accumulator_2_remaining_balance,accumulator_2_remaining_balance_action_code,accumulator_3_balance_qualifier,accumulator_3_network_indicator,accumulator_3_applied_amount,accumulator_3_applied_amount_action_code,accumulator_3_benefit_period_amount,accumulator_3_benefit_period_amount_action_code,accumulator_3_remaining_balance,accumulator_3_remaining_balance_action_code,accumulator_4_balance_qualifier,accumulator_4_network_indicator,accumulator_4_applied_amount,accumulator_4_applied_amount_action_code,accumulator_4_benefit_period_amount,accumulator_4_benefit_period_amount_action_code,accumulator_4_remaining_balance,accumulator_4_remaining_balance_action_code,reserved_5,line_number,job_key'),
  (2,1,'ACCDLYINT_TST_INRXBCI_YYMMDDHHMMSS','00489INGENIORX','00990CAREMARK','OUTBOUND',null),
  (3,2,'ACCDLYINT_TST_CVSINRX_MMDDYY.HHMMSS','00990CAREMARK','00489INGENIORX','INBOUND','processor_routing_identification, record_type, transmission_file_type, version_release_number, sender_identifier, receiver_identifier, submission_number, transaction_response_status, reject_code, record_length, reserved_1, transmission_date, transmission_time, date_of_service, service_provider_identifier_qualifier, service_provider_identifier, document_reference_identifier_qualifier, document_reference_identifier, transmission_identifier, benefit_type, in_network_indicator, formulary_status, accumulator_action_code, sender_reference_number, insurance_code, accumulator_balance_benefit_type, benefit_effective_date, benefit_termination_date, accumulator_change_source_code, transaction_identifier, transaction_identifier_cross_reference, adjustment_reason_code, accumulator_reference_time_stamp, reserved_2, cardholder_identifier, group_identifier, patient_first_name, middle_initial, patient_last_name, patient_relationship_code, date_of_birth, patient_gender_code, patient_state_province_address, cardholder_last_name, carrier_number, contract_number, client_pass_through, family_identifier_number, cardholder_identifier_alternate, group_identifier_alternate, patient_identifier, person_code, reserved_3, accumulator_balance_count, accumulator_specific_category_type, reserved_4, accumulator_1_balance_qualifier, accumulator_1_network_indicator, accumulator_1_applied_amount, accumulator_1_applied_amount_action_code, accumulator_1_benefit_period_amount, accumulator_1_benefit_period_amount_action_code, accumulator_1_remaining_balance, accumulator_1_remaining_balance_action_code, accumulator_2_balance_qualifier, accumulator_2_network_indicator, accumulator_2_applied_amount, accumulator_2_applied_amount_action_code, accumulator_2_benefit_period_amount, accumulator_2_benefit_period_amount_action_code, accumulator_2_remaining_balance, accumulator_2_remaining_balance_action_code, accumulator_3_balance_qualifier, accumulator_3_network_indicator, accumulator_3_applied_amount, accumulator_3_applied_amount_action_code, accumulator_3_benefit_period_amount, accumulator_3_benefit_period_amount_action_code, accumulator_3_remaining_balance, accumulator_3_remaining_balance_action_code, accumulator_4_balance_qualifier, accumulator_4_network_indicator, accumulator_4_applied_amount, accumulator_4_applied_amount_action_code, accumulator_4_benefit_period_amount, accumulator_4_benefit_period_amount_action_code, accumulator_4_remaining_balance, accumulator_4_remaining_balance_action_code, accumulator_5_balance_qualifier, accumulator_5_network_indicator, accumulator_5_applied_amount, accumulator_5_applied_amount_action_code, accumulator_5_benefit_period_amount, accumulator_5_benefit_period_amount_action_code, accumulator_5_remaining_balance, accumulator_5_remaining_balance_action_code, accumulator_6_balance_qualifier, accumulator_6_network_indicator, accumulator_6_applied_amount, accumulator_6_applied_amount_action_code, accumulator_6_benefit_period_amount, accumulator_6_benefit_period_amount_action_code, accumulator_6_remaining_balance, accumulator_6_remaining_balance_action_code, reserved_5, accumulator_7_balance_qualifier, accumulator_7_network_indicator, accumulator_7_applied_amount, accumulator_7_applied_amount_action_code, accumulator_7_benefit_period_amount, accumulator_7_benefit_period_amount_action_code, accumulator_7_remaining_balance, accumulator_7_remaining_balance_action_code, accumulator_8_balance_qualifier, accumulator_8_network_indicator, accumulator_8_applied_amount, accumulator_8_applied_amount_action_code, accumulator_8_benefit_period_amount, accumulator_8_benefit_period_amount_action_code, accumulator_8_remaining_balance, accumulator_8_remaining_balance_action_code, accumulator_9_balance_qualifier, accumulator_9_network_indicator, accumulator_9_applied_amount, accumulator_9_applied_amount_action_code, accumulator_9_benefit_period_amount, accumulator_9_benefit_period_amount_action_code, accumulator_9_remaining_balance, accumulator_9_remaining_balance_action_code, accumulator_10_balance_qualifier, accumulator_10_network_indicator, accumulator_10_applied_amount, accumulator_10_applied_amount_action_code, accumulator_10_benefit_period_amount, accumulator_10_benefit_period_amount_action_code, accumulator_10_remaining_balance, accumulator_10_remaining_balance_action_code, accumulator_11_balance_qualifier, accumulator_11_network_indicator, accumulator_11_applied_amount, accumulator_11_applied_amount_action_code, accumulator_11_benefit_period_amount, accumulator_11_benefit_period_amount_action_code, accumulator_11_remaining_balance, accumulator_11_remaining_balance_action_code, accumulator_12_balance_qualifier, accumulator_12_network_indicator, accumulator_12_applied_amount, accumulator_12_applied_amount_action_code, accumulator_12_benefit_period_amount, accumulator_12_benefit_period_amount_action_code, accumulator_12_remaining_balance, accumulator_12_remaining_balance_action_code, optional_data_indicator, total_amount_paid, total_amount_paid_action_code, amount_of_copay, amount_of_copay_action_code, patient_pay_amount, patient_pay_amount_action_code, amount_attributed_to_product_selection_brand, amount_attributed_to_product_selection_brand_action_code, amount_attributed_to_sales_tax, amount_attributed_to_sales_tax_action_code, amount_attributed_to_processor_fee, amount_attributed_to_processor_fee_action_code, gross_amount_due, gross_amount_due_action_code, invoiced_amount, invoiced_amount_action_code, penalty_amount, penalty_amount_action_code, reserved_6, product_service_identifier_qualifier, product_service_identifier, days_supply, quantity_dispensed, product_service_name, brand_generic_indicator, therapeutic_class_code_qualifier, therapeutic_class_code, dispensed_as_written, reserved_7, line_number,job_key'),
  (4,2,'ACCDLYINT_TST_BCI_YYMMDDHHMMSS','00489INGENIORX','20500BCIDAHO','OUTBOUND',null), 
  (5,2,'ACCDLYERR_TST_BCI_YYMMDDHHMMSS','00489INGENIORX','20500BCIDAHO','OUTBOUND',null),
  (6,2,'RECACC_TST_CVSINRX_MMDDYYHHMMSS','00990CAREMARK','00489INGENIORX','INBOUND','record_type,unique_record_identifier,data_file_sender_identifier,data_file_sender_name,cdh_client_identifier,patient_identifier,patient_date_of_birth,patient_first_name,patient_middle_initial,patient_last_name,patient_gender,carrier,account,group_name,accumulation_benefit_begin_date,accumulation_benefit_end_date,accumulator_segment_count,accumulator_1_balance_qualifier,accumulator_1_specific_category_type,accumulator_1_network_indicator,medical_claims_accumulation_1_balance,pharmacy_claims_accumulation_1_balance,total_medical_pharmacy_claims_accumulation_1_balance,accumulator_2_balance_qualifier,accumulator_2_specific_category_type,accumulator_2_network_indicator,medical_claims_accumulation_2_balance,pharmacy_claims_accumulation_2_balance,total_medical_pharmacy_claims_accumulation_2_balance,accumulator_3_balance_qualifier,accumulator_3_specific_category_type,accumulator_3_network_indicator,medical_claims_accumulation_3_balance,pharmacy_claims_accumulation_3_balance,total_medical_pharmacy_claims_accumulation_3_balance,accumulator_4_balance_qualifier,accumulator_4_specific_category_type,accumulator_4_network_indicator,medical_claims_accumulation_4_balance,pharmacy_claims_accumulation_4_balance,total_medical_pharmacy_claims_accumulation_4_balance,accumulator_5_balance_qualifier,accumulator_5_specific_category_type,accumulator_5_network_indicator,medical_claims_accumulation_5_balance,pharmacy_claims_accumulation_5_balance,total_medical_pharmacy_claims_accumulation_5_balance,accumulator_6_balance_qualifier,accumulator_6_specific_category_type,accumulator_6_network_indicator,medical_claims_accumulation_6_balance,pharmacy_claims_accumulation_6_balance,total_medical_pharmacy_claims_accumulation_6_balance,accumulator_7_balance_qualifier,accumulator_7_specific_category_type,accumulator_7_network_indicator,medical_claims_accumulation_7_balance,pharmacy_claims_accumulation_7_balance,total_medical_pharmacy_claims_accumulation_7_balance,accumulator_8_balance_qualifier,accumulator_8_specific_category_type,accumulator_8_network_indicator,medical_claims_accumulation_8_balance,pharmacy_claims_accumulation_8_balance,total_medical_pharmacy_claims_accumulation_8_balance,accumulator_9_balance_qualifier,accumulator_9_specific_category_type,accumulator_9_network_indicator,medical_claims_accumulation_9_balance,pharmacy_claims_accumulation_9_balance,total_medical_pharmacy_claims_accumulation_9_balance,accumulator_10_balance_qualifier,accumulator_10_specific_category_type,accumulator_10_network_indicator,medical_claims_accumulation_10_balance,pharmacy_claims_accumulation_10_balance,total_medical_pharmacy_claims_accumulation_10_balance,accumulator_11_balance_qualifier,accumulator_11_specific_category_type,accumulator_11_network_indicator,medical_claims_accumulation_11_balance,pharmacy_claims_accumulation_11_balance,total_medical_pharmacy_claims_accumulation_11_balance,accumulator_12_balance_qualifier,accumulator_12_specific_category_type,accumulator_12_network_indicator,medical_claims_accumulation_12_balance,pharmacy_claims_accumulation_12_balance,total_medical_pharmacy_claims_accumulation_12_balance,filler_space,line_number,job_key') ,
  (7,1,'ACCDLYERR_TST_BCIINRX_YYMMDDHHMMSS','20500BCIDAHO','00489INGENIORX','INBOUND','processor_routing_identification,record_type,transmission_file_type,version_release_number,sender_identifier,receiver_identifier,submission_number,transaction_response_status,reject_code,record_length,reserved_1,transmission_date,transmission_time,date_of_service,service_provider_identifier_qualifier,service_provider_identifier,document_reference_identifier_qualifier,document_reference_identifier,transmission_identifier,benefit_type,in_network_indicator,formulary_status,accumulator_action_code,sender_reference_number,insurance_code,accumulator_balance_benefit_type,benefit_effective_date,benefit_termination_date,accumulator_change_source_code,transaction_identifier,transaction_identifier_cross_reference,adjustment_reason_code,accumulator_reference_time_stamp,reserved_2,cardholder_identifier,group_identifier,patient_first_name,middle_initial,patient_last_name,patient_relationship_code,date_of_birth,patient_gender_code,patient_state_province_address,cardholder_last_name,carrier_number,contract_number,client_pass_through,family_identifier_number,cardholder_identifier_alternate,group_identifier_alternate,patient_identifier,person_code,reserved_3,accumulator_balance_count,accumulator_specific_category_type,reserved_4,accumulator_1_balance_qualifier,accumulator_1_network_indicator,accumulator_1_applied_amount,accumulator_1_applied_amount_action_code,accumulator_1_benefit_period_amount,accumulator_1_benefit_period_amount_action_code,accumulator_1_remaining_balance,accumulator_1_remaining_balance_action_code,accumulator_2_balance_qualifier,accumulator_2_network_indicator,accumulator_2_applied_amount,accumulator_2_applied_amount_action_code,accumulator_2_benefit_period_amount,accumulator_2_benefit_period_amount_action_code,accumulator_2_remaining_balance,accumulator_2_remaining_balance_action_code,accumulator_3_balance_qualifier,accumulator_3_network_indicator,accumulator_3_applied_amount,accumulator_3_applied_amount_action_code,accumulator_3_benefit_period_amount,accumulator_3_benefit_period_amount_action_code,accumulator_3_remaining_balance,accumulator_3_remaining_balance_action_code,accumulator_4_balance_qualifier,accumulator_4_network_indicator,accumulator_4_applied_amount,accumulator_4_applied_amount_action_code,accumulator_4_benefit_period_amount,accumulator_4_benefit_period_amount_action_code,accumulator_4_remaining_balance,accumulator_4_remaining_balance_action_code,reserved_5,line_number,job_key');

/*==============================================================*/
/* Table: FILE_COLUMNS						    			    */
/*==============================================================*/

CREATE TABLE IF NOT EXISTS ahub_dw.file_columns (
	file_column_id         integer NOT NULL,
	client_file_id         integer NOT NULL,
	file_column_name       varchar(127) NOT NULL,
	column_position        varchar(10) NOT NULL,
	table_column_name      varchar(256) NOT NULL,
	is_accumulator         varchar(1) NOT NULL,
	created_by             varchar(100) DEFAULT 'AHUB ETL'::character varying NOT NULL,
	created_timestamp      timestamp DEFAULT convert_timezone(('US/Eastern'::character varying)::text, ('now'::character varying)::timestamp without time zone) NOT NULL,
	updated_by             varchar(100),
	updated_timestamp      timestamp,
	PRIMARY KEY (file_column_id),
    CONSTRAINT FK_FILE_column_file_CLIENT foreign key(client_file_id) references AHUB_DW.CLIENT_FILES(client_file_id)
) distkey(file_column_id);

--To be fixed
INSERT INTO ahub_dw.file_columns (file_column_id,client_file_id,file_column_name,column_position,table_column_name,is_accumulator) 
VALUES
   (1,1,'Processor Routing Identification','0:200','ACCUMULATOR_DETAIL.PROCESSOR_ROUTING_IDENTIFICATION','N'),
   (2,1,'Record Type ','200:202','ACCUMULATOR_DETAIL.RECORD_TYPE','N'),
   (3,1,'Transmission File Type ','202:204','ACCUMULATOR_DETAIL.TRANSMISSION_FILE_TYPE','N'),
   (5,1,'Sender ID ','206:236','ACCUMULATOR_DETAIL.SENDER_IDENTIFIER','N'),
   (6,1,'Receiver ID ','236:266','ACCUMULATOR_DETAIL.RECEIVER_IDENTIFIER','N'),
   (7,1,'Submission Number ','266:270','ACCUMULATOR_DETAIL.SUBMISSION_NUMBER','N'),
   (8,1,'Transaction Response Status ','270:271','ACCUMULATOR_DETAIL.TRANSACTION_RESPONSE_STATUS','N'),
   (9,1,'Reject Code','271:274','ACCUMULATOR_DETAIL.REJECT_CODE','N'),
   (12,1,'Transmission Date ','299:307','ACCUMULATOR_DETAIL.TRANSMISSION_DATE','N'),
   (13,1,'Transmission Time ','307:315','ACCUMULATOR_DETAIL.TRANSMISSION_TIME','N'),
   (14,1,'Date of Service ','315:323','ACCUMULATOR_DETAIL.DATE_OF_SERVICE','N'),
   (19,1,'Transmission ID ','357:407','ACCUMULATOR_DETAIL.TRANSMISSION_IDENTIFIER','N'),
   (20,1,'Benefit Type ','407:408','ACCUMULATOR_DETAIL.BENEFIT_TYPE','N'),
   (23,1,'Accumulator Action Code ','410:412','ACCUMULATOR_DETAIL.ACCUMULATOR_ACTION_CODE','N'),
   (27,1,'Benefit Effective Date ','463:471','ACCUMULATOR_DETAIL.BENEFIT_EFFECTIVE_DATE','N'),
   (28,1,'Benefit Termination Date ','471:479','ACCUMULATOR_DETAIL.BENEFIT_TERMINATION_DATE','N'),
   (30,1,'Transaction ID','480:510','ACCUMULATOR_DETAIL.TRANSACTION_IDENTIFIER','N'),
   (37,1,'Patient First Name ','615:640','ACCUMULATOR_DETAIL.PATIENT_FIRST_NAME','N'),
   (39,1,'Patient Last Name ','641:676','ACCUMULATOR_DETAIL.PATIENT_LAST_NAME','N'),
   (40,1,'Patient Relationship Code ','676:677','ACCUMULATOR_DETAIL.PATIENT_RELATIONSHIP_CODE','N'),
   (41,1,'Date of Birth ','677:685','ACCUMULATOR_DETAIL.DATE_OF_BIRTH','N'),
   (42,1,'Patient Gender Code ','685:686','ACCUMULATOR_DETAIL.PATIENT_GENDER_CODE','N'),
   (45,1,'Carrier Number ','723:732','ACCUMULATOR_DETAIL.CARRIER_NUMBER','N'),
   (46,1,'Contract Number ','732:747','ACCUMULATOR_DETAIL.CONTRACT_NUMBER','N'),
   (47,1,'Client Pass Through ','747:797','ACCUMULATOR_DETAIL.CLIENT_PASS_THROUGH','N'),
   (51,1,'Patient ID ','852:872','ACCUMULATOR_DETAIL.PATIENT_IDENTIFIER','N'),
   (54,1,'Accumulator Balance Count ','965:967','ACCUMULATOR_DETAIL.ACCUMULATOR_BALANCE_COUNT','N'),
   (55,1,'Accumulator Specific Category Type','967:969','ACCUMULATOR_DETAIL.ACCUMULATOR_SPECIFIC_CATEGORY_TYPE','Y'),
   (57,1,'Accumulator Balance Qualifier 1','989:991','ACCUMULATOR_DETAIL.ACCUMULATOR_1_BALANCE_QUALIFIER','Y'),
   (58,1,'Accumulator Network Indicator 1','991:992','ACCUMULATOR_DETAIL.ACCUMULATOR_1_NETWORK_INDICATOR','Y'),
   (59,1,'Accumulator Applied Amount 1','992:1002','ACCUMULATOR_DETAIL.ACCUMULATOR_1_APPLIED_AMOUNT','Y'),
   (60,1,'Action Code 1','1002:1003','ACCUMULATOR_DETAIL.ACCUMULATOR_1_APPLIED_AMOUNT_ACTION_CODE','Y'),
   (65,1,'Accumulator Balance Qualifier 2','1025:1027','ACCUMULATOR_DETAIL.ACCUMULATOR_2_BALANCE_QUALIFIER','Y'),
   (66,1,'Accumulator Network Indicator 2','1027:1028','ACCUMULATOR_DETAIL.ACCUMULATOR_2_NETWORK_INDICATOR','Y'),
   (67,1,'Accumulator Applied Amount 2','1028:1038','ACCUMULATOR_DETAIL.ACCUMULATOR_2_APPLIED_AMOUNT','Y'),
   (68,1,'Action Code 2','1038:1039','ACCUMULATOR_DETAIL.ACCUMULATOR_2_APPLIED_AMOUNT_ACTION_CODE','Y'),
   (73,1,'Accumulator Balance Qualifier 3','1061:1063','ACCUMULATOR_DETAIL.ACCUMULATOR_3_BALANCE_QUALIFIER','Y'),
   (74,1,'Accumulator Network Indicator 3','1063:1064','ACCUMULATOR_DETAIL.ACCUMULATOR_3_NETWORK_INDICATOR','Y'),
   (75,1,'Accumulator Applied Amount 3','1064:1074','ACCUMULATOR_DETAIL.ACCUMULATOR_3_APPLIED_AMOUNT','Y'),
   (76,1,'Action Code 3','1074:1075','ACCUMULATOR_DETAIL.ACCUMULATOR_3_APPLIED_AMOUNT_ACTION_CODE','Y'),
   (81,1,'Accumulator Balance Qualifier 4','1097:1099','ACCUMULATOR_DETAIL.ACCUMULATOR_4_BALANCE_QUALIFIER','Y'),
   (82,1,'Accumulator Network Indicator 4','1099:1100','ACCUMULATOR_DETAIL.ACCUMULATOR_4_NETWORK_INDICATOR','Y'),
   (83,1,'Accumulator Applied Amount 4','1100:1110','ACCUMULATOR_DETAIL.ACCUMULATOR_4_APPLIED_AMOUNT','Y'),
   (84,1,'Action Code 4','1110:1111','ACCUMULATOR_DETAIL.ACCUMULATOR_4_APPLIED_AMOUNT_ACTION_CODE','Y'),
   (89,1,'Accumulator Balance Qualifier 5','1133:1135','ACCUMULATOR_DETAIL.ACCUMULATOR_5_BALANCE_QUALIFIER','Y'),
   (90,1,'Accumulator Network Indicator 5','1135:1136','ACCUMULATOR_DETAIL.ACCUMULATOR_5_NETWORK_INDICATOR','Y'),
   (91,1,'Accumulator Applied Amount 5','1136:1146','ACCUMULATOR_DETAIL.ACCUMULATOR_5_APPLIED_AMOUNT','Y'),
   (92,1,'Action Code 5','1146:1147','ACCUMULATOR_DETAIL.ACCUMULATOR_5_APPLIED_AMOUNT_ACTION_CODE','Y'),
   (97,1,'Accumulator Balance Qualifier 6','1169:1171','ACCUMULATOR_DETAIL.ACCUMULATOR_6_BALANCE_QUALIFIER','Y'),
   (98,1,'Accumulator Network Indicator 6','1171:1172','ACCUMULATOR_DETAIL.ACCUMULATOR_6_NETWORK_INDICATOR','Y'),
   (99,1,'Accumulator Applied Amount 6','1172:1182','ACCUMULATOR_DETAIL.ACCUMULATOR_6_APPLIED_AMOUNT','Y'),
   (100,1,'Action Code 6','1182:1183','ACCUMULATOR_DETAIL.ACCUMULATOR_6_APPLIED_AMOUNT_ACTION_CODE','Y'),
   (106,1,'Accumulator Balance Qualifier 7','1229:1231','ACCUMULATOR_DETAIL.ACCUMULATOR_7_BALANCE_QUALIFIER','Y'),
   (107,1,'Accumulator Network Indicator 7','1231:1232','ACCUMULATOR_DETAIL.ACCUMULATOR_7_NETWORK_INDICATOR','Y'),
   (108,1,'Accumulator Applied Amount 7','1232:1242','ACCUMULATOR_DETAIL.ACCUMULATOR_7_APPLIED_AMOUNT','Y'),
   (109,1,'Action Code 7','1242:1243','ACCUMULATOR_DETAIL.ACCUMULATOR_7_APPLIED_AMOUNT_ACTION_CODE','Y'),
   (114,1,'Accumulator Balance Qualifier 8','1265:1267','ACCUMULATOR_DETAIL.ACCUMULATOR_8_BALANCE_QUALIFIER','Y'),
   (115,1,'Accumulator Network Indicator 8','1267:1268','ACCUMULATOR_DETAIL.ACCUMULATOR_8_NETWORK_INDICATOR','Y'),
   (116,1,'Accumulator Applied Amount 8','1268:1278','ACCUMULATOR_DETAIL.ACCUMULATOR_8_APPLIED_AMOUNT','Y'),
   (117,1,'Action Code 8','1278:1279','ACCUMULATOR_DETAIL.ACCUMULATOR_8_APPLIED_AMOUNT_ACTION_CODE','Y'),
   (122,1,'Accumulator Balance Qualifier 9','1301:1303','ACCUMULATOR_DETAIL.ACCUMULATOR_9_BALANCE_QUALIFIER','Y'),
   (123,1,'Accumulator Network Indicator 9','1303:1304','ACCUMULATOR_DETAIL.ACCUMULATOR_9_NETWORK_INDICATOR','Y'),
   (124,1,'Accumulator Applied Amount 9','1304:1314','ACCUMULATOR_DETAIL.ACCUMULATOR_9_APPLIED_AMOUNT','Y'),
   (125,1,'Action Code 9','1314:1315','ACCUMULATOR_DETAIL.ACCUMULATOR_9_APPLIED_AMOUNT_ACTION_CODE','Y'),
   (130,1,'Accumulator Balance Qualifier 10','1337:1339','ACCUMULATOR_DETAIL.ACCUMULATOR_10_BALANCE_QUALIFIER','Y'),
   (131,1,'Accumulator Network Indicator 10','1339:1340','ACCUMULATOR_DETAIL.ACCUMULATOR_10_NETWORK_INDICATOR','Y'),
   (132,1,'Accumulator Applied Amount 10','1340:1350','ACCUMULATOR_DETAIL.ACCUMULATOR_10_APPLIED_AMOUNT','Y'),
   (133,1,'Action Code 10','1350:1351','ACCUMULATOR_DETAIL.ACCUMULATOR_10_APPLIED_AMOUNT_ACTION_CODE','Y'),
   (138,1,'Accumulator Balance Qualifier 11','1373:1375','ACCUMULATOR_DETAIL.ACCUMULATOR_11_BALANCE_QUALIFIER','Y'),
   (139,1,'Accumulator Network Indicator 11','1375:1376','ACCUMULATOR_DETAIL.ACCUMULATOR_11_NETWORK_INDICATOR','Y'),
   (140,1,'Accumulator Applied Amount 11','1376:1386','ACCUMULATOR_DETAIL.ACCUMULATOR_11_APPLIED_AMOUNT','Y'),
   (141,1,'Action Code 11','1386:1387','ACCUMULATOR_DETAIL.ACCUMULATOR_11_APPLIED_AMOUNT_ACTION_CODE','Y'),
   (146,1,'Accumulator Balance Qualifier 12','1409:1411','ACCUMULATOR_DETAIL.ACCUMULATOR_12_BALANCE_QUALIFIER','Y'),
   (147,1,'Accumulator Network Indicator 12','1411:1412','ACCUMULATOR_DETAIL.ACCUMULATOR_12_NETWORK_INDICATOR','Y'),
   (148,1,'Accumulator Applied Amount 12','1412:1422','ACCUMULATOR_DETAIL.ACCUMULATOR_12_APPLIED_AMOUNT','Y'),
   (149,1,'Action Code 12','1422:1423','ACCUMULATOR_DETAIL.ACCUMULATOR_12_APPLIED_AMOUNT_ACTION_CODE','Y'),
   (300,7,'Transmission File Type','202:204','ACCUMULATOR_DETAIL.TRANSMISSION_FILE_TYPE','N'),
   (800,7,'Transaction Response Status','270:271','ACCUMULATOR_DETAIL.TRANSACTION_RESPONSE_STATUS','N'),
   (900,7,'Reject Code','271:274','ACCUMULATOR_DETAIL.REJECT_CODE','N');

/*==============================================================*/
/* Table: column_error_codes					    			    */
/*==============================================================*/

  CREATE TABLE IF NOT EXISTS ahub_dw.column_error_codes (
	error_code        integer NOT NULL,
	error_message     varchar(500) NOT NULL,
    created_by        varchar(100) DEFAULT 'AHUB ETL'::character varying NOT NULL,
	created_timestamp timestamp DEFAULT convert_timezone(('US/Eastern'::character varying)::text, ('now'::character varying)::timestamp without time zone) NOT NULL,
	updated_by        varchar(100),
	updated_timestamp timestamp,
	PRIMARY KEY (error_code)
) distkey(error_code);

INSERT INTO ahub_dw.column_error_codes (error_code,error_message) 
VALUES
  (101,'Record Type is not valid'),
  (102,'Unique Record Identifier is blank'),
  (103,'Unique Record Identifier has an invalid format'),
  (104,'Claim Source is not valid'),
  (105,'Transmission File Type is not valid'),
  (106,'Record Response Status Code cannot be blank for Response messages'),
  (108,'Record Counter has an invalid format'),
  (109,'Data File Sender ID does not exist'),
  (111,'Data File Receiver ID does not exist'),
  (113,'Client ID does not exist'),
  (115,'Patient’s Date of Birth not a valid date'),
  (118,'Patient’s Gender is not valid'),
  (119,'Patient’s Relationship Code is not valid'),
  (125,'Claim Transaction Type is not valid'),
  (126,'Claim Date of Service has an invalid format'),
  (127,'Claim Post Date has an invalid format'),
  (128,'Claim Post Time has an invalid format'),
  (137,'Out of Pocket Amount has an invalid format'),
  (147,'In/Out of Network Indicator is not valid'),
  (151,'Record Type is blank'),
  (153,'Transmission Type is blank'),
  (154,'Record Response Status Code is invalid'),
  (155,'Production or Test Data is blank'),
  (156,'Record Counter is blank'),
  (157,'Claim Reject Code has an invalid format '),
  (158,'Data File Sender ID is invalid'),
  (160,'Data File Receiver ID is invalid'),
  (162,'Client ID is blank'),
  (163,'Patient ID is blank'),
  (164,'Patient’s Date of Birth is blank'),
  (165,'Patient’s First Name is blank'),
  (166,'Patient’s Last Name is blank'),
  (167,'Patient’s Gender is blank'),
  (170,'Claim Transaction Type is blank'),
  (171,'Claim Date of Service is blank'),
  (172,'Claim Post Date is blank'),
  (173,'Claim Post Time is blank'),
  (178,'In/Out of Network Indicator is blank'),
  (616,'OON DED amount has an invalid format'),
  (636,'Not Match/Invalid Accum Segment Count'),
  (624,'Accum Value Indicator Is Not Valid'),
  (634,'Missing/Invalid Specific Category Type'),
  (635,'Invalid Accum Balance Qualifier'),
  (152,'Claim Source is blank'),
  (615, 'OON DED amount is blank');

/*==============================================================*/
/* Table: column_rules						    			    */
/*==============================================================*/
  
CREATE TABLE IF NOT EXISTS ahub_dw.column_rules (
	column_rules_id   integer NOT NULL,
	file_column_id    integer NOT NULL,
	priority          integer NOT NULL,
	validation_type   varchar(50) NOT NULL,
	equal_to          varchar(255),
	python_formula    varchar(1000),
	list_of_values    varchar(255),
	error_code        integer NOT NULL,
	error_level       integer,
	is_active         char(1),
	created_by        varchar(100) DEFAULT 'AHUB ETL'::character varying NOT NULL,
	created_timestamp timestamp DEFAULT convert_timezone(('US/Eastern'::character varying)::text, ('now'::character varying)::timestamp without time zone) NOT NULL,
	updated_by        varchar(100),
	updated_timestamp timestamp,
	PRIMARY KEY (column_rules_id),
	CONSTRAINT FK_FILE_column_column_rules foreign key(file_column_id) references AHUB_DW.FILE_COLUMNS(file_column_id),
	CONSTRAINT FK_FILE_column_column_error_codes foreign key(error_code) references AHUB_DW.column_error_codes(error_code)
) distkey(file_column_id);

INSERT INTO ahub_dw.column_rules(column_rules_id, file_column_id, priority, validation_type, equal_to, python_formula, list_of_values, error_code, error_level, is_active) VALUES 
(10, 1, 1, 'REQUIRED', null, null, null, 155, 1, 'Y'),
(20, 2, 1, 'REQUIRED', null, null, null, 151, 1, 'Y'),
(30, 2, 2, 'EQUALTO', 'DT', null, null, 101, 1, 'Y'),
(40, 3, 1, 'REQUIRED', null, null, null, 153, 1, 'Y'),
(50, 3, 2, 'EQUALTO', 'DQ', null, null, 105, 1, 'Y' ),
(60, 5, 1, 'REQUIRED', null, null, null, 109, 1, 'Y'),
(70, 5, 2, 'EQUALTO', '20500BCIDAHO', null, null, 158, 1, 'Y'  ),
(80, 6, 1, 'REQUIRED', null, null, null, 111, 1, 'Y'           ),
(90, 6, 2, 'EQUALTO', '00489INGENIORX', null, null, 160, 1, 'Y'),
(100, 7, 1, 'REQUIRED', null, null, null, 156, 1, 'Y'          ),
(110, 7, 2, 'NUMBER', null, null, null, 108, 1, 'Y'            ),
(120, 8, 1, 'BLANK', 'DQ', 'if ((current_line[202:204]=="DQ") and (len(current_value.strip())>0)):
      error_occured=True
      ', null, 106, 1, 'Y'),
(130, 8, 2, 'DONOTAPPLY', null, 'if ((current_line[202:204]=="DR")):
  if((current_value=="A") or  ( current_value=="R")):
    error_occured=False
  else:
    error_occured=True
else:
  error_occured=False ', null, 154, 1, 'N'            ),
(140, 9, 1, 'BLANK', null, null, null, 157, 1, 'Y'    ),
(150, 12, 1, 'REQUIRED', null, null, null, 172, 1, 'Y'),
(160, 12, 2, 'DATE', null, null, null, 127, 2, 'Y'    ),
(170, 13, 1, 'REQUIRED', null, null, null, 173, 1, 'Y'),
(180, 13, 2, 'TIME', null, null, null, 128, 2, 'Y'    ),
(190, 14, 1, 'REQUIRED', null, null, null, 171, 1, 'Y'),
(200, 14, 2, 'DATE', null, null, null, 126, 1, 'Y'    ),
(220, 19, 1, 'REQUIRED', null, null, null, 102, 1, 'Y'),
(240, 23, 1, 'REQUIRED', null, null, null, 170, 1, 'Y'),
(250, 23, 2, 'RANGE', null, null, '00,11,02,03,04,05,10,20', 125, 1, 'Y'),
(260, 37, 1, 'REQUIRED', null, null, null, 165, 1, 'Y'),
(270, 39, 1, 'REQUIRED', null, null, null, 166, 1, 'Y'),
(290, 41, 1, 'REQUIRED', null, null, null, 164, 1, 'Y'),
(300, 41, 2, 'DATE', null, null, null, 115, 2, 'Y'    ),
(310, 42, 1, 'REQUIRED', null, null, null, 167, 1, 'Y'),
(330, 47, 1, 'REQUIRED', null, null, null, 162, 1, 'Y'),
(340, 47, 2, 'EQUALTO', 'INGENIORXBCI00489', null, null, 113, 1, 'Y'),
(350, 51, 1, 'REQUIRED', null, null, null, 163, 1, 'Y'              ),
(400, 55, 1, 'REQUIRED_AND_RANGE', null, null, '1,3,6,7,8,9,A,D,E,F,I,ZZ', 634, 2, 'Y'),
(410, 57, 1, 'REQUIRED_AND_RANGE', null, null, '02,04,05,06,07,08,14,15', 635, 2, 'Y' ),
(420, 58, 1, 'REQUIRED', null, null, null, 178, 1, 'Y'                               ),
(450, 59, 2, 'NUMBER', null, null, null, 137, 2, 'Y'                                 ),
(460, 60, 1, 'REQUIRED_AND_RANGE', null, null, '+,R', 624, 1, 'Y'                    ),
(470, 65, 1, 'REQUIRED_AND_RANGE', null, null, '02,04,05,06,07,08,14,15', 635, 2, 'Y'),
(480, 66, 1, 'REQUIRED', null, null, null, 178, 1, 'Y'                               ),
(510, 67, 2, 'NUMBER', null, null, null, 137, 2, 'Y'                                 ),
(520, 68, 1, 'REQUIRED_AND_RANGE', null, null, '+,R', 624, 1, 'Y'                    ),
(530, 73, 1, 'REQUIRED_AND_RANGE', null, null, '02,04,05,06,07,08,14,15', 635, 2, 'Y'),
(540, 74, 1, 'REQUIRED', null, null, null, 178, 1, 'Y'                               ),
(570, 75, 2, 'NUMBER', null, null, null, 137, 2, 'Y'                                 ),
(580, 76, 1, 'REQUIRED_AND_RANGE', null, null, '+,R', 624, 1, 'Y'                    ),
(590, 81, 1, 'REQUIRED_AND_RANGE', null, null, '02,04,05,06,07,08,14,15', 635, 2, 'Y'),
(600, 82, 1, 'REQUIRED', null, null, null, 178, 1, 'Y'             ),
(630, 83, 2, 'NUMBER', null, null, null, 137, 2, 'Y'               ),
(640, 84, 1, 'REQUIRED_AND_RANGE', null, null, '+,R'  , 624, 1, 'Y'),
(225, 19, 2, 'EXTERNAL', '', '', '', 103, 1, 'Y'                   ),
(210, 14, 3, 'CUSTOM', null, 'try:
    datetime_object=datetime.strptime(current_value,''%Y%m%d'')
    if (datetime_object >=(datetime.today() + timedelta(days=-670)) and datetime_object <= datetime.today()):
        error_occured=False
    else:
        error_occured=True
except ValueError:
    error_occured=True', null, 126, 1, 'Y'),
(320, 42, 2, 'RANGE', null, null, '0,1,2', 118, 2, 'Y'),
(490, 66, 2, 'RANGE', null, null, '1,2,3', 147, 1, 'Y'),
(550, 74, 2, 'RANGE', null, null, '1,2,3', 147, 1, 'Y'),
(610, 82, 2, 'RANGE', null, null, '1,2,3', 147, 1, 'Y'),
(430, 58, 2, 'RANGE', null, null, '1,2,3', 147, 1, 'Y'),
(230, 20, 1, 'REQUIRED', '', null, '', 152, 1, 'Y'                 ),
(235, 20, 2, 'EQUALTO', '9', '', '', 104, 2, 'Y'                   ),
(280, 40, 1, 'BLANK_OR_RANGE', null, null, '0,1,2,3,4', 119, 2, 'Y'),
(440, 59, 1, 'REQUIRED', null, null, null, 615, 2, 'Y'             ),
(500, 67, 1, 'REQUIRED', null, null, null, 615, 2, 'Y'             ),
(560, 75, 1, 'REQUIRED', null, null, null, 615, 2, 'Y'             ),
(620, 83, 1, 'REQUIRED', null, null, null, 615, 2, 'Y'             ),
(360, 54, 1, 'REQUIRED', null, null, null, 635, 2, 'N'             ),
(370, 54, 2, 'NUMBER', null, null, null, 635, 2, 'N'               ),
(380, 54, 3, 'CUSTOM', null, 'mylist=["989:991","1025:1027","1061:1063","1097:1099","1133:1135","1169:1171","1229:1231","1265:1267","1301:1303","1337:1339","1373:1375","1409:1411"]
count=0
for item in mylist:
  positions = item.split(":")
  start_pos = int(positions[0])
  end_pos=int(positions[1])
  if(len(current_line[start_pos:end_pos].strip())>0):
    count=count+1
if (count==int(current_value)):
  error_occured=False
else:
  error_occured=True', null, 635, 2, 'N'),
(390, 54, 4, 'RANGE', null, null, '01,02,03,04', 635, 2, 'Y'),
(800,300,1,'REQUIRED','DR',NULL,NULL,153,1,'Y'),
(810,300,2,'EQUALTO','DR',NULL,NULL,105,1,'Y'),
(820,800,1,'REQUIRED','R',NULL,NULL,106,1,'Y'),
(830,800,2,'EQUALTO','R',NULL,NULL,154,1,'Y'),
(840,900,1,'REQUIRED','non-empty',NULL,NULL,157,1,'Y');

/*==============================================================*/
/* Table: JOB								    			    */
/*==============================================================*/

CREATE TABLE IF NOT EXISTS ahub_dw.job (
	job_key                         bigint DEFAULT "identity"(186097, 0, '1,1'::text) NOT NULL,
	"name"                          varchar(256) NOT NULL,
	start_timestamp                 timestamp NOT NULL,
	end_timestamp                   timestamp,
	file_name                       varchar(500) NOT NULL,
	file_type                       varchar(50) NOT NULL,
	file_record_count               bigint,
	pre_processing_file_location    varchar(512),
	status                          varchar(256) NOT NULL,
	error_message                   varchar(1024),
	sender_identifier               varchar(256),
	receiver_identifier             varchar(256),
	post_processing_file_location   varchar(512),
	client_id                       integer NOT NULL,
	client_file_id                  integer NOT NULL,
	outbound_file_name              varchar(512),
	outbound_file_generation_complete  boolean default false NOT NULL,
	level1_error_reporting_complete    boolean default false NOT NULL, 
	created_by                      varchar(100) DEFAULT 'AHUB ETL'::character varying NOT NULL,
	created_timestamp               timestamp DEFAULT convert_timezone('US/Eastern'::text, ('now'::text)::timestamp without time zone) NOT NULL,
	updated_by                      varchar(100),
	updated_timestamp               timestamp,
	PRIMARY KEY (job_key)
)
distkey(job_key);

/*==============================================================*/
/* Table: JOB_DETAIL								    	    */
/*==============================================================*/

CREATE TABLE IF NOT EXISTS ahub_dw.job_detail (
	job_detail_key       bigint DEFAULT "identity"(186104, 0, '1,1'::text) NOT NULL,
	job_key              bigint NOT NULL,
	sequence_number      integer NOT NULL,
	"name"               varchar(256) NOT NULL,
	description          varchar(1000),
	start_timestamp      timestamp NOT NULL,
	end_timestamp        timestamp,
	status               varchar(256) NOT NULL,
	error_message        varchar(1024),
	output_file_name     varchar(255),
	input_file_name      varchar(255),
	created_by           varchar(100) DEFAULT 'AHUB ETL'::character varying NOT NULL,
	created_timestamp    timestamp DEFAULT convert_timezone('US/Eastern'::text, ('now'::text)::timestamp without time zone) NOT NULL,
	updated_by           varchar(100),
	updated_timestamp    timestamp,
	PRIMARY KEY (job_detail_key),
	CONSTRAINT FK_job_detail_job foreign key(job_key) references AHUB_DW.JOB(job_key)
)
distkey(job_key)

/*==============================================================*/
/* Table: file_validation_result								    	    */
/*==============================================================*/

CREATE TABLE IF NOT EXISTS ahub_dw.file_validation_result (
	file_validation_result_key  bigint DEFAULT "identity"(194050, 0, ('1,1'::character varying)::text) NOT NULL,
	job_key						bigint NOT NULL,
	line_number                 integer NOT NULL,
	column_rules_id             integer NOT NULL,
	validation_result           char(1) NOT NULL,
	error_message               varchar(500),
	created_by                  varchar(100) DEFAULT 'AHUB ETL'::character varying NOT NULL,
	created_timestamp           timestamp DEFAULT convert_timezone(('US/Eastern'::character varying)::text, ('now'::character varying)::timestamp without time zone) NOT NULL,
	updated_by                  varchar(100),
	updated_timestamp           timestamp,
	PRIMARY KEY (file_validation_result_key),
	CONSTRAINT FK_file_validation_result_job foreign key(job_key) references AHUB_DW.JOB(job_key)
) distkey(job_key)
COMPOUND SORTKEY (JOB_KEY, LINE_NUMBER);

/*==============================================================*/
/* Table: ACCUMULATOR_RECONCILIATION_DETAIL                     */
/*==============================================================*/

CREATE TABLE IF NOT EXISTS AHUB_DW.ACCUMULATOR_RECONCILIATION_DETAIL 
(
  ACCUMULATOR_RECONCILIATION_DETAIL_KEY                     BIGINT IDENTITY(1,1),
  RECORD_TYPE                                               VARCHAR(3),
  UNIQUE_RECORD_IDENTIFIER                                  VARCHAR(50),
  DATA_FILE_SENDER_IDENTIFIER                               VARCHAR(5),
  DATA_FILE_SENDER_NAME                                     VARCHAR(15),
  CDH_CLIENT_IDENTIFIER                                     VARCHAR(30),
  PATIENT_IDENTIFIER                                        VARCHAR(20),
  PATIENT_DATE_OF_BIRTH                                     VARCHAR(8),
  PATIENT_FIRST_NAME                                        VARCHAR(15),
  PATIENT_MIDDLE_INITIAL                                    VARCHAR(1),
  PATIENT_LAST_NAME                                         VARCHAR(20),
  PATIENT_GENDER                                            VARCHAR(1),
  CARRIER                                                   VARCHAR(15),
  ACCOUNT                                                   VARCHAR(15),
  GROUP_NAME                                                VARCHAR(15),
  ACCUMULATION_BENEFIT_BEGIN_DATE                           VARCHAR(8),
  ACCUMULATION_BENEFIT_END_DATE                             VARCHAR(8),
  ACCUMULATOR_SEGMENT_COUNT                                 VARCHAR(2),
  ACCUMULATOR_1_BALANCE_QUALIFIER                           VARCHAR(2),
  ACCUMULATOR_1_SPECIFIC_CATEGORY_TYPE                      VARCHAR(2),
  ACCUMULATOR_1_NETWORK_INDICATOR                           VARCHAR(1),
  MEDICAL_CLAIMS_ACCUMULATION_1_BALANCE                     VARCHAR(11),
  PHARMACY_CLAIMS_ACCUMULATION_1_BALANCE                    VARCHAR(11),
  TOTAL_MEDICAL_PHARMACY_CLAIMS_ACCUMULATION_1_BALANCE      VARCHAR(11),
  ACCUMULATOR_2_BALANCE_QUALIFIER                           VARCHAR(2),
  ACCUMULATOR_2_SPECIFIC_CATEGORY_TYPE                      VARCHAR(2),
  ACCUMULATOR_2_NETWORK_INDICATOR                           VARCHAR(1),
  MEDICAL_CLAIMS_ACCUMULATION_2_BALANCE                     VARCHAR(11),
  PHARMACY_CLAIMS_ACCUMULATION_2_BALANCE                    VARCHAR(11),
  TOTAL_MEDICAL_PHARMACY_CLAIMS_ACCUMULATION_2_BALANCE      VARCHAR(11),
  ACCUMULATOR_3_BALANCE_QUALIFIER                           VARCHAR(2),
  ACCUMULATOR_3_SPECIFIC_CATEGORY_TYPE                      VARCHAR(2),
  ACCUMULATOR_3_NETWORK_INDICATOR                           VARCHAR(1),
  MEDICAL_CLAIMS_ACCUMULATION_3_BALANCE                     VARCHAR(11),
  PHARMACY_CLAIMS_ACCUMULATION_3_BALANCE                    VARCHAR(11),
  TOTAL_MEDICAL_PHARMACY_CLAIMS_ACCUMULATION_3_BALANCE      VARCHAR(11),
  ACCUMULATOR_4_BALANCE_QUALIFIER                           VARCHAR(2),
  ACCUMULATOR_4_SPECIFIC_CATEGORY_TYPE                      VARCHAR(2),
  ACCUMULATOR_4_NETWORK_INDICATOR                           VARCHAR(1),
  MEDICAL_CLAIMS_ACCUMULATION_4_BALANCE                     VARCHAR(11),
  PHARMACY_CLAIMS_ACCUMULATION_4_BALANCE                    VARCHAR(11),
  TOTAL_MEDICAL_PHARMACY_CLAIMS_ACCUMULATION_4_BALANCE      VARCHAR(11),
  ACCUMULATOR_5_BALANCE_QUALIFIER                           VARCHAR(2),
  ACCUMULATOR_5_SPECIFIC_CATEGORY_TYPE                      VARCHAR(2),
  ACCUMULATOR_5_NETWORK_INDICATOR                           VARCHAR(1),
  MEDICAL_CLAIMS_ACCUMULATION_5_BALANCE                     VARCHAR(11),
  PHARMACY_CLAIMS_ACCUMULATION_5_BALANCE                    VARCHAR(11),
  TOTAL_MEDICAL_PHARMACY_CLAIMS_ACCUMULATION_5_BALANCE      VARCHAR(11),
  ACCUMULATOR_6_BALANCE_QUALIFIER                           VARCHAR(2),
  ACCUMULATOR_6_SPECIFIC_CATEGORY_TYPE                      VARCHAR(2),
  ACCUMULATOR_6_NETWORK_INDICATOR                           VARCHAR(1),
  MEDICAL_CLAIMS_ACCUMULATION_6_BALANCE                     VARCHAR(11),
  PHARMACY_CLAIMS_ACCUMULATION_6_BALANCE                    VARCHAR(11),
  TOTAL_MEDICAL_PHARMACY_CLAIMS_ACCUMULATION_6_BALANCE      VARCHAR(11),
  ACCUMULATOR_7_BALANCE_QUALIFIER                           VARCHAR(2),
  ACCUMULATOR_7_SPECIFIC_CATEGORY_TYPE                      VARCHAR(2),
  ACCUMULATOR_7_NETWORK_INDICATOR                           VARCHAR(1),
  MEDICAL_CLAIMS_ACCUMULATION_7_BALANCE                     VARCHAR(11),
  PHARMACY_CLAIMS_ACCUMULATION_7_BALANCE                    VARCHAR(11),
  TOTAL_MEDICAL_PHARMACY_CLAIMS_ACCUMULATION_7_BALANCE      VARCHAR(11),
  ACCUMULATOR_8_BALANCE_QUALIFIER                           VARCHAR(2),
  ACCUMULATOR_8_SPECIFIC_CATEGORY_TYPE                      VARCHAR(2),
  ACCUMULATOR_8_NETWORK_INDICATOR                           VARCHAR(1),
  MEDICAL_CLAIMS_ACCUMULATION_8_BALANCE                     VARCHAR(11),
  PHARMACY_CLAIMS_ACCUMULATION_8_BALANCE                    VARCHAR(11),
  TOTAL_MEDICAL_PHARMACY_CLAIMS_ACCUMULATION_8_BALANCE      VARCHAR(11),
  ACCUMULATOR_9_BALANCE_QUALIFIER                           VARCHAR(2),
  ACCUMULATOR_9_SPECIFIC_CATEGORY_TYPE                      VARCHAR(2),
  ACCUMULATOR_9_NETWORK_INDICATOR                           VARCHAR(1),
  MEDICAL_CLAIMS_ACCUMULATION_9_BALANCE                     VARCHAR(11),
  PHARMACY_CLAIMS_ACCUMULATION_9_BALANCE                    VARCHAR(11),
  TOTAL_MEDICAL_PHARMACY_CLAIMS_ACCUMULATION_9_BALANCE      VARCHAR(11),
  ACCUMULATOR_10_BALANCE_QUALIFIER                          VARCHAR(2),
  ACCUMULATOR_10_SPECIFIC_CATEGORY_TYPE                     VARCHAR(2),
  ACCUMULATOR_10_NETWORK_INDICATOR                          VARCHAR(1),
  MEDICAL_CLAIMS_ACCUMULATION_10_BALANCE                    VARCHAR(11),
  PHARMACY_CLAIMS_ACCUMULATION_10_BALANCE                   VARCHAR(11),
  TOTAL_MEDICAL_PHARMACY_CLAIMS_ACCUMULATION_10_BALANCE     VARCHAR(11),
  ACCUMULATOR_11_BALANCE_QUALIFIER                          VARCHAR(2),
  ACCUMULATOR_11_SPECIFIC_CATEGORY_TYPE                     VARCHAR(2),
  ACCUMULATOR_11_NETWORK_INDICATOR                          VARCHAR(1),
  MEDICAL_CLAIMS_ACCUMULATION_11_BALANCE                    VARCHAR(11),
  PHARMACY_CLAIMS_ACCUMULATION_11_BALANCE                   VARCHAR(11),
  TOTAL_MEDICAL_PHARMACY_CLAIMS_ACCUMULATION_11_BALANCE     VARCHAR(11),
  ACCUMULATOR_12_BALANCE_QUALIFIER                          VARCHAR(2),
  ACCUMULATOR_12_SPECIFIC_CATEGORY_TYPE                     VARCHAR(2),
  ACCUMULATOR_12_NETWORK_INDICATOR                          VARCHAR(1),
  MEDICAL_CLAIMS_ACCUMULATION_12_BALANCE                    VARCHAR(11),
  PHARMACY_CLAIMS_ACCUMULATION_12_BALANCE                   VARCHAR(11),
  TOTAL_MEDICAL_PHARMACY_CLAIMS_ACCUMULATION_12_BALANCE     VARCHAR(11),
  FILLER_SPACE                                              VARCHAR(113),
  LINE_NUMBER                                               INTEGER not null,
  JOB_KEY                                                   BIGINT not null,
  CREATED_BY                                                VARCHAR(100) default 'AHUB ETL' not null,
  CREATED_TIMESTAMP                                         TIMESTAMP default CONVERT_TIMEZONE ( 'US/Eastern', sysdate) not null,
  UPDATE_BY                                                 VARCHAR(100),
  UPDATE_TIMESTAMP                                          TIMESTAMP,
CONSTRAINT IDX_PK_ACCUMULATOR_RECONCILIATION_DETAIL primary key (ACCUMULATOR_RECONCILIATION_DETAIL_KEY)
)DISTSTYLE KEY
DISTKEY (ACCUMULATOR_RECONCILIATION_DETAIL_KEY)
COMPOUND SORTKEY(JOB_KEY,LINE_NUMBER);
;

/*==============================================================*/
/* Table: ACCUMULATOR_DETAIL									*/
/*==============================================================*/

CREATE TABLE IF NOT EXISTS AHUB_DW.ACCUMULATOR_DETAIL 
(
  ACCUMULATOR_DETAIL_KEY                                    BIGINT IDENTITY(1,1) NOT NULL ,
  PROCESSOR_ROUTING_IDENTIFICATION                          VARCHAR(200)  ,
  RECORD_TYPE                                               VARCHAR(2)  ,
  TRANSMISSION_FILE_TYPE                                    VARCHAR(2)  ,
  VERSION_RELEASE_NUMBER                                    VARCHAR(2)  ,
  SENDER_IDENTIFIER                                         VARCHAR(30)  ,
  RECEIVER_IDENTIFIER                                       VARCHAR(30)  ,
  SUBMISSION_NUMBER                                         VARCHAR(4)  ,
  TRANSACTION_RESPONSE_STATUS                               VARCHAR(1)  ,
  REJECT_CODE                                               VARCHAR(3)  ,
  RECORD_LENGTH                                             VARCHAR(5)  ,
  RESERVED_1                                                VARCHAR(20)  ,
  TRANSMISSION_DATE                                         VARCHAR(8)  ,
  TRANSMISSION_TIME                                         VARCHAR(8)  ,
  DATE_OF_SERVICE                                           VARCHAR(8)  ,
  SERVICE_PROVIDER_IDENTIFIER_QUALIFIER                     VARCHAR(2)  ,
  SERVICE_PROVIDER_IDENTIFIER                               VARCHAR(15)  ,
  DOCUMENT_REFERENCE_IDENTIFIER_QUALIFIER                   VARCHAR(2)  ,
  DOCUMENT_REFERENCE_IDENTIFIER                             VARCHAR(15)  ,
  TRANSMISSION_IDENTIFIER                                   VARCHAR(50)  ,
  BENEFIT_TYPE                                              VARCHAR(1)  ,
  IN_NETWORK_INDICATOR                                      VARCHAR(1)  ,
  FORMULARY_STATUS                                          VARCHAR(1)  ,
  ACCUMULATOR_ACTION_CODE                                   VARCHAR(2)  ,
  SENDER_REFERENCE_NUMBER                                   VARCHAR(30)  ,
  INSURANCE_CODE                                            VARCHAR(20)  ,
  ACCUMULATOR_BALANCE_BENEFIT_TYPE                          VARCHAR(1)  ,
  BENEFIT_EFFECTIVE_DATE                                    VARCHAR(8)  ,
  BENEFIT_TERMINATION_DATE                                  VARCHAR(8)  ,
  ACCUMULATOR_CHANGE_SOURCE_CODE                            VARCHAR(1)  ,
  TRANSACTION_IDENTIFIER                                    VARCHAR(30)  ,
  TRANSACTION_IDENTIFIER_CROSS_REFERENCE                    VARCHAR(30)  ,
  ADJUSTMENT_REASON_CODE                                    VARCHAR(1)  ,
  ACCUMULATOR_REFERENCE_TIME_STAMP                          VARCHAR(26)  ,
  RESERVED_2                                                VARCHAR(13)  ,
  CARDHOLDER_IDENTIFIER                                     VARCHAR(20)  ,
  GROUP_IDENTIFIER                                          VARCHAR(15)  ,
  PATIENT_FIRST_NAME                                        VARCHAR(25)  ,
  MIDDLE_INITIAL                                            VARCHAR(1)  ,
  PATIENT_LAST_NAME                                         VARCHAR(35)  ,
  PATIENT_RELATIONSHIP_CODE                                 VARCHAR(1)  ,
  DATE_OF_BIRTH                                             VARCHAR(8)  ,
  PATIENT_GENDER_CODE                                       VARCHAR(1)  ,
  PATIENT_STATE_PROVINCE_ADDRESS                            VARCHAR(2)  ,
  CARDHOLDER_LAST_NAME                                      VARCHAR(35)  ,
  CARRIER_NUMBER                                            VARCHAR(9)  ,
  CONTRACT_NUMBER                                           VARCHAR(15)  ,
  CLIENT_PASS_THROUGH                                       VARCHAR(50)  ,
  FAMILY_IDENTIFIER_NUMBER                                  VARCHAR(20)  ,
  CARDHOLDER_IDENTIFIER_ALTERNATE                           VARCHAR(20)  ,
  GROUP_IDENTIFIER_ALTERNATE                                VARCHAR(15)  ,
  PATIENT_IDENTIFIER                                        VARCHAR(20)  ,
  PERSON_CODE                                               VARCHAR(3)  ,
  RESERVED_3                                                VARCHAR(90)  ,
  ACCUMULATOR_BALANCE_COUNT                                 VARCHAR(2)  ,
  ACCUMULATOR_SPECIFIC_CATEGORY_TYPE                        VARCHAR(2)  ,
  RESERVED_4                                                VARCHAR(20)  ,
  ACCUMULATOR_1_BALANCE_QUALIFIER                           VARCHAR(2)  ,
  ACCUMULATOR_1_NETWORK_INDICATOR                           VARCHAR(1)  ,
  ACCUMULATOR_1_APPLIED_AMOUNT                              VARCHAR(10)  ,
  ACCUMULATOR_1_APPLIED_AMOUNT_ACTION_CODE                  VARCHAR(1)  ,
  ACCUMULATOR_1_BENEFIT_PERIOD_AMOUNT                       VARCHAR(10)  ,
  ACCUMULATOR_1_BENEFIT_PERIOD_AMOUNT_ACTION_CODE           VARCHAR(1)  ,
  ACCUMULATOR_1_REMAINING_BALANCE                           VARCHAR(10)  ,
  ACCUMULATOR_1_REMAINING_BALANCE_ACTION_CODE               VARCHAR(1)  ,
  ACCUMULATOR_2_BALANCE_QUALIFIER                           VARCHAR(2)  ,
  ACCUMULATOR_2_NETWORK_INDICATOR                           VARCHAR(1)  ,
  ACCUMULATOR_2_APPLIED_AMOUNT                              VARCHAR(10)  ,
  ACCUMULATOR_2_APPLIED_AMOUNT_ACTION_CODE                  VARCHAR(1)  ,
  ACCUMULATOR_2_BENEFIT_PERIOD_AMOUNT                       VARCHAR(10)  ,
  ACCUMULATOR_2_BENEFIT_PERIOD_AMOUNT_ACTION_CODE           VARCHAR(1)  ,
  ACCUMULATOR_2_REMAINING_BALANCE                           VARCHAR(10)  ,
  ACCUMULATOR_2_REMAINING_BALANCE_ACTION_CODE               VARCHAR(1)  ,
  ACCUMULATOR_3_BALANCE_QUALIFIER                           VARCHAR(2)  ,
  ACCUMULATOR_3_NETWORK_INDICATOR                           VARCHAR(1)  ,
  ACCUMULATOR_3_APPLIED_AMOUNT                              VARCHAR(10)  ,
  ACCUMULATOR_3_APPLIED_AMOUNT_ACTION_CODE                  VARCHAR(1)  ,
  ACCUMULATOR_3_BENEFIT_PERIOD_AMOUNT                       VARCHAR(10)  ,
  ACCUMULATOR_3_BENEFIT_PERIOD_AMOUNT_ACTION_CODE           VARCHAR(1)  ,
  ACCUMULATOR_3_REMAINING_BALANCE                           VARCHAR(10)  ,
  ACCUMULATOR_3_REMAINING_BALANCE_ACTION_CODE               VARCHAR(1)  ,
  ACCUMULATOR_4_BALANCE_QUALIFIER                           VARCHAR(2)  ,
  ACCUMULATOR_4_NETWORK_INDICATOR                           VARCHAR(1)  ,
  ACCUMULATOR_4_APPLIED_AMOUNT                              VARCHAR(10)  ,
  ACCUMULATOR_4_APPLIED_AMOUNT_ACTION_CODE                  VARCHAR(1)  ,
  ACCUMULATOR_4_BENEFIT_PERIOD_AMOUNT                       VARCHAR(10)  ,
  ACCUMULATOR_4_BENEFIT_PERIOD_AMOUNT_ACTION_CODE           VARCHAR(1)  ,
  ACCUMULATOR_4_REMAINING_BALANCE                           VARCHAR(10)  ,
  ACCUMULATOR_4_REMAINING_BALANCE_ACTION_CODE               VARCHAR(1)  ,
  ACCUMULATOR_5_BALANCE_QUALIFIER                           VARCHAR(2)  ,
  ACCUMULATOR_5_NETWORK_INDICATOR                           VARCHAR(1)  ,
  ACCUMULATOR_5_APPLIED_AMOUNT                              VARCHAR(10)  ,
  ACCUMULATOR_5_APPLIED_AMOUNT_ACTION_CODE                  VARCHAR(1)  ,
  ACCUMULATOR_5_BENEFIT_PERIOD_AMOUNT                       VARCHAR(10)  ,
  ACCUMULATOR_5_BENEFIT_PERIOD_AMOUNT_ACTION_CODE           VARCHAR(1)  ,
  ACCUMULATOR_5_REMAINING_BALANCE                           VARCHAR(10)  ,
  ACCUMULATOR_5_REMAINING_BALANCE_ACTION_CODE               VARCHAR(1)  ,
  ACCUMULATOR_6_BALANCE_QUALIFIER                           VARCHAR(2)  ,
  ACCUMULATOR_6_NETWORK_INDICATOR                           VARCHAR(1)  ,
  ACCUMULATOR_6_APPLIED_AMOUNT                              VARCHAR(10)  ,
  ACCUMULATOR_6_APPLIED_AMOUNT_ACTION_CODE                  VARCHAR(1)  ,
  ACCUMULATOR_6_BENEFIT_PERIOD_AMOUNT                       VARCHAR(10)  ,
  ACCUMULATOR_6_BENEFIT_PERIOD_AMOUNT_ACTION_CODE           VARCHAR(1)  ,
  ACCUMULATOR_6_REMAINING_BALANCE                           VARCHAR(10)  ,
  ACCUMULATOR_6_REMAINING_BALANCE_ACTION_CODE               VARCHAR(1)  ,
  RESERVED_5                                                VARCHAR(570)  ,
  ACCUMULATOR_7_BALANCE_QUALIFIER                           VARCHAR(2)  ,
  ACCUMULATOR_7_NETWORK_INDICATOR                           VARCHAR(1)  ,
  ACCUMULATOR_7_APPLIED_AMOUNT                              VARCHAR(10)  ,
  ACCUMULATOR_7_APPLIED_AMOUNT_ACTION_CODE                  VARCHAR(1)  ,
  ACCUMULATOR_7_BENEFIT_PERIOD_AMOUNT                       VARCHAR(10)  ,
  ACCUMULATOR_7_BENEFIT_PERIOD_AMOUNT_ACTION_CODE           VARCHAR(1)  ,
  ACCUMULATOR_7_REMAINING_BALANCE                           VARCHAR(10)  ,
  ACCUMULATOR_7_REMAINING_BALANCE_ACTION_CODE               VARCHAR(1)  ,
  ACCUMULATOR_8_BALANCE_QUALIFIER                           VARCHAR(2)  ,
  ACCUMULATOR_8_NETWORK_INDICATOR                           VARCHAR(1)  ,
  ACCUMULATOR_8_APPLIED_AMOUNT                              VARCHAR(10)  ,
  ACCUMULATOR_8_APPLIED_AMOUNT_ACTION_CODE                  VARCHAR(1)  ,
  ACCUMULATOR_8_BENEFIT_PERIOD_AMOUNT                       VARCHAR(10)  ,
  ACCUMULATOR_8_BENEFIT_PERIOD_AMOUNT_ACTION_CODE           VARCHAR(1)  ,
  ACCUMULATOR_8_REMAINING_BALANCE                           VARCHAR(10)  ,
  ACCUMULATOR_8_REMAINING_BALANCE_ACTION_CODE               VARCHAR(1)  ,
  ACCUMULATOR_9_BALANCE_QUALIFIER                           VARCHAR(2)  ,
  ACCUMULATOR_9_NETWORK_INDICATOR                           VARCHAR(1)  ,
  ACCUMULATOR_9_APPLIED_AMOUNT                              VARCHAR(10)  ,
  ACCUMULATOR_9_APPLIED_AMOUNT_ACTION_CODE                  VARCHAR(1)  ,
  ACCUMULATOR_9_BENEFIT_PERIOD_AMOUNT                       VARCHAR(10)  ,
  ACCUMULATOR_9_BENEFIT_PERIOD_AMOUNT_ACTION_CODE           VARCHAR(1)  ,
  ACCUMULATOR_9_REMAINING_BALANCE                           VARCHAR(10)  ,
  ACCUMULATOR_9_REMAINING_BALANCE_ACTION_CODE               VARCHAR(1)  ,
  ACCUMULATOR_10_BALANCE_QUALIFIER                          VARCHAR(2)  ,
  ACCUMULATOR_10_NETWORK_INDICATOR                          VARCHAR(1)  ,
  ACCUMULATOR_10_APPLIED_AMOUNT                             VARCHAR(10)  ,
  ACCUMULATOR_10_APPLIED_AMOUNT_ACTION_CODE                 VARCHAR(1)  ,
  ACCUMULATOR_10_BENEFIT_PERIOD_AMOUNT                      VARCHAR(10)  ,
  ACCUMULATOR_10_BENEFIT_PERIOD_AMOUNT_ACTION_CODE          VARCHAR(1)  ,
  ACCUMULATOR_10_REMAINING_BALANCE                          VARCHAR(10)  ,
  ACCUMULATOR_10_REMAINING_BALANCE_ACTION_CODE              VARCHAR(1)  ,
  ACCUMULATOR_11_BALANCE_QUALIFIER                          VARCHAR(2)  ,
  ACCUMULATOR_11_NETWORK_INDICATOR                          VARCHAR(1)  ,
  ACCUMULATOR_11_APPLIED_AMOUNT                             VARCHAR(10)  ,
  ACCUMULATOR_11_APPLIED_AMOUNT_ACTION_CODE                 VARCHAR(1)  ,
  ACCUMULATOR_11_BENEFIT_PERIOD_AMOUNT                      VARCHAR(10)  ,
  ACCUMULATOR_11_BENEFIT_PERIOD_AMOUNT_ACTION_CODE          VARCHAR(1)  ,
  ACCUMULATOR_11_REMAINING_BALANCE                          VARCHAR(10)  ,
  ACCUMULATOR_11_REMAINING_BALANCE_ACTION_CODE              VARCHAR(1)  ,
  ACCUMULATOR_12_BALANCE_QUALIFIER                          VARCHAR(2)  ,
  ACCUMULATOR_12_NETWORK_INDICATOR                          VARCHAR(1)  ,
  ACCUMULATOR_12_APPLIED_AMOUNT                             VARCHAR(10)  ,
  ACCUMULATOR_12_APPLIED_AMOUNT_ACTION_CODE                 VARCHAR(1)  ,
  ACCUMULATOR_12_BENEFIT_PERIOD_AMOUNT                      VARCHAR(10)  ,
  ACCUMULATOR_12_BENEFIT_PERIOD_AMOUNT_ACTION_CODE          VARCHAR(1)  ,
  ACCUMULATOR_12_REMAINING_BALANCE                          VARCHAR(10)  ,
  ACCUMULATOR_12_REMAINING_BALANCE_ACTION_CODE              VARCHAR(1)  ,
  OPTIONAL_DATA_INDICATOR                                   VARCHAR(1)  ,
  TOTAL_AMOUNT_PAID                                         VARCHAR(10)  ,
  TOTAL_AMOUNT_PAID_ACTION_CODE                             VARCHAR(1)  ,
  AMOUNT_OF_COPAY                                           VARCHAR(10)  ,
  AMOUNT_OF_COPAY_ACTION_CODE                               VARCHAR(1)  ,
  PATIENT_PAY_AMOUNT                                        VARCHAR(10)  ,
  PATIENT_PAY_AMOUNT_ACTION_CODE                            VARCHAR(1)  ,
  AMOUNT_ATTRIBUTED_TO_PRODUCT_SELECTION_BRAND              VARCHAR(10)  ,
  AMOUNT_ATTRIBUTED_TO_PRODUCT_SELECTION_BRAND_ACTION_CODE  VARCHAR(1)  ,
  AMOUNT_ATTRIBUTED_TO_SALES_TAX                            VARCHAR(10)  ,
  AMOUNT_ATTRIBUTED_TO_SALES_TAX_ACTION_CODE                VARCHAR(1)  ,
  AMOUNT_ATTRIBUTED_TO_PROCESSOR_FEE                        VARCHAR(10)  ,
  AMOUNT_ATTRIBUTED_TO_PROCESSOR_FEE_ACTION_CODE            VARCHAR(1)  ,
  GROSS_AMOUNT_DUE                                          VARCHAR(10)  ,
  GROSS_AMOUNT_DUE_ACTION_CODE                              VARCHAR(1)  ,
  INVOICED_AMOUNT                                           VARCHAR(10)  ,
  INVOICED_AMOUNT_ACTION_CODE                               VARCHAR(1)  ,
  PENALTY_AMOUNT                                            VARCHAR(10)  ,
  PENALTY_AMOUNT_ACTION_CODE                                VARCHAR(1)  ,
  RESERVED_6                                                VARCHAR(23)  ,
  PRODUCT_SERVICE_IDENTIFIER_QUALIFIER                      VARCHAR(2)  ,
  PRODUCT_SERVICE_IDENTIFIER                                VARCHAR(19)  ,
  DAYS_SUPPLY                                               VARCHAR(3)  ,
  QUANTITY_DISPENSED                                        VARCHAR(10)  ,
  PRODUCT_SERVICE_NAME                                      VARCHAR(30)  ,
  BRAND_GENERIC_INDICATOR                                   VARCHAR(1)  ,
  THERAPEUTIC_CLASS_CODE_QUALIFIER                          VARCHAR(1)  ,
  THERAPEUTIC_CLASS_CODE                                    VARCHAR(17)  ,
  DISPENSED_AS_WRITTEN                                      VARCHAR(1)  ,
  RESERVED_7                                                VARCHAR(48)  ,
  LINE_NUMBER                                               INTEGER not null,
  JOB_KEY                                                   BIGINT not null,
  CREATED_BY                                                VARCHAR(100) default 'AHUB ETL' not null,
  CREATED_TIMESTAMP                                         TIMESTAMP default CONVERT_TIMEZONE ( 'US/Eastern', sysdate) not null,
  UPDATED_BY VARCHAR(100) ,
  UPDATED_TIMESTAMP TIMESTAMP,
CONSTRAINT IDX_PK_ACCUMULATOR_DETAIL PRIMARY KEY (ACCUMULATOR_DETAIL_KEY)
)
DISTSTYLE KEY
DISTKEY (ACCUMULATOR_DETAIL_KEY)
COMPOUND SORTKEY (JOB_KEY, LINE_NUMBER);

/*==============================================================*/
/* View: VW_ACCUMULATOR_DETAIL_DETOKENIZED  					*/
/*==============================================================*/

CREATE OR REPLACE VIEW AHUB_DW.VW_ACCUMULATOR_DETAIL_DETOKENIZED AS (
SELECT 
 DETAIL.JOB_KEY  
,DETAIL.LINE_NUMBER                                              
,DETAIL.ACCUMULATOR_DETAIL_KEY                                  
,DETAIL.PROCESSOR_ROUTING_IDENTIFICATION                        
,DETAIL.RECORD_TYPE                                             
,DETAIL.TRANSMISSION_FILE_TYPE                                  
,DETAIL.VERSION_RELEASE_NUMBER                                  
,DETAIL.SENDER_IDENTIFIER                                       
,DETAIL.RECEIVER_IDENTIFIER                                     
,DETAIL.SUBMISSION_NUMBER                                       
,DETAIL.TRANSACTION_RESPONSE_STATUS                             
,DETAIL.REJECT_CODE                                             
,DETAIL.RECORD_LENGTH                                           
,DETAIL.RESERVED_1                                              
,DETAIL.TRANSMISSION_DATE                                       
,DETAIL.TRANSMISSION_TIME                                       
,DETAIL.DATE_OF_SERVICE                                           
,DETAIL.SERVICE_PROVIDER_IDENTIFIER_QUALIFIER                   
,DETAIL.SERVICE_PROVIDER_IDENTIFIER                             
,DETAIL.DOCUMENT_REFERENCE_IDENTIFIER_QUALIFIER                 
,DETAIL.DOCUMENT_REFERENCE_IDENTIFIER                           
,DETAIL.TRANSMISSION_IDENTIFIER                                 
,DETAIL.BENEFIT_TYPE                                            
,DETAIL.IN_NETWORK_INDICATOR                                    
,DETAIL.FORMULARY_STATUS                                        
,DETAIL.ACCUMULATOR_ACTION_CODE                                 
,DETAIL.SENDER_REFERENCE_NUMBER                                 
,DETAIL.INSURANCE_CODE                                          
,DETAIL.ACCUMULATOR_BALANCE_BENEFIT_TYPE                        
,DETAIL.BENEFIT_EFFECTIVE_DATE                                  
,DETAIL.BENEFIT_TERMINATION_DATE                                
,DETAIL.ACCUMULATOR_CHANGE_SOURCE_CODE                          
,DETAIL.TRANSACTION_IDENTIFIER                                  
,DETAIL.TRANSACTION_IDENTIFIER_CROSS_REFERENCE                  
,DETAIL.ADJUSTMENT_REASON_CODE                                  
,DETAIL.ACCUMULATOR_REFERENCE_TIME_STAMP                        
,DETAIL.RESERVED_2      
,DETAIL.CARDHOLDER_IDENTIFIER                                        
,concat(concat('~`I', trim(DETAIL.CARDHOLDER_IDENTIFIER)), 'I`~')  CARDHOLDER_IDENTIFIER_PHI                                   
,DETAIL.GROUP_IDENTIFIER              
,DETAIL.PATIENT_FIRST_NAME                          
,concat(concat('~`N', trim(DETAIL.PATIENT_FIRST_NAME)), 'N`~') PATIENT_FIRST_NAME_PHI
,DETAIL.MIDDLE_INITIAL                                     
,concat(concat('~`N', trim(DETAIL.MIDDLE_INITIAL)), 'N`~')  MIDDLE_INITIAL_PHI
,DETAIL.PATIENT_LAST_NAME                                          
,concat(concat('~`N', trim(DETAIL.PATIENT_LAST_NAME)), 'N`~') PATIENT_LAST_NAME_PHI                                       
,DETAIL.PATIENT_RELATIONSHIP_CODE        
,DETAIL.DATE_OF_BIRTH                       
,concat(concat('~`D', trim(DETAIL.DATE_OF_BIRTH)), 'D`~') DATE_OF_BIRTH_PHI                                           
,DETAIL.PATIENT_GENDER_CODE                                     
,DETAIL.PATIENT_STATE_PROVINCE_ADDRESS 
,DETAIL.CARDHOLDER_LAST_NAME                         
,concat(concat('~`N', trim(DETAIL.CARDHOLDER_LAST_NAME)), 'N`~') CARDHOLDER_LAST_NAME_PHI                                    
,DETAIL.CARRIER_NUMBER                                          
,DETAIL.CONTRACT_NUMBER                                         
,DETAIL.CLIENT_PASS_THROUGH               
,DETAIL.FAMILY_IDENTIFIER_NUMBER                      
,concat(concat('~`I', trim(DETAIL.FAMILY_IDENTIFIER_NUMBER)), 'I`~')  FAMILY_IDENTIFIER_NUMBER_PHI
,DETAIL.CARDHOLDER_IDENTIFIER_ALTERNATE                                
,concat(concat('~`I', trim(DETAIL.CARDHOLDER_IDENTIFIER_ALTERNATE)), 'I`~')  CARDHOLDER_IDENTIFIER_ALTERNATE_PHI                         
,DETAIL.GROUP_IDENTIFIER_ALTERNATE 
,DETAIL.PATIENT_IDENTIFIER                             
,concat(concat('~`I', trim(DETAIL.PATIENT_IDENTIFIER)), 'I`~')  PATIENT_IDENTIFIER_PHI                                      
,DETAIL.PERSON_CODE                                             
,DETAIL.RESERVED_3                                              
,DETAIL.ACCUMULATOR_BALANCE_COUNT                               
,DETAIL.ACCUMULATOR_SPECIFIC_CATEGORY_TYPE                      
,DETAIL.RESERVED_4                                              
,DETAIL.ACCUMULATOR_1_BALANCE_QUALIFIER                         
,DETAIL.ACCUMULATOR_1_NETWORK_INDICATOR                         
,DETAIL.ACCUMULATOR_1_APPLIED_AMOUNT                            
,DETAIL.ACCUMULATOR_1_APPLIED_AMOUNT_ACTION_CODE                
,DETAIL.ACCUMULATOR_1_BENEFIT_PERIOD_AMOUNT                     
,DETAIL.ACCUMULATOR_1_BENEFIT_PERIOD_AMOUNT_ACTION_CODE         
,DETAIL.ACCUMULATOR_1_REMAINING_BALANCE                         
,DETAIL.ACCUMULATOR_1_REMAINING_BALANCE_ACTION_CODE             
,DETAIL.ACCUMULATOR_2_BALANCE_QUALIFIER                         
,DETAIL.ACCUMULATOR_2_NETWORK_INDICATOR                         
,DETAIL.ACCUMULATOR_2_APPLIED_AMOUNT                            
,DETAIL.ACCUMULATOR_2_APPLIED_AMOUNT_ACTION_CODE                
,DETAIL.ACCUMULATOR_2_BENEFIT_PERIOD_AMOUNT                     
,DETAIL.ACCUMULATOR_2_BENEFIT_PERIOD_AMOUNT_ACTION_CODE         
,DETAIL.ACCUMULATOR_2_REMAINING_BALANCE                         
,DETAIL.ACCUMULATOR_2_REMAINING_BALANCE_ACTION_CODE             
,DETAIL.ACCUMULATOR_3_BALANCE_QUALIFIER                         
,DETAIL.ACCUMULATOR_3_NETWORK_INDICATOR                         
,DETAIL.ACCUMULATOR_3_APPLIED_AMOUNT                            
,DETAIL.ACCUMULATOR_3_APPLIED_AMOUNT_ACTION_CODE                
,DETAIL.ACCUMULATOR_3_BENEFIT_PERIOD_AMOUNT                     
,DETAIL.ACCUMULATOR_3_BENEFIT_PERIOD_AMOUNT_ACTION_CODE         
,DETAIL.ACCUMULATOR_3_REMAINING_BALANCE                         
,DETAIL.ACCUMULATOR_3_REMAINING_BALANCE_ACTION_CODE             
,DETAIL.ACCUMULATOR_4_BALANCE_QUALIFIER                         
,DETAIL.ACCUMULATOR_4_NETWORK_INDICATOR                         
,DETAIL.ACCUMULATOR_4_APPLIED_AMOUNT                            
,DETAIL.ACCUMULATOR_4_APPLIED_AMOUNT_ACTION_CODE                
,DETAIL.ACCUMULATOR_4_BENEFIT_PERIOD_AMOUNT                     
,DETAIL.ACCUMULATOR_4_BENEFIT_PERIOD_AMOUNT_ACTION_CODE         
,DETAIL.ACCUMULATOR_4_REMAINING_BALANCE                         
,DETAIL.ACCUMULATOR_4_REMAINING_BALANCE_ACTION_CODE             
,DETAIL.ACCUMULATOR_5_BALANCE_QUALIFIER                         
,DETAIL.ACCUMULATOR_5_NETWORK_INDICATOR                         
,DETAIL.ACCUMULATOR_5_APPLIED_AMOUNT                            
,DETAIL.ACCUMULATOR_5_APPLIED_AMOUNT_ACTION_CODE                
,DETAIL.ACCUMULATOR_5_BENEFIT_PERIOD_AMOUNT                     
,DETAIL.ACCUMULATOR_5_BENEFIT_PERIOD_AMOUNT_ACTION_CODE         
,DETAIL.ACCUMULATOR_5_REMAINING_BALANCE                         
,DETAIL.ACCUMULATOR_5_REMAINING_BALANCE_ACTION_CODE             
,DETAIL.ACCUMULATOR_6_BALANCE_QUALIFIER                         
,DETAIL.ACCUMULATOR_6_NETWORK_INDICATOR                         
,DETAIL.ACCUMULATOR_6_APPLIED_AMOUNT                            
,DETAIL.ACCUMULATOR_6_APPLIED_AMOUNT_ACTION_CODE                
,DETAIL.ACCUMULATOR_6_BENEFIT_PERIOD_AMOUNT                     
,DETAIL.ACCUMULATOR_6_BENEFIT_PERIOD_AMOUNT_ACTION_CODE         
,DETAIL.ACCUMULATOR_6_REMAINING_BALANCE                         
,DETAIL.ACCUMULATOR_6_REMAINING_BALANCE_ACTION_CODE             
,DETAIL.RESERVED_5                                              
,DETAIL.ACCUMULATOR_7_BALANCE_QUALIFIER                         
,DETAIL.ACCUMULATOR_7_NETWORK_INDICATOR                         
,DETAIL.ACCUMULATOR_7_APPLIED_AMOUNT                            
,DETAIL.ACCUMULATOR_7_APPLIED_AMOUNT_ACTION_CODE                
,DETAIL.ACCUMULATOR_7_BENEFIT_PERIOD_AMOUNT                     
,DETAIL.ACCUMULATOR_7_BENEFIT_PERIOD_AMOUNT_ACTION_CODE         
,DETAIL.ACCUMULATOR_7_REMAINING_BALANCE                         
,DETAIL.ACCUMULATOR_7_REMAINING_BALANCE_ACTION_CODE             
,DETAIL.ACCUMULATOR_8_BALANCE_QUALIFIER                         
,DETAIL.ACCUMULATOR_8_NETWORK_INDICATOR                         
,DETAIL.ACCUMULATOR_8_APPLIED_AMOUNT                            
,DETAIL.ACCUMULATOR_8_APPLIED_AMOUNT_ACTION_CODE                
,DETAIL.ACCUMULATOR_8_BENEFIT_PERIOD_AMOUNT                     
,DETAIL.ACCUMULATOR_8_BENEFIT_PERIOD_AMOUNT_ACTION_CODE         
,DETAIL.ACCUMULATOR_8_REMAINING_BALANCE                         
,DETAIL.ACCUMULATOR_8_REMAINING_BALANCE_ACTION_CODE             
,DETAIL.ACCUMULATOR_9_BALANCE_QUALIFIER                         
,DETAIL.ACCUMULATOR_9_NETWORK_INDICATOR                         
,DETAIL.ACCUMULATOR_9_APPLIED_AMOUNT                            
,DETAIL.ACCUMULATOR_9_APPLIED_AMOUNT_ACTION_CODE                
,DETAIL.ACCUMULATOR_9_BENEFIT_PERIOD_AMOUNT                     
,DETAIL.ACCUMULATOR_9_BENEFIT_PERIOD_AMOUNT_ACTION_CODE         
,DETAIL.ACCUMULATOR_9_REMAINING_BALANCE                         
,DETAIL.ACCUMULATOR_9_REMAINING_BALANCE_ACTION_CODE             
,DETAIL.ACCUMULATOR_10_BALANCE_QUALIFIER                        
,DETAIL.ACCUMULATOR_10_NETWORK_INDICATOR                        
,DETAIL.ACCUMULATOR_10_APPLIED_AMOUNT                           
,DETAIL.ACCUMULATOR_10_APPLIED_AMOUNT_ACTION_CODE               
,DETAIL.ACCUMULATOR_10_BENEFIT_PERIOD_AMOUNT                    
,DETAIL.ACCUMULATOR_10_BENEFIT_PERIOD_AMOUNT_ACTION_CODE        
,DETAIL.ACCUMULATOR_10_REMAINING_BALANCE                        
,DETAIL.ACCUMULATOR_10_REMAINING_BALANCE_ACTION_CODE            
,DETAIL.ACCUMULATOR_11_BALANCE_QUALIFIER                        
,DETAIL.ACCUMULATOR_11_NETWORK_INDICATOR                        
,DETAIL.ACCUMULATOR_11_APPLIED_AMOUNT                           
,DETAIL.ACCUMULATOR_11_APPLIED_AMOUNT_ACTION_CODE               
,DETAIL.ACCUMULATOR_11_BENEFIT_PERIOD_AMOUNT                    
,DETAIL.ACCUMULATOR_11_BENEFIT_PERIOD_AMOUNT_ACTION_CODE        
,DETAIL.ACCUMULATOR_11_REMAINING_BALANCE                        
,DETAIL.ACCUMULATOR_11_REMAINING_BALANCE_ACTION_CODE            
,DETAIL.ACCUMULATOR_12_BALANCE_QUALIFIER                        
,DETAIL.ACCUMULATOR_12_NETWORK_INDICATOR                        
,DETAIL.ACCUMULATOR_12_APPLIED_AMOUNT                           
,DETAIL.ACCUMULATOR_12_APPLIED_AMOUNT_ACTION_CODE               
,DETAIL.ACCUMULATOR_12_BENEFIT_PERIOD_AMOUNT                    
,DETAIL.ACCUMULATOR_12_BENEFIT_PERIOD_AMOUNT_ACTION_CODE        
,DETAIL.ACCUMULATOR_12_REMAINING_BALANCE                        
,DETAIL.ACCUMULATOR_12_REMAINING_BALANCE_ACTION_CODE            
,DETAIL.OPTIONAL_DATA_INDICATOR                                 
,DETAIL.TOTAL_AMOUNT_PAID                                       
,DETAIL.TOTAL_AMOUNT_PAID_ACTION_CODE                           
,DETAIL.AMOUNT_OF_COPAY                                         
,DETAIL.AMOUNT_OF_COPAY_ACTION_CODE                             
,DETAIL.PATIENT_PAY_AMOUNT                                      
,DETAIL.PATIENT_PAY_AMOUNT_ACTION_CODE                          
,DETAIL.AMOUNT_ATTRIBUTED_TO_PRODUCT_SELECTION_BRAND            
,DETAIL.AMOUNT_ATTRIBUTED_TO_PRODUCT_SELECTION_BRAND_ACTION_CODE
,DETAIL.AMOUNT_ATTRIBUTED_TO_SALES_TAX                          
,DETAIL.AMOUNT_ATTRIBUTED_TO_SALES_TAX_ACTION_CODE              
,DETAIL.AMOUNT_ATTRIBUTED_TO_PROCESSOR_FEE                      
,DETAIL.AMOUNT_ATTRIBUTED_TO_PROCESSOR_FEE_ACTION_CODE          
,DETAIL.GROSS_AMOUNT_DUE                                        
,DETAIL.GROSS_AMOUNT_DUE_ACTION_CODE                            
,DETAIL.INVOICED_AMOUNT                                         
,DETAIL.INVOICED_AMOUNT_ACTION_CODE                             
,DETAIL.PENALTY_AMOUNT                                          
,DETAIL.PENALTY_AMOUNT_ACTION_CODE                              
,DETAIL.RESERVED_6                                              
,DETAIL.PRODUCT_SERVICE_IDENTIFIER_QUALIFIER                    
,DETAIL.PRODUCT_SERVICE_IDENTIFIER                              
,DETAIL.DAYS_SUPPLY                                             
,DETAIL.QUANTITY_DISPENSED                                      
,DETAIL.PRODUCT_SERVICE_NAME                                    
,DETAIL.BRAND_GENERIC_INDICATOR                                 
,DETAIL.THERAPEUTIC_CLASS_CODE_QUALIFIER                        
,DETAIL.THERAPEUTIC_CLASS_CODE                                  
,DETAIL.DISPENSED_AS_WRITTEN                                    
,DETAIL.RESERVED_7                                              
,JOB.FILE_NAME
,JOB.CREATED_TIMESTAMP AS FILE_LOADED_TIMESTAMP 
,CLIENT.abbreviated_name AS CLIENT_NAME                                             
FROM AHUB_DW.ACCUMULATOR_DETAIL AS DETAIL
LEFT JOIN AHUB_DW.JOB AS JOB
  ON DETAIL.JOB_KEY = JOB.JOB_KEY
LEFT JOIN AHUB_DW.CLIENT 
  ON JOB.CLIENT_ID = CLIENT.CLIENT_ID
);

/*==============================================================*/
/* View: VW_ACCUMULATOR_DETAIL_DETOKENIZED  					*/
/*==============================================================*/

CREATE OR REPLACE VIEW AHUB_DW.VW_ACCUMULATOR_DETAIL_VALIDATION_RESULT AS (
SELECT result.job_key,
       result.line_number,
	   columns.file_column_name,
	   decode(result.validation_result,'Y','Passed','N','Failed','') as validation_result,
	   result.error_message as validation_result_error_message,
	   rule.error_code, 
	   rule.error_level,
	   error.error_message as standard_error_message,
       result.created_timestamp
FROM ahub_dw.file_validation_result AS result
 LEFT JOIN  AHUB_DW.column_rules AS rule
   ON result.column_rules_id = rule.column_rules_id
 LEFT JOIN  AHUB_DW.file_columns AS columns
   ON rule.file_column_id = columns.file_column_id
 LEFT JOIN AHUB_DW.column_error_codes AS error
   ON rule.error_code = error.error_code
 );


/*==============================================================*/
/* Table: stg_accum_hist_dtl						    	    */
/*==============================================================*/

DROP TABLE IF NOT EXISTS ahub_stg.stg_accum_hist_dtl ; 

CREATE TABLE IF NOT EXISTS AHUB_STG.STG_ACCUM_HIST_DTL
(
recd_typ	VARCHAR(1)  NOT NULL,
Member_ID	VARCHAR(18) NOT NULL,
carrier_nbr 	VARCHAR(9)  NOT NULL,
account_nbr	VARCHAR(15) NOT NULL,
group_id	VARCHAR(15),
adj_typ		VARCHAR(1),
adj_amt	        DECIMAL(18,2),
accum_cd	VARCHAR(10),
adj_dt		VARCHAR(7),
adj_cd		VARCHAR(10),
plan_cd		VARCHAR(10),
care_facility	VARCHAR(6),
member_state_cd	VARCHAR(2),
patient_first_nm VARCHAR(15),
patient_last_nm	VARCHAR(25),
patient_DOB	VARCHAR(8),
Patient_relationship_cd	VARCHAR(1),
RESERVED_SP    VARCHAR(135),
line_number INTEGER NOT NULL,
JOB_KEY BIGINT NOT NULL
)
diststyle all;

/*==============================================================*/
/* View: VW_ACCUMULATOR_RECONCILIATION_DETAIL_DETOKENIZED		*/
/*==============================================================*/

CREATE OR REPLACE VIEW AHUB_DW.VW_ACCUMULATOR_RECONCILIATION_DETAIL_DETOKENIZED AS (
SELECT 
    recon.job_key
   ,recon.line_number
   ,recon.accumulator_reconciliation_detail_key
   ,recon.record_type
   ,recon.unique_record_identifier
   ,recon.data_file_sender_identifier
   ,recon.data_file_sender_name
   ,recon.cdh_client_identifier
   ,recon.patient_identifier
   ,concat(concat('~`I', trim(recon.patient_identifier)), 'I`~')  patient_identifier_PHI  
   ,recon.patient_date_of_birth
   ,concat(concat('~`D', trim(recon.patient_date_of_birth)), 'D`~') patient_date_of_birth_PHI   
   ,recon.patient_first_name
   ,concat(concat('~`N', trim(recon.patient_first_name)), 'N`~') patient_first_name_PHI
   ,recon.patient_middle_initial
   ,recon.patient_last_name
   ,concat(concat('~`N', trim(recon.patient_last_name)), 'N`~') patient_last_name_PHI  
   ,recon.patient_gender
   ,recon.carrier
   ,recon.account
   ,recon.group_name
   ,recon.accumulation_benefit_begin_date
   ,recon.accumulation_benefit_end_date
   ,recon.accumulator_segment_count
   ,recon.accumulator_1_balance_qualifier
   ,recon.accumulator_1_specific_category_type
   ,recon.accumulator_1_network_indicator
   ,recon.medical_claims_accumulation_1_balance
   ,recon.pharmacy_claims_accumulation_1_balance
   ,recon.total_medical_pharmacy_claims_accumulation_1_balance
   ,recon.accumulator_2_balance_qualifier
   ,recon.accumulator_2_specific_category_type
   ,recon.accumulator_2_network_indicator
   ,recon.medical_claims_accumulation_2_balance
   ,recon.pharmacy_claims_accumulation_2_balance
   ,recon.total_medical_pharmacy_claims_accumulation_2_balance
   ,recon.accumulator_3_balance_qualifier
   ,recon.accumulator_3_specific_category_type
   ,recon.accumulator_3_network_indicator
   ,recon.medical_claims_accumulation_3_balance
   ,recon.pharmacy_claims_accumulation_3_balance
   ,recon.total_medical_pharmacy_claims_accumulation_3_balance
   ,recon.accumulator_4_balance_qualifier
   ,recon.accumulator_4_specific_category_type
   ,recon.accumulator_4_network_indicator
   ,recon.medical_claims_accumulation_4_balance
   ,recon.pharmacy_claims_accumulation_4_balance
   ,recon.total_medical_pharmacy_claims_accumulation_4_balance
   ,recon.accumulator_5_balance_qualifier
   ,recon.accumulator_5_specific_category_type
   ,recon.accumulator_5_network_indicator
   ,recon.medical_claims_accumulation_5_balance
   ,recon.pharmacy_claims_accumulation_5_balance
   ,recon.total_medical_pharmacy_claims_accumulation_5_balance
   ,recon.accumulator_6_balance_qualifier
   ,recon.accumulator_6_specific_category_type
   ,recon.accumulator_6_network_indicator
   ,recon.medical_claims_accumulation_6_balance
   ,recon.pharmacy_claims_accumulation_6_balance
   ,recon.total_medical_pharmacy_claims_accumulation_6_balance
   ,recon.accumulator_7_balance_qualifier
   ,recon.accumulator_7_specific_category_type
   ,recon.accumulator_7_network_indicator
   ,recon.medical_claims_accumulation_7_balance
   ,recon.pharmacy_claims_accumulation_7_balance
   ,recon.total_medical_pharmacy_claims_accumulation_7_balance
   ,recon.accumulator_8_balance_qualifier
   ,recon.accumulator_8_specific_category_type
   ,recon.accumulator_8_network_indicator
   ,recon.medical_claims_accumulation_8_balance
   ,recon.pharmacy_claims_accumulation_8_balance
   ,recon.total_medical_pharmacy_claims_accumulation_8_balance
   ,recon.accumulator_9_balance_qualifier
   ,recon.accumulator_9_specific_category_type
   ,recon.accumulator_9_network_indicator
   ,recon.medical_claims_accumulation_9_balance
   ,recon.pharmacy_claims_accumulation_9_balance
   ,recon.total_medical_pharmacy_claims_accumulation_9_balance
   ,recon.accumulator_10_balance_qualifier
   ,recon.accumulator_10_specific_category_type
   ,recon.accumulator_10_network_indicator
   ,recon.medical_claims_accumulation_10_balance
   ,recon.pharmacy_claims_accumulation_10_balance
   ,recon.total_medical_pharmacy_claims_accumulation_10_balance
   ,recon.accumulator_11_balance_qualifier
   ,recon.accumulator_11_specific_category_type
   ,recon.accumulator_11_network_indicator
   ,recon.medical_claims_accumulation_11_balance
   ,recon.pharmacy_claims_accumulation_11_balance
   ,recon.total_medical_pharmacy_claims_accumulation_11_balance
   ,recon.accumulator_12_balance_qualifier
   ,recon.accumulator_12_specific_category_type
   ,recon.accumulator_12_network_indicator
   ,recon.medical_claims_accumulation_12_balance
   ,recon.pharmacy_claims_accumulation_12_balance
   ,recon.total_medical_pharmacy_claims_accumulation_12_balance
   ,recon.filler_space
   ,job.file_name
   ,job.created_timestamp as file_loaded_timestamp 
   ,client.abbreviated_name as client_name  
FROM ahub_dw.accumulator_reconciliation_detail AS recon
LEFT JOIN ahub_dw.job AS job
  ON recon.job_key = job.job_key
LEFT JOIN ahub_dw.client 
  ON job.client_id = client.client_id
);


/*==============================================================*/
/* Create Read Only group   		        	        */
/*==============================================================*/

CREATE GROUP business_user_ro_group;

/*==============================================================*/
/* Create the read only user (business user) 		        */
/*==============================================================*/

create user business_user_ro password 'Ad5153c434b4';

/*==============================================================*/
/* Alter group add new business user to it		        */
/*==============================================================*/

ALTER GROUP business_user_ro_group ADD USER business_user_ro;

/*==============================================================*/
/* --Grant usage access on the schema to read only group        */
/*==============================================================*/

GRANT USAGE ON SCHEMA "ahub_dw" TO GROUP business_user_ro_group;

/*==============================================================*/
/* Grant SELECT access on all tables to read only group         */
/*==============================================================*/

GRANT SELECT ON ALL TABLES IN SCHEMA "ahub_dw" TO GROUP business_user_ro_group;







