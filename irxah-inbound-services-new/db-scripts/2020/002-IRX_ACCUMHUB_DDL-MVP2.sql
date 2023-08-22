/*==============================================================*/
/* Script : IRX_ACCUMHUB_DDL.sql								*/
/* Author : Sanjay Sharma (ag61753)								*/
/* Project : AHUB (MVP2)                                        */
/* Production Release Date: 04/17/2020                          */
/*==============================================================*/

/*==============================================================*/
/* Schema: AHUB_DW									            */
/*==============================================================*/

CREATE SCHEMA AHUB_DW AUTHORIZATION awsuser;

/*==============================================================*/
/* Table: CLIENT_REFERENCE									    */
/*==============================================================*/

CREATE TABLE AHUB_DW.CLIENT_REFERENCE
(
  CLIENT_KEY BIGINT IDENTITY(1,1) Not NULL,
  CLIENT_NAME VARCHAR(100) Not NULL,
  CLIENT_FULL_NAME VARCHAR(256) ,
  START_DATE Date ,
  END_DATE Date ,
  CREATED_BY VARCHAR(100) Not NULL,
  CREATED_TIMESTAMP TIMESTAMP Not NULL,
  UPDATED_BY VARCHAR(100) ,
  UPDATED_TIMESTAMP TIMESTAMP ,
CONSTRAINT IDX_PK_CLIENT_REFERENCE PRIMARY KEY (CLIENT_KEY) 
)
DISTSTYLE KEY
DISTKEY (CLIENT_KEY);

/*==============================================================*/
/* Table: CLIENT_FILE_SUSCRIPTION_REFERENCE						*/
/*==============================================================*/

CREATE TABLE AHUB_DW.CLIENT_FILE_SUBSCRIPTION_REFERENCE
(
  CLIENT_FILE_SUBSCRIPTION_KEY BIGINT IDENTITY(1,1)  Not NULL,
  CLIENT_KEY BIGINT Not NULL,
  FILE_NAME_PATTERN VARCHAR(256) Not NULL,
  FILE_SUBSCRIPTION_START_DATE DATE,
  FILE_SUBSCRIPTION_END_DATE DATE ,
  FILE_FREQUENCY VARCHAR(200) ,
  SENDER_IDENTIFIER VARCHAR(30) ,
  RECIEVER_IDENTIFIER VARCHAR(30) ,
  FILE_TYPE VARCHAR(50) ,
  CREATED_BY VARCHAR(100) Not NULL,
  CREATED_TIMESTAMP TIMESTAMP Not NULL,
  UPDATED_BY VARCHAR(100) ,
  UPDATED_TIMESTAMP TIMESTAMP ,
CONSTRAINT IDX_PK_CLIENT_FILE_SUNCRIPTION_REFERENCE PRIMARY KEY (CLIENT_FILE_SUBSCRIPTION_KEY) ,
CONSTRAINT FK_CLIENT_REFERENCE_CLIENT_FILE_SUBSCRIPTION_REFERENCE foreign key(CLIENT_KEY) references AHUB_DW.CLIENT_REFERENCE(CLIENT_KEY)
)
DISTSTYLE KEY
DISTKEY (CLIENT_KEY);

/*==============================================================*/
/* Table: ERROR_REFERENCE										*/
/*==============================================================*/

CREATE TABLE AHUB_DW.ERROR_REFERENCE
(
  ERROR_KEY BIGINT IDENTITY(1,1) NOT NULL,
  ERROR_CODE INT NOT NULL,
  ERROR_MESSAGE VARCHAR(1024) NOT NULL,
  ERROR_VALIDATION_TYPE VARCHAR(50) ,
  FIELD_NAME VARCHAR(127) ,
  CREATED_BY VARCHAR(100) Not NULL,
  CREATED_TIMESTAMP TIMESTAMP Not NULL,
  UPDATED_BY VARCHAR(100) ,
  UPDATED_TIMESTAMP TIMESTAMP ,
CONSTRAINT IDX_PK_ERROR_REFERENCE PRIMARY KEY (ERROR_KEY)
)
DISTSTYLE KEY
DISTKEY (ERROR_KEY);

/*==============================================================*/
/* Table: RULE_REFERENCE										*/
/*==============================================================*/

CREATE TABLE AHUB_DW.RULE_REFERENCE 
(
  RULE_KEY                      BIGINT IDENTITY(1,1) not null,
  RULE_NAME                     VARCHAR(500) not null,
  RULE_DESCRIPTION              VARCHAR(1000),
  RULE_SUCCESS_CRITERIA         VARCHAR(4000),
  RULE_FIELD_NAME               VARCHAR(127),
  ERROR_KEY                     BIGINT,
  CREATED_BY                    VARCHAR(100) not null,
  CREATED_TIMESTAMP             TIMESTAMP not null,
  UPDATED_BY                    VARCHAR(100),
  UPDATED_TIMESTAMP             TIMESTAMP,
CONSTRAINT IDX_PK_RULE_REFERENCE PRIMARY KEY (RULE_KEY)
)
DISTSTYLE KEY
DISTKEY (RULE_KEY);

/*==============================================================*/
/* Table: CLIENT_RULE_REFERENCE									*/
/*==============================================================*/

CREATE TABLE AHUB_DW.CLIENT_RULE_REFERENCE
(
  CLIENT_RULE_KEY BIGINT IDENTITY(1,1) NOT NULL,
  RULE_KEY BIGINT Not NULL,
  CLIENT_KEY BIGINT Not NULL,
  CLIENT_FILE_SUBSCRIPTION_KEY BIGINT Not NULL,
  ERROR_KEY BIGINT ,
  RULE_START_DATE DATE ,
  RULE_END_DATE DATE ,
  RULE_SEQUENCE_NUMBER INTEGER ,
  RULE_TYPE_NAME VARCHAR(100) ,
  RULE_TYPE_VALUE VARCHAR(100) ,
  CREATED_BY VARCHAR(100) Not NULL,
  CREATED_TIMESTAMP TIMESTAMP Not NULL,
  UPDATED_BY VARCHAR(100) ,
  UPDATED_TIMESTAMP TIMESTAMP ,
CONSTRAINT IDX_PK_CLIENT_RULE_REFERENCE PRIMARY KEY (CLIENT_RULE_KEY),
CONSTRAINT FK_RULE_REFERENCE_CLIENT_RULE_REFERENCE foreign key(RULE_KEY) references AHUB_DW.RULE_REFERENCE(RULE_KEY),
CONSTRAINT FK_CLIENT_REFERENCE_CLIENT_RULE_REFERENCE foreign key(CLIENT_KEY) references AHUB_DW.CLIENT_REFERENCE(CLIENT_KEY),
CONSTRAINT FK_CLIENT_FILE_SUBCRIPITION_REFERENCE_CLIENT_RULE_REFERENCE foreign key(CLIENT_FILE_SUBSCRIPTION_KEY) references AHUB_DW.CLIENT_FILE_SUBSCRIPTION_REFERENCE(CLIENT_FILE_SUBSCRIPTION_KEY),
CONSTRAINT FK_ERROR_REFERENCE_CLIENT_RULE_REFERENCE foreign key(ERROR_KEY) references AHUB_DW.ERROR_REFERENCE(ERROR_KEY)
)
DISTSTYLE KEY
DISTKEY (RULE_KEY);


/*==============================================================*/
/* Table: JOB													*/
/*==============================================================*/

CREATE TABLE AHUB_DW.JOB (
JOB_KEY                       BIGINT IDENTITY(1,1) not null,
JOB_NAME                      VARCHAR(256) not null,
JOB_START_TIMESTAMP           TIMESTAMP not null,
JOB_END_TIMESTAMP			  TIMESTAMP,
FILE_NAME                     VARCHAR(500) not null,
FILE_TYPE                     VARCHAR(50) not null,
FILE_RECORD_COUNT             BIGINT,
PRE_PROCESSING_FILE_LOCATION  VARCHAR(1024),
JOB_STATUS                    VARCHAR(256) not null,
ERROR_MESSAGE                 VARCHAR(1024),
SENDER_IDENTIFIER             VARCHAR(256),
RECEIVER_IDENTIFIER           VARCHAR(256),
POST_PROCESSING_FILE_LOCATION VARCHAR(1024),
SOURCE_CODE                   VARCHAR(50),
CREATED_BY                    VARCHAR(100) not null,
CREATED_TIMESTAMP             TIMESTAMP not null,
UPDATED_BY                    VARCHAR(100),
UPDATED_TIMESTAMP             TIMESTAMP,
CONSTRAINT IDX_PK_JOB PRIMARY KEY (JOB_KEY)
)
DISTSTYLE KEY
DISTKEY (JOB_KEY);

/*==============================================================*/
/* Table: JOB_DETAIL													*/
/*==============================================================*/

CREATE TABLE AHUB_DW.JOB_DETAIL (
  JOB_DETAIL_KEY BIGINT IDENTITY(1,1) Not NULL,
  JOB_KEY BIGINT Not NULL,
  SEQUENCE_NUMBER BIGINT Not NULL,
  JOB_DETAIL_NAME VARCHAR(256) Not NULL,
  JOB_DETAIL_DESCRIPTION VARCHAR(1000) ,
  JOB_DETAIL_START_TIMESTAMP TIMESTAMP Not NULL,
  JOB_DETAIL_END_TIMESTAMP TIMESTAMP ,
  FILE_NAME VARCHAR(500) Not NULL,
  JOB_DETAIL_STATUS VARCHAR(256) Not NULL,
  ERROR_MESSAGE VARCHAR(1024),
  CREATED_BY VARCHAR(100) Not NULL,
  CREATED_TIMESTAMP TIMESTAMP Not NULL,
  UPDATED_BY VARCHAR(100) ,
  UPDATED_TIMESTAMP TIMESTAMP ,
CONSTRAINT IDX_PK_JOB_DETAIL PRIMARY KEY (JOB_DETAIL_KEY) ,
CONSTRAINT FK_JOB_JOB_DETAIL foreign key(JOB_KEY) references AHUB_DW.JOB(JOB_KEY)
)
DISTSTYLE KEY
DISTKEY (JOB_KEY);

/*==============================================================*/
/* Table: ACCUMULATOR_RECONCILIATION_DETAIL                     */
/*==============================================================*/

CREATE TABLE AHUB_DW.ACCUMULATOR_RECONCILIATION_DETAIL 
(
  ACCUMULATOR_RECONCILIATION_DETAIL_KEY BIGINT IDENTITY(1,1) not null,
  RECORD_TYPE VARCHAR(3) not null,
  UNIQUE_RECORD_IDENTIFIER VARCHAR(50) not null,
  DATA_FILE_SENDER_IDENTIFIER VARCHAR(5) not null,
  DATA_FILE_SENDER__NAME VARCHAR(15) not null,
  CDH_CLIENT_IDENTIFIER VARCHAR(30) not null,
  PATIENT_IDENTIFIER VARCHAR(20) not null,
  PATIENT_DATE_OF_BIRTH VARCHAR(8) not null,
  PATIENT_FIRST_NAME VARCHAR(15) not null,
  PATIENT_MIDDLE_INITIAL VARCHAR(1),
  PATIENT_LAST_NAME VARCHAR(20) not null,
  PATIENT_GENDER VARCHAR(1) not null,
  CARRIER VARCHAR(15),
  ACCOUNT VARCHAR(15),
  GROUP_NAME VARCHAR(15),
  ACCUMULATION_BENEFIT_BEGIN_DATE VARCHAR(8) not null,
  ACCUMULATION_BENEFIT_END_DATE VARCHAR(8) not null,
  ACCUMULATOR_SEGMENT_COUNT VARCHAR(2),
  ACCUMULATOR_1_BALANCE_QUALIFIER VARCHAR(2),
  ACCUMULATOR_1_SPECIFIC_CATEGORY_TYPE VARCHAR(2),
  ACCUMULATOR_1_NETWORK_INDICATOR VARCHAR(1),
  MEDICAL_CLAIMS_ACCUMULATION_1_BALANCE VARCHAR(11),
  PHARMACY_CLAIMS_ACCUMULATION_1_BALANCE VARCHAR(11),
  TOTAL_MEDICAL_PHARMACY_CLAIMS_ACCUMULATION_1_BALANCE VARCHAR(11),
  ACCUMULATOR_2_BALANCE_QUALIFIER VARCHAR(2),
  ACCUMULATOR_2_SPECIFIC_CATEGORY_TYPE VARCHAR(2),
  ACCUMULATOR_2_NETWORK_INDICATOR VARCHAR(1),
  MEDICAL_CLAIMS_ACCUMULATION_2_BALANCE VARCHAR(11),
  PHARMACY_CLAIMS_ACCUMULATION_2_BALANCE VARCHAR(11),
  TOTAL_MEDICAL_PHARMACY_CLAIMS_ACCUMULATION_2_BALANCE VARCHAR(11),
  ACCUMULATOR_3_BALANCE_QUALIFIER VARCHAR(2),
  ACCUMULATOR_3_SPECIFIC_CATEGORY_TYPE VARCHAR(2),
  ACCUMULATOR_3_NETWORK_INDICATOR VARCHAR(1),
  MEDICAL_CLAIMS_ACCUMULATION_3_BALANCE VARCHAR(11),
  PHARMACY_CLAIMS_ACCUMULATION_3_BALANCE VARCHAR(11),
  TOTAL_MEDICAL_PHARMACY_CLAIMS_ACCUMULATION_3_BALANCE VARCHAR(11),
  ACCUMULATOR_4_BALANCE_QUALIFIER VARCHAR(2),
  ACCUMULATOR_4_SPECIFIC_CATEGORY_TYPE VARCHAR(2),
  ACCUMULATOR_4_NETWORK_INDICATOR VARCHAR(1),
  MEDICAL_CLAIMS_ACCUMULATION_4_BALANCE VARCHAR(11),
  PHARMACY_CLAIMS_ACCUMULATION_4_BALANCE VARCHAR(11),
  TOTAL_MEDICAL_PHARMACY_CLAIMS_ACCUMULATION_4_BALANCE VARCHAR(11),
  ACCUMULATOR_5_BALANCE_QUALIFIER VARCHAR(2),
  ACCUMULATOR_5_SPECIFIC_CATEGORY_TYPE VARCHAR(2),
  ACCUMULATOR_5_NETWORK_INDICATOR VARCHAR(1),
  MEDICAL_CLAIMS_ACCUMULATION_5_BALANCE VARCHAR(11),
  PHARMACY_CLAIMS_ACCUMULATION_5_BALANCE VARCHAR(11),
  TOTAL_MEDICAL_PHARMACY_CLAIMS_ACCUMULATION_5_BALANCE VARCHAR(11),
  ACCUMULATOR_6_BALANCE_QUALIFIER VARCHAR(2),
  ACCUMULATOR_6_SPECIFIC_CATEGORY_TYPE VARCHAR(2),
  ACCUMULATOR_6_NETWORK_INDICATOR VARCHAR(1),
  MEDICAL_CLAIMS_ACCUMULATION_6_BALANCE VARCHAR(11),
  PHARMACY_CLAIMS_ACCUMULATION_6_BALANCE VARCHAR(11),
  TOTAL_MEDICAL_PHARMACY_CLAIMS_ACCUMULATION_6_BALANCE VARCHAR(11),
  ACCUMULATOR_7_BALANCE_QUALIFIER VARCHAR(2),
  ACCUMULATOR_7_SPECIFIC_CATEGORY_TYPE VARCHAR(2),
  ACCUMULATOR_7_NETWORK_INDICATOR VARCHAR(1),
  MEDICAL_CLAIMS_ACCUMULATION_7_BALANCE VARCHAR(11),
  PHARMACY_CLAIMS_ACCUMULATION_7_BALANCE VARCHAR(11),
  TOTAL_MEDICAL_PHARMACY_CLAIMS_ACCUMULATION_7_BALANCE VARCHAR(11),
  ACCUMULATOR_8_BALANCE_QUALIFIER VARCHAR(2),
  ACCUMULATOR_8_SPECIFIC_CATEGORY_TYPE VARCHAR(2),
  ACCUMULATOR_8_NETWORK_INDICATOR VARCHAR(1),
  MEDICAL_CLAIMS_ACCUMULATION_8_BALANCE VARCHAR(11),
  PHARMACY_CLAIMS_ACCUMULATION_8_BALANCE VARCHAR(11),
  TOTAL_MEDICAL_PHARMACY_CLAIMS_ACCUMULATION_8_BALANCE VARCHAR(11),
  ACCUMULATOR_9_BALANCE_QUALIFIER VARCHAR(2),
  ACCUMULATOR_9_SPECIFIC_CATEGORY_TYPE VARCHAR(2),
  ACCUMULATOR_9_NETWORK_INDICATOR VARCHAR(1),
  MEDICAL_CLAIMS_ACCUMULATION_9_BALANCE VARCHAR(11),
  PHARMACY_CLAIMS_ACCUMULATION_9_BALANCE VARCHAR(11),
  TOTAL_MEDICAL_PHARMACY_CLAIMS_ACCUMULATION_9_BALANCE VARCHAR(11),
  ACCUMULATOR_10_BALANCE_QUALIFIER VARCHAR(2),
  ACCUMULATOR_10_SPECIFIC_CATEGORY_TYPE VARCHAR(2),
  ACCUMULATOR_10_NETWORK_INDICATOR VARCHAR(1),
  MEDICAL_CLAIMS_ACCUMULATION_10_BALANCE VARCHAR(11),
  PHARMACY_CLAIMS_ACCUMULATION_10_BALANCE VARCHAR(11),
  TOTAL_MEDICAL_PHARMACY_CLAIMS_ACCUMULATION_10_BALANCE VARCHAR(11),
  ACCUMULATOR_11_BALANCE_QUALIFIER VARCHAR(2),
  ACCUMULATOR_11_SPECIFIC_CATEGORY_TYPE VARCHAR(2),
  ACCUMULATOR_11_NETWORK_INDICATOR VARCHAR(1),
  MEDICAL_CLAIMS_ACCUMULATION_11_BALANCE VARCHAR(11),
  PHARMACY_CLAIMS_ACCUMULATION_11_BALANCE VARCHAR(11),
  TOTAL_MEDICAL_PHARMACY_CLAIMS_ACCUMULATION_11_BALANCE VARCHAR(11),
  ACCUMULATOR_12_BALANCE_QUALIFIER VARCHAR(2),
  ACCUMULATOR_12_SPECIFIC_CATEGORY_TYPE VARCHAR(2),
  ACCUMULATOR_12_NETWORK_INDICATOR VARCHAR(1),
  MEDICAL_CLAIMS_ACCUMULATION_12_BALANCE VARCHAR(11),
  PHARMACY_CLAIMS_ACCUMULATION_12_BALANCE VARCHAR(11),
  TOTAL_MEDICAL_PHARMACY_CLAIMS_ACCUMULATION_12_BALANCE VARCHAR(11),
  FILLER_SPACE VARCHAR(113) not null,
  SOURCE_CODE VARCHAR(50) not null,
  JOB_KEY BIGINT not null,
  FILE_NAME VARCHAR(500) not null,
  CREATED_BY VARCHAR(100) not null,
  CREATED_TIMESTAMP TIMESTAMP not null,
  UPDATE_BY VARCHAR(100),
  UPDATE_TIMESTAMP TIMESTAMP,
CONSTRAINT IDX_PK_ACCUMULATOR_RECONCILIATION_DETAIL primary key (ACCUMULATOR_RECONCILIATION_DETAIL_KEY)
)DISTSTYLE KEY
DISTKEY (ACCUMULATOR_RECONCILIATION_DETAIL_KEY);
;

/*==============================================================*/
/* Table: ACCUMULATOR_DETAIL									*/
/*==============================================================*/

CREATE TABLE AHUB_DW.ACCUMULATOR_DETAIL 
(
  ACCUMULATOR_DETAIL_KEY BIGINT IDENTITY(1,1) NOT NULL ,
  PROCESSOR_ROUTING_IDENTIFICATION VARCHAR(200)  ,
  RECORD_TYPE VARCHAR(2) NOT NULL ,
  TRANSMISSION_FILE_TYPE VARCHAR(2) NOT NULL ,
  VERSION_RELEASE_NUMBER VARCHAR(2) NOT NULL ,
  SENDER_IDENTIFIER VARCHAR(30) NOT NULL ,
  RECEIVER_IDENTIFIER VARCHAR(30) NOT NULL ,
  SUBMISSION_NUMBER VARCHAR(4) NOT NULL ,
  TRANSACTION_RESPONSE_STATUS VARCHAR(1)  ,
  REJECT_CODE VARCHAR(3)  ,
  RECORD_LENGTH VARCHAR(5) NOT NULL ,
  RESERVED_1 VARCHAR(20) NOT NULL ,
  TRANSMISSION_DATE VARCHAR(8) NOT NULL ,
  TRANSMISSION_TIME VARCHAR(8) NOT NULL ,
  DATE_OF_SERVICE VARCHAR(8) NOT NULL ,
  SERVICE_PROVIDER_IDENTIFIER_QUALIFIER VARCHAR(2)  ,
  SERVICE_PROVIDER_IDENTIFIER VARCHAR(15)  ,
  DOCUMENT_REFERENCE_IDENTIFIER_QUALIFIER VARCHAR(2)  ,
  DOCUMENT_REFERENCE_IDENTIFIER VARCHAR(15)  ,
  TRANSMISSION_IDENTIFIER VARCHAR(50) NOT NULL ,
  BENEFIT_TYPE VARCHAR(1) NOT NULL ,
  IN_NETWORK_INDICATOR VARCHAR(1)  ,
  FORMULARY_STATUS VARCHAR(1) NOT NULL ,
  ACCUMULATOR_ACTION_CODE VARCHAR(2) NOT NULL ,
  SENDER_REFERENCE_NUMBER VARCHAR(30)  ,
  INSURANCE_CODE VARCHAR(20)  ,
  ACCUMULATOR_BALANCE_BENEFIT_TYPE VARCHAR(1)  ,
  BENEFIT_EFFECTIVE_DATE VARCHAR(8)  ,
  BENEFIT_TERMINATION_DATE VARCHAR(8)  ,
  ACCUMULATOR_CHANGE_SOURCE_CODE VARCHAR(1)  ,
  TRANSACTION_IDENTIFIER VARCHAR(30)  ,
  TRANSACTION_IDENTIFIER_CROSS_REFERENCE VARCHAR(30)  ,
  ADJUSTMENT_REASON_CODE VARCHAR(1)  ,
  ACCUMULATOR_REFERENCE_TIME_STAMP VARCHAR(26)  ,
  RESERVED_2 VARCHAR(13) NOT NULL ,
  CARDHOLDER_IDENTIFIER VARCHAR(20) NOT NULL ,
  GROUP_IDENTIFIER VARCHAR(15)  ,
  PATIENT_FIRST_NAME VARCHAR(25) NOT NULL ,
  MIDDLE_INITIAL VARCHAR(1)  ,
  PATIENT_LAST_NAME VARCHAR(35) NOT NULL ,
  PATIENT_RELATIONSHIP_CODE VARCHAR(1)  ,
  DATE_OF_BIRTH VARCHAR(8) NOT NULL ,
  PATIENT_GENDER_CODE VARCHAR(1)  ,
  PATIENT_STATE_PROVINCE_ADDRESS VARCHAR(2)  ,
  CARDHOLDER_LAST_NAME VARCHAR(35)  ,
  CARRIER_NUMBER VARCHAR(9)  ,
  CONTRACT_NUMBER VARCHAR(15)  ,
  CLIENT_PASS_THROUGH VARCHAR(50)  ,
  FAMILY_IDENTIFIER_NUMBER VARCHAR(20)  ,
  CARDHOLDER_IDENTIFIER_ALTERNATE VARCHAR(20)  ,
  GROUP_IDENTIFIER_ALTERNATE VARCHAR(15)  ,
  PATIENT_IDENTIFIER VARCHAR(20)  ,
  PERSON_CODE VARCHAR(3)  ,
  RESERVED_3 VARCHAR(90) NOT NULL ,
  ACCUMULATOR_BALANCE_COUNT VARCHAR(2) NOT NULL ,
  ACCUMULATOR_SPECIFIC_CATEGORY_TYPE VARCHAR(2)  ,
  RESERVED_4 VARCHAR(20) NOT NULL ,
  ACCUMULATOR_1_BALANCE_QUALIFIER VARCHAR(2) NOT NULL ,
  ACCUMULATOR_1_NETWORK_INDICATOR VARCHAR(1) NOT NULL ,
  ACCUMULATOR_1_APPLIED_AMOUNT VARCHAR(10) NOT NULL ,
  ACCUMULATOR_1_APPLIED_AMOUNT_ACTION_CODE VARCHAR(1) NOT NULL ,
  ACCUMULATOR_1_BENEFIT_PERIOD_AMOUNT VARCHAR(10)  ,
  ACCUMULATOR_1_BENEFIT_PERIOD_AMOUNT_ACTION_CODE VARCHAR(1)  ,
  ACCUMULATOR_1_REMAINING_BALANCE VARCHAR(10)  ,
  ACCUMULATOR_1_REMAINING_BALANCE_ACTION_CODE VARCHAR(1)  ,
  ACCUMULATOR_2_BALANCE_QUALIFIER VARCHAR(2)  ,
  ACCUMULATOR_2_NETWORK_INDICATOR VARCHAR(1)  ,
  ACCUMULATOR_2_APPLIED_AMOUNT VARCHAR(10)  ,
  ACCUMULATOR_2_APPLIED_AMOUNT_ACTION_CODE VARCHAR(1)  ,
  ACCUMULATOR_2_BENEFIT_PERIOD_AMOUNT VARCHAR(10)  ,
  ACCUMULATOR_2_BENEFIT_PERIOD_AMOUNT_ACTION_CODE VARCHAR(1)  ,
  ACCUMULATOR_2_REMAINING_BALANCE VARCHAR(10)  ,
  ACCUMULATOR_2_REMAINING_BALANCE_ACTION_CODE VARCHAR(1)  ,
  ACCUMULATOR_3_BALANCE_QUALIFIER VARCHAR(2)  ,
  ACCUMULATOR_3_NETWORK_INDICATOR VARCHAR(1)  ,
  ACCUMULATOR_3_APPLIED_AMOUNT VARCHAR(10)  ,
  ACCUMULATOR_3_APPLIED_AMOUNT_ACTION_CODE VARCHAR(1)  ,
  ACCUMULATOR_3_BENEFIT_PERIOD_AMOUNT VARCHAR(10)  ,
  ACCUMULATOR_3_BENEFIT_PERIOD_AMOUNT_ACTION_CODE VARCHAR(1)  ,
  ACCUMULATOR_3_REMAINING_BALANCE VARCHAR(10)  ,
  ACCUMULATOR_3_REMAINING_BALANCE_ACTION_CODE VARCHAR(1)  ,
  ACCUMULATOR_4_BALANCE_QUALIFIER VARCHAR(2)  ,
  ACCUMULATOR_4_NETWORK_INDICATOR VARCHAR(1)  ,
  ACCUMULATOR_4_APPLIED_AMOUNT VARCHAR(10)  ,
  ACCUMULATOR_4_APPLIED_AMOUNT_ACTION_CODE VARCHAR(1)  ,
  ACCUMULATOR_4_BENEFIT_PERIOD_AMOUNT VARCHAR(10)  ,
  ACCUMULATOR_4_BENEFIT_PERIOD_AMOUNT_ACTION_CODE VARCHAR(1)  ,
  ACCUMULATOR_4_REMAINING_BALANCE VARCHAR(10)  ,
  ACCUMULATOR_4_REMAINING_BALANCE_ACTION_CODE VARCHAR(1)  ,
  ACCUMULATOR_5_BALANCE_QUALIFIER VARCHAR(2)  ,
  ACCUMULATOR_5_NETWORK_INDICATOR VARCHAR(1)  ,
  ACCUMULATOR_5_APPLIED_AMOUNT VARCHAR(10)  ,
  ACCUMULATOR_5_APPLIED_AMOUNT_ACTION_CODE VARCHAR(1)  ,
  ACCUMULATOR_5_BENEFIT_PERIOD_AMOUNT VARCHAR(10)  ,
  ACCUMULATOR_5_BENEFIT_PERIOD_AMOUNT_ACTION_CODE VARCHAR(1)  ,
  ACCUMULATOR_5_REMAINING_BALANCE VARCHAR(10)  ,
  ACCUMULATOR_5_REMAINING_BALANCE_ACTION_CODE VARCHAR(1)  ,
  ACCUMULATOR_6_BALANCE_QUALIFIER VARCHAR(2)  ,
  ACCUMULATOR_6_NETWORK_INDICATOR VARCHAR(1)  ,
  ACCUMULATOR_6_APPLIED_AMOUNT VARCHAR(10)  ,
  ACCUMULATOR_6_APPLIED_AMOUNT_ACTION_CODE VARCHAR(1)  ,
  ACCUMULATOR_6_BENEFIT_PERIOD_AMOUNT VARCHAR(10)  ,
  ACCUMULATOR_6_BENEFIT_PERIOD_AMOUNT_ACTION_CODE VARCHAR(1)  ,
  ACCUMULATOR_6_REMAINING_BALANCE VARCHAR(10)  ,
  ACCUMULATOR_6_REMAINING_BALANCE_ACTION_CODE VARCHAR(1)  ,
  RESERVED_5 VARCHAR(24) NOT NULL ,
  ACCUMULATOR_7_BALANCE_QUALIFIER VARCHAR(2)  ,
  ACCUMULATOR_7_NETWORK_INDICATOR VARCHAR(1)  ,
  ACCUMULATOR_7_APPLIED_AMOUNT VARCHAR(10)  ,
  ACCUMULATOR_7_APPLIED_AMOUNT_ACTION_CODE VARCHAR(1)  ,
  ACCUMULATOR_7_BENEFIT_PERIOD_AMOUNT VARCHAR(10)  ,
  ACCUMULATOR_7_BENEFIT_PERIOD_AMOUNT_ACTION_CODE VARCHAR(1)  ,
  ACCUMULATOR_7_REMAINING_BALANCE VARCHAR(10)  ,
  ACCUMULATOR_7_REMAINING_BALANCE_ACTION_CODE VARCHAR(1)  ,
  ACCUMULATOR_8_BALANCE_QUALIFIER VARCHAR(2)  ,
  ACCUMULATOR_8_NETWORK_INDICATOR VARCHAR(1)  ,
  ACCUMULATOR_8_APPLIED_AMOUNT VARCHAR(10)  ,
  ACCUMULATOR_8_APPLIED_AMOUNT_ACTION_CODE VARCHAR(1)  ,
  ACCUMULATOR_8_BENEFIT_PERIOD_AMOUNT VARCHAR(10)  ,
  ACCUMULATOR_8_BENEFIT_PERIOD_AMOUNT_ACTION_CODE VARCHAR(1)  ,
  ACCUMULATOR_8_REMAINING_BALANCE VARCHAR(10)  ,
  ACCUMULATOR_8_REMAINING_BALANCE_ACTION_CODE VARCHAR(1)  ,
  ACCUMULATOR_9_BALANCE_QUALIFIER VARCHAR(2)  ,
  ACCUMULATOR_9_NETWORK_INDICATOR VARCHAR(1)  ,
  ACCUMULATOR_9_APPLIED_AMOUNT VARCHAR(10)  ,
  ACCUMULATOR_9_APPLIED_AMOUNT_ACTION_CODE VARCHAR(1)  ,
  ACCUMULATOR_9_BENEFIT_PERIOD_AMOUNT VARCHAR(10)  ,
  ACCUMULATOR_9_BENEFIT_PERIOD_AMOUNT_ACTION_CODE VARCHAR(1)  ,
  ACCUMULATOR_9_REMAINING_BALANCE VARCHAR(10)  ,
  ACCUMULATOR_9_REMAINING_BALANCE_ACTION_CODE VARCHAR(1)  ,
  ACCUMULATOR_10_BALANCE_QUALIFIER VARCHAR(2)  ,
  ACCUMULATOR_10_NETWORK_INDICATOR VARCHAR(1)  ,
  ACCUMULATOR_10_APPLIED_AMOUNT VARCHAR(10)  ,
  ACCUMULATOR_10_APPLIED_AMOUNT_ACTION_CODE VARCHAR(1)  ,
  ACCUMULATOR_10_BENEFIT_PERIOD_AMOUNT VARCHAR(10)  ,
  ACCUMULATOR_10_BENEFIT_PERIOD_AMOUNT_ACTION_CODE VARCHAR(1)  ,
  ACCUMULATOR_10_REMAINING_BALANCE VARCHAR(10)  ,
  ACCUMULATOR_10_REMAINING_BALANCE_ACTION_CODE VARCHAR(1)  ,
  ACCUMULATOR_11_BALANCE_QUALIFIER VARCHAR(2)  ,
  ACCUMULATOR_11_NETWORK_INDICATOR VARCHAR(1)  ,
  ACCUMULATOR_11_APPLIED_AMOUNT VARCHAR(10)  ,
  ACCUMULATOR_11_APPLIED_AMOUNT_ACTION_CODE VARCHAR(1)  ,
  ACCUMULATOR_11_BENEFIT_PERIOD_AMOUNT VARCHAR(10)  ,
  ACCUMULATOR_11_BENEFIT_PERIOD_AMOUNT_ACTION_CODE VARCHAR(1)  ,
  ACCUMULATOR_11_REMAINING_BALANCE VARCHAR(10)  ,
  ACCUMULATOR_11_REMAINING_BALANCE_ACTION_CODE VARCHAR(1)  ,
  ACCUMULATOR_12_BALANCE_QUALIFIER VARCHAR(2)  ,
  ACCUMULATOR_12_NETWORK_INDICATOR VARCHAR(1)  ,
  ACCUMULATOR_12_APPLIED_AMOUNT VARCHAR(10)  ,
  ACCUMULATOR_12_APPLIED_AMOUNT_ACTION_CODE VARCHAR(1)  ,
  ACCUMULATOR_12_BENEFIT_PERIOD_AMOUNT VARCHAR(10)  ,
  ACCUMULATOR_12_BENEFIT_PERIOD_AMOUNT_ACTION_CODE VARCHAR(1)  ,
  ACCUMULATOR_12_REMAINING_BALANCE VARCHAR(10)  ,
  ACCUMULATOR_12_REMAINING_BALANCE_ACTION_CODE VARCHAR(1)  ,
  OPTIONAL_DATA_INDICATOR VARCHAR(1) NOT NULL ,
  TOTAL_AMOUNT_PAID VARCHAR(10)  ,
  TOTAL_AMOUNT_PAID_ACTION_CODE VARCHAR(1)  ,
  AMOUNT_OF_COPAY VARCHAR(10)  ,
  AMOUNT_OF_COPAY_ACTION_CODE VARCHAR(1)  ,
  PATIENT_PAY_AMOUNT VARCHAR(10)  ,
  PATIENT_PAY_AMOUNT_ACTION_CODE VARCHAR(1)  ,
  AMOUNT_ATTRIBUTED_TO_PRODUCT_SELECTION_BRAND VARCHAR(10)  ,
  AMOUNT_ATTRIBUTED_TO_PRODUCT_SELECTION_BRAND_ACTION_CODE VARCHAR(1)  ,
  AMOUNT_ATTRIBUTED_TO_SALES_TAX VARCHAR(10)  ,
  AMOUNT_ATTRIBUTED_TO_SALES_TAX_ACTION_CODE VARCHAR(1)  ,
  AMOUNT_ATTRIBUTED_TO_PROCESSOR_FEE VARCHAR(10)  ,
  AMOUNT_ATTRIBUTED_TO_PROCESSOR_FEE_ACTION_CODE VARCHAR(1)  ,
  GROSS_AMOUNT_DUE VARCHAR(10)  ,
  GROSS_AMOUNT_DUE_ACTION_CODE VARCHAR(1)  ,
  INVOICED_AMOUNT VARCHAR(10)  ,
  INVOICED_AMOUNT_ACTION_CODE VARCHAR(1)  ,
  PENALTY_AMOUNT VARCHAR(10)  ,
  PENALTY_AMOUNT_ACTION_CODE VARCHAR(1)  ,
  RESERVED_6 VARCHAR(23) NOT NULL ,
  PRODUCT_SERVICE_IDENTIFIER_QUALIFIER VARCHAR(2)  ,
  PRODUCT_SERVICE_IDENTIFIER VARCHAR(19)  ,
  DAYS_SUPPLY VARCHAR(3)  ,
  QUANTITY_DISPENSED VARCHAR(10)  ,
  PRODUCT_SERVICE_NAME VARCHAR(30)  ,
  BRAND_GENERIC_INDICATOR VARCHAR(1)  ,
  THERAPEUTIC_CLASS_CODE_QUALIFIER VARCHAR(1)  ,
  THERAPEUTIC_CLASS_CODE VARCHAR(17)  ,
  DISPENSED_AS_WRITTEN VARCHAR(1)  ,
  RESERVED_7 VARCHAR(48) NOT NULL ,
  SOURCE_CODE VARCHAR(50) NOT NULL,
  JOB_KEY BIGINT NOT NULL ,
  FILE_NAME VARCHAR(1024) NOT NULL,
  CREATED_BY VARCHAR(100) NOT NULL ,
  CREATED_TIMESTAMP TIMESTAMP NOT NULL ,
  UPDATED_BY VARCHAR(100) ,
  UPDATED_TIMESTAMP TIMESTAMP,
CONSTRAINT IDX_PK_ACCUMULATOR_DETAIL PRIMARY KEY (ACCUMULATOR_DETAIL_KEY)
)
DISTSTYLE KEY
DISTKEY (ACCUMULATOR_DETAIL_KEY);

/*==============================================================*/
/* Table: ACCUMULATOR_DETAIL_VALIDATION_RESULT									*/
/*==============================================================*/

CREATE TABLE AHUB_DW.ACCUMULATOR_DETAIL_VALIDATION_RESULT
(
  ACCUMULATOR_DETAIL_KEY BIGINT IDENTITY(1,1) NOT NULL,
  VALIDATION_SEQUENCE_NUMBER INT NOT NULL ,
  CLIENT_RULE_KEY BIGINT NOT NULL ,
  VALIDATION_PASSED_FLAG CHAR(1) ,
  CREATED_BY VARCHAR(100) NOT NULL ,
  CREATED_TIMESTAMP TIMESTAMP NOT NULL ,
  UPDATED_BY VARCHAR(100) ,
  UPDATED_TIMESTAMP TIMESTAMP,
CONSTRAINT IDX_PK_ACCUMULATOR_DETAIL_VALIDATION_RESULT PRIMARY KEY (ACCUMULATOR_DETAIL_KEY,VALIDATION_SEQUENCE_NUMBER,CLIENT_RULE_KEY),
CONSTRAINT FK_CLIENT_RULE_REFERENCE_ACCUMULATOR_DETAIL_VALIDATION_RESULT foreign key(CLIENT_RULE_KEY) references AHUB_DW.CLIENT_RULE_REFERENCE(CLIENT_RULE_KEY),
CONSTRAINT FK_ACCUMULATOR_DETAIL_ACCUMULATOR_DETAIL_VALIDATION_RESULT foreign key(ACCUMULATOR_DETAIL_KEY) references AHUB_DW.ACCUMULATOR_DETAIL(ACCUMULATOR_DETAIL_KEY)
)
DISTSTYLE KEY
DISTKEY (ACCUMULATOR_DETAIL_KEY);