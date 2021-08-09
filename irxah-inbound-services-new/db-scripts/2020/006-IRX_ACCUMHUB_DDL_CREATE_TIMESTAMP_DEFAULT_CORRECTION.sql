/* Add new CREATE_TIMESTAMP column with UTC and default sysdate*/

ALTER TABLE ahub_dw.client                                  ADD COLUMN CREATED_TIMESTAMP_UTC TIMESTAMP without time zone default sysdate NOT NULL;
ALTER TABLE ahub_dw.client_files							ADD COLUMN CREATED_TIMESTAMP_UTC TIMESTAMP without time zone default sysdate NOT NULL;
ALTER TABLE ahub_dw.file_columns							ADD COLUMN CREATED_TIMESTAMP_UTC TIMESTAMP without time zone default sysdate NOT NULL;
ALTER TABLE ahub_dw.column_error_codes						ADD COLUMN CREATED_TIMESTAMP_UTC TIMESTAMP without time zone default sysdate NOT NULL;
ALTER TABLE ahub_dw.column_rules							ADD COLUMN CREATED_TIMESTAMP_UTC TIMESTAMP without time zone default sysdate NOT NULL;
ALTER TABLE ahub_dw.job										ADD COLUMN CREATED_TIMESTAMP_UTC TIMESTAMP without time zone default sysdate NOT NULL;
ALTER TABLE ahub_dw.job_detail								ADD COLUMN CREATED_TIMESTAMP_UTC TIMESTAMP without time zone default sysdate NOT NULL;
ALTER TABLE ahub_dw.file_validation_result					ADD COLUMN CREATED_TIMESTAMP_UTC TIMESTAMP without time zone default sysdate NOT NULL;
ALTER TABLE AHUB_DW.ACCUMULATOR_RECONCILIATION_DETAIL		ADD COLUMN CREATED_TIMESTAMP_UTC TIMESTAMP without time zone default sysdate NOT NULL;
ALTER TABLE AHUB_DW.ACCUMULATOR_DETAIL						ADD COLUMN CREATED_TIMESTAMP_UTC TIMESTAMP without time zone default sysdate NOT NULL;


/*Update new column CREATED_TIMESTAMP_UTC with existing column CREATED_TIMESTAMP after converting from ET to UTC*/

UPDATE ahub_dw.client                               SET CREATED_TIMESTAMP_UTC = convert_timezone('US/Eastern', 'UTC', created_timestamp) ;                  
UPDATE ahub_dw.client_files							SET CREATED_TIMESTAMP_UTC = convert_timezone('US/Eastern', 'UTC', created_timestamp) ;
UPDATE ahub_dw.file_columns							SET CREATED_TIMESTAMP_UTC = convert_timezone('US/Eastern', 'UTC', created_timestamp) ;
UPDATE ahub_dw.column_error_codes					SET CREATED_TIMESTAMP_UTC = convert_timezone('US/Eastern', 'UTC', created_timestamp) ;
UPDATE ahub_dw.column_rules							SET CREATED_TIMESTAMP_UTC = convert_timezone('US/Eastern', 'UTC', created_timestamp) ;
UPDATE ahub_dw.job									SET CREATED_TIMESTAMP_UTC = convert_timezone('US/Eastern', 'UTC', created_timestamp) ;
UPDATE ahub_dw.job_detail							SET CREATED_TIMESTAMP_UTC = convert_timezone('US/Eastern', 'UTC', created_timestamp) ;
UPDATE ahub_dw.file_validation_result				SET CREATED_TIMESTAMP_UTC = convert_timezone('US/Eastern', 'UTC', created_timestamp) ;
UPDATE AHUB_DW.ACCUMULATOR_RECONCILIATION_DETAIL	SET CREATED_TIMESTAMP_UTC = convert_timezone('US/Eastern', 'UTC', created_timestamp) ;
UPDATE AHUB_DW.ACCUMULATOR_DETAIL					SET CREATED_TIMESTAMP_UTC = convert_timezone('US/Eastern', 'UTC', created_timestamp) ;

/*Remove existing column CREATED_TIMESTAMP*/

ALTER TABLE ahub_dw.client                                  DROP COLUMN CREATED_TIMESTAMP CASCADE;
ALTER TABLE ahub_dw.client_files							DROP COLUMN CREATED_TIMESTAMP CASCADE;
ALTER TABLE ahub_dw.file_columns							DROP COLUMN CREATED_TIMESTAMP CASCADE;
ALTER TABLE ahub_dw.column_error_codes						DROP COLUMN CREATED_TIMESTAMP CASCADE;
ALTER TABLE ahub_dw.column_rules							DROP COLUMN CREATED_TIMESTAMP CASCADE;
ALTER TABLE ahub_dw.job										DROP COLUMN CREATED_TIMESTAMP CASCADE;
ALTER TABLE ahub_dw.job_detail								DROP COLUMN CREATED_TIMESTAMP CASCADE;
ALTER TABLE ahub_dw.file_validation_result					DROP COLUMN CREATED_TIMESTAMP CASCADE;
ALTER TABLE AHUB_DW.ACCUMULATOR_RECONCILIATION_DETAIL		DROP COLUMN CREATED_TIMESTAMP CASCADE;
ALTER TABLE AHUB_DW.ACCUMULATOR_DETAIL						DROP COLUMN CREATED_TIMESTAMP CASCADE;

/*Rename the new column CREATED_TIMESTAMP_UTC with CREATED_TIMESTAMP*/

ALTER TABLE ahub_dw.client                                  RENAME COLUMN CREATED_TIMESTAMP_UTC TO CREATED_TIMESTAMP;
ALTER TABLE ahub_dw.client_files							RENAME COLUMN CREATED_TIMESTAMP_UTC TO CREATED_TIMESTAMP;
ALTER TABLE ahub_dw.file_columns							RENAME COLUMN CREATED_TIMESTAMP_UTC TO CREATED_TIMESTAMP;
ALTER TABLE ahub_dw.column_error_codes						RENAME COLUMN CREATED_TIMESTAMP_UTC TO CREATED_TIMESTAMP;
ALTER TABLE ahub_dw.column_rules							RENAME COLUMN CREATED_TIMESTAMP_UTC TO CREATED_TIMESTAMP;
ALTER TABLE ahub_dw.job										RENAME COLUMN CREATED_TIMESTAMP_UTC TO CREATED_TIMESTAMP;
ALTER TABLE ahub_dw.job_detail								RENAME COLUMN CREATED_TIMESTAMP_UTC TO CREATED_TIMESTAMP;
ALTER TABLE ahub_dw.file_validation_result					RENAME COLUMN CREATED_TIMESTAMP_UTC TO CREATED_TIMESTAMP;
ALTER TABLE AHUB_DW.ACCUMULATOR_RECONCILIATION_DETAIL		RENAME COLUMN CREATED_TIMESTAMP_UTC TO CREATED_TIMESTAMP;
ALTER TABLE AHUB_DW.ACCUMULATOR_DETAIL						RENAME COLUMN CREATED_TIMESTAMP_UTC TO CREATED_TIMESTAMP;


/* Run below commands after any change on table DDL*/

/*==============================================================*/
/* --Grant usage access on the schema to read only group        */
/*==============================================================*/

GRANT USAGE ON SCHEMA "ahub_dw" TO GROUP business_user_ro_group;

/*==============================================================*/
/* Grant SELECT access on all tables to read only group         */
/*==============================================================*/

GRANT SELECT ON ALL TABLES IN SCHEMA "ahub_dw" TO GROUP business_user_ro_group;