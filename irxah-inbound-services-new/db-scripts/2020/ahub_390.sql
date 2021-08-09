-----drop invalid columns from client_files and job tables

alter table ahub_dw.client_files
DROP sender_identifier cascade;
alter table ahub_dw.client_files
DROP receiver_identifier cascade;
alter table ahub_dw.job
DROP sender_identifier cascade;
alter table ahub_dw.job
DROP receiver_identifier cascade;

-----add columns to existing client_files table (ADD COLUMN supports adding only one column in each ALTER TABLE statement)

alter table ahub_dw.client_files
ADD environment_level VARCHAR(50)  ENCODE lzo;
alter table ahub_dw.client_files
ADD archive_folder VARCHAR(256)  ENCODE lzo;
alter table ahub_dw.client_files
ADD error_folder VARCHAR(256)  ENCODE lzo;
alter table ahub_dw.client_files
ADD structure_specification_file VARCHAR(256)  ENCODE lzo;
alter table ahub_dw.client_files
ADD expected_line_length INTEGER ENCODE az64;
alter table ahub_dw.client_files
ADD iam_arn VARCHAR(100)  ENCODE lzo;
alter table ahub_dw.client_files
ADD s3_merge_output_path VARCHAR(256)  ENCODE lzo;
alter table ahub_dw.client_files
ADD file_type_column_in_outbound_file VARCHAR(50)  ENCODE lzo;
alter table ahub_dw.client_files
ADD transmission_file_type VARCHAR(50)  ENCODE lzo;
alter table ahub_dw.client_files
ADD input_sender_id VARCHAR(50)  ENCODE lzo;
alter table ahub_dw.client_files
ADD input_receiver_id VARCHAR(50)  ENCODE lzo;
alter table ahub_dw.client_files
ADD output_sender_id VARCHAR(50)  ENCODE lzo;
alter table ahub_dw.client_files
ADD output_receiver_id VARCHAR(50)  ENCODE lzo;
alter table ahub_dw.client_files
ADD s3_output_file_name_prefix VARCHAR(256)  ENCODE lzo;
alter table ahub_dw.client_files
ADD file_processing_location VARCHAR(256)  ENCODE lzo;
alter table ahub_dw.client_files
ADD position_specification_file VARCHAR(256)  ENCODE lzo;
alter table ahub_dw.client_files
ADD validate_file_string VARCHAR(50)  ENCODE lzo;
alter table ahub_dw.client_files
ADD validate_detail_records_in_file_structure VARCHAR(50)  ENCODE lzo;
alter table ahub_dw.client_files
ADD process_name VARCHAR(100)  ENCODE lzo;
alter table ahub_dw.client_files
ADD process_description VARCHAR(500)  ENCODE lzo;
alter table ahub_dw.client_files
ADD reserved_variable VARCHAR(100)  ENCODE lzo;

-----add new columns to existing job table. practically these are replacements of earlier sender_identifier & receiver_identifier columns

alter table ahub_dw.job
ADD input_sender_id VARCHAR(50)  ENCODE lzo;
alter table ahub_dw.job
ADD input_receiver_id VARCHAR(50)  ENCODE lzo;


## STOP HERE AND READ BELOW.
################# search by key word GOTO<env> to directly search the sqls that to be run as per environment (SIT,UAT,PROD) ###################

## GOTOSIT
## run below sqls in SIT environment only

-----update newly added fields for client_file_id = 1

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
,process_name = 'BCI Integrated file to CVS'
,process_description = 'Validate inbound BCI Integrated file, split, load into redshift database and generate a CVS outbound file'
where client_file_id in (1);

-----update newly added fields for client_file_id = 7 (inbound BCI error file)

update ahub_dw.client_files
set
environment_level = 'SIT'			--varies as per env
,archive_folder = 'inbound/bci/archive'
,error_folder = 'inbound/bci/error'
,structure_specification_file = 'inbound/bci/specification/bcierrorpositions.csv'
,expected_line_length = 1700
,iam_arn = 'arn:aws:iam::795038802291:role/irx-accum-phi-redshift-glue'			--varies as per env
,file_processing_location = 'inbound/bci/temp'
,position_specification_file = 'inbound/bci/specification/bci_range.txt'
,validate_file_string = 'FALSE'
,validate_detail_records_in_file_structure = 'REQUIRED'   ---this field id used by bci error file only, rest all flow have null value
,input_sender_id = '20500BCIDAHO'
,input_receiver_id = '00489INGENIORX'
,process_name = 'Store BCI Error File in Database'
,process_description = 'Validate inbound BCI Error file, split, load into redshift database'
where client_file_id in (7);

-----update newly added fields for client_file_id = 3 (inbound CVS file)

update ahub_dw.client_files
set
environment_level = 'SIT'			--varies as per env
,archive_folder = 'inbound/cvs/archive'
,error_folder = 'inbound/cvs/error'
,structure_specification_file = 'inbound/cvs/specification/cvspositions.csv'
,expected_line_length = 1700
,iam_arn = 'arn:aws:iam::795038802291:role/irx-accum-phi-redshift-glue'			--varies as per env
,file_processing_location = 'inbound/cvs/temp'
,position_specification_file = 'inbound/cvs/specification/cvs_range.txt'
,validate_file_string = 'TRUE'
,validate_detail_records_in_file_structure = ''
,input_sender_id = '00990CAREMARK'
,input_receiver_id = '00489INGENIORX'
,process_name = 'CVS Integrated file to Database'
,process_description = 'Validate inbound CVS Integrated file, split, load into redshift database'
where client_file_id in (3);

-----update newly added fields for client_file_id = 4 (outbound BCI file)

update ahub_dw.client_files
set
environment_level = 'SIT'			--varies as per env
,iam_arn = 'arn:aws:iam::795038802291:role/irx-accum-phi-redshift-glue'			--varies as per env
,file_processing_location = 'outbound/bci/temp'
,s3_merge_output_path = 'outbound/bci/txtfiles'
,file_type_column_in_outbound_file = 'T'			--varies as per env, its P in Prod and rest all envs T
,input_sender_id = '00990CAREMARK'
,output_sender_id = '00489INGENIORX'
,output_receiver_id = '20500BCIDAHO'
,process_name = 'Export CVS File from Database'
,process_description = 'From redshift database export file to BCI'
where client_file_id in (4);

-----update newly added fields for client_file_id = 6 (inbound CVS Recon file)

update ahub_dw.client_files
set
environment_level = 'SIT'			--varies as per env
,archive_folder = 'inbound/cvs/archive'
,error_folder = 'inbound/cvs/error'
,structure_specification_file = 'inbound/cvs/specification/reconpositions.csv'
,expected_line_length = 800
,iam_arn = 'arn:aws:iam::795038802291:role/irx-accum-phi-redshift-glue'			--varies as per env
,file_processing_location = 'inbound/cvs/temp'
,position_specification_file = 'inbound/cvs/specification/recon_range.txt'
,validate_file_string = 'FALSE'
,validate_detail_records_in_file_structure = ''
,input_sender_id = '00990CAREMARK'
,input_receiver_id = '00489INGENIORX'
,process_name = 'Recon Integrated file to CVS'
,process_description = 'Validate inbound Recon Integrated file, split, load into redshift database'
where client_file_id in (6);

------ inserting new record to accommodate CVS ACCHIST file with client_file_id = 8 (name_pattern varies for prod)

INSERT INTO ahub_dw.client_files (client_file_id,client_id,name_pattern,file_type,created_by,created_timestamp,updated_by,updated_timestamp,import_columns,alert_notification_arn,alert_threshold,is_alert_active,frequency_type,frequency_count,total_files_per_day,grace_period,poll_time_in_24_hour_format,sla_notification_arn,processing_notification_arn,file_timezone,process_duration_threshold,file_description,long_description,outbound_file_generation_notification_arn,outbound_successful_acknowledgement,current_timezone) VALUES
(8,2,'ACCHIST_TST_BCIINRX_YYMMDDHHMMSS','INBOUND','AHUB','2020-03-26 10:36:35.412',NULL,NULL,NULL,'arn:aws:sns:us-east-1:795038802291:irx_ahub_error_notification',1.00,false,'',0,0,00,'00:00','795038802291:irx_ahub_sla_notification','795038802291:irx_ahub_processing_notification','America/New_York',300,'CVS Accumulation History','Path : CVS → CVS
File Pattern : ACCHIST_TST_BCIINRX_YYMMDDHHMMSS (UAT)
ACCHIST_BCI_INRX_YYMMDDHHMMSS (PROD)','795038802291:irx_ahub_outbound_file_generation_notification',false,'NA')

------ update newly added fields for client_file_id = 8 (inbound CVS ACCHIST file)

update ahub_dw.client_files
set
environment_level = 'SIT'			--varies as per env
,archive_folder = 'inbound/cvs/archive'
,error_folder = 'inbound/cvs/error'
,structure_specification_file = 'inbound/cvs/specification/cvspositions.csv'
,expected_line_length = 1700
,iam_arn = 'arn:aws:iam::795038802291:role/irx-accum-phi-redshift-glue'			--varies as per env
,file_processing_location = 'inbound/cvs/temp'
,position_specification_file = 'inbound/cvs/specification/accumhist_range.txt'
,validate_file_string = 'FALSE'
,s3_merge_output_path = 'outbound/cvs/history'
,file_type_column_in_outbound_file = 'T'			--varies as per env, its P in Prod and rest all envs T
,input_sender_id = '00489INGENIORX'
,input_receiver_id = '20500BCIDAHO'
,s3_output_file_name_prefix = 'ACCHIST_TSTLD_BCIINRX_'			--varies as per env
,reserved_variable = 'inbound/cvs/specification/crosswalk_file.txt'
,process_name = 'CVS Accum History file to CVS'
,process_description = 'Skip Validation of inbound CVS Accum history file, split, load into redshift database and generate a CVS outbound file'
where client_file_id in (8);


## GOTOUAT
## run below sqls in UAT environment only

-----update newly added fields for client_file_id = 1

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
,process_name = 'BCI Integrated file to CVS'
,process_description = 'Validate inbound BCI Integrated file, split, load into redshift database and generate a CVS outbound file'
where client_file_id in (1);


-----update newly added fields for client_file_id = 7 (inbound BCI error file)

update ahub_dw.client_files
set
environment_level = 'PRE-PROD'			--varies as per env
,archive_folder = 'inbound/bci/archive'
,error_folder = 'inbound/bci/error'
,structure_specification_file = 'inbound/bci/specification/bcierrorpositions.csv'
,expected_line_length = 1700
,iam_arn = 'arn:aws:iam::974356221399:role/irx-accum-phi-redshift-glue'			--varies as per env
,file_processing_location = 'inbound/bci/temp'
,position_specification_file = 'inbound/bci/specification/bci_range.txt'
,validate_file_string = 'FALSE'
,validate_detail_records_in_file_structure = 'REQUIRED'   ---this field id used by bci error file only, rest all flow have null value
,input_sender_id = '20500BCIDAHO'
,input_receiver_id = '00489INGENIORX'
,process_name = 'Store BCI Error File in Database'
,process_description = 'Validate inbound BCI Error file, split, load into redshift database'
where client_file_id in (7);


-----update newly added fields for client_file_id = 3 (inbound CVS file)

update ahub_dw.client_files
set
environment_level = 'PRE-PROD'			--varies as per env
,archive_folder = 'inbound/cvs/archive'
,error_folder = 'inbound/cvs/error'
,structure_specification_file = 'inbound/cvs/specification/cvspositions.csv'
,expected_line_length = 1700
,iam_arn = 'arn:aws:iam::974356221399:role/irx-accum-phi-redshift-glue'			--varies as per env
,file_processing_location = 'inbound/cvs/temp'
,position_specification_file = 'inbound/cvs/specification/cvs_range.txt'
,validate_file_string = 'TRUE'
,validate_detail_records_in_file_structure = ''
,input_sender_id = '00990CAREMARK'
,input_receiver_id = '00489INGENIORX'
,process_name = 'CVS Integrated file to Database'
,process_description = 'Validate inbound CVS Integrated file, split, load into redshift database'
where client_file_id in (3);

-----update newly added fields for client_file_id = 4 (outbound BCI file)

update ahub_dw.client_files
set
environment_level = 'PRE-PROD'			--varies as per env
,iam_arn = 'arn:aws:iam::974356221399:role/irx-accum-phi-redshift-glue'			--varies as per env
,file_processing_location = 'outbound/bci/temp'
,s3_merge_output_path = 'outbound/bci/txtfiles'
,file_type_column_in_outbound_file = 'T'			--varies as per env, its P in Prod and rest all envs T
,input_sender_id = '00990CAREMARK'
,output_sender_id = '00489INGENIORX'
,output_receiver_id = '20500BCIDAHO'
,process_name = 'Export CVS File from Database'
,process_description = 'From redshift database export file to BCI'
where client_file_id in (4);


-----update newly added fields for client_file_id = 6 (inbound CVS Recon file)

update ahub_dw.client_files
set
environment_level = 'PRE-PROD'			--varies as per env
,archive_folder = 'inbound/cvs/archive'
,error_folder = 'inbound/cvs/error'
,structure_specification_file = 'inbound/cvs/specification/reconpositions.csv'
,expected_line_length = 800
,iam_arn = 'arn:aws:iam::974356221399:role/irx-accum-phi-redshift-glue'			--varies as per env
,file_processing_location = 'inbound/cvs/temp'
,position_specification_file = 'inbound/cvs/specification/recon_range.txt'
,validate_file_string = 'FALSE'
,validate_detail_records_in_file_structure = ''
,input_sender_id = '00990CAREMARK'
,input_receiver_id = '00489INGENIORX'
,process_name = 'Recon Integrated file to CVS'
,process_description = 'Validate inbound Recon Integrated file, split, load into redshift database'
where client_file_id in (6);

------ inserting new record to accommodate CVS ACCHIST file with client_file_id = 8 (name_pattern varies for prod)

INSERT INTO ahub_dw.client_files (client_file_id,client_id,name_pattern,file_type,created_by,created_timestamp,updated_by,updated_timestamp,import_columns,alert_notification_arn,alert_threshold,is_alert_active,frequency_type,frequency_count,total_files_per_day,grace_period,poll_time_in_24_hour_format,sla_notification_arn,processing_notification_arn,file_timezone,process_duration_threshold,file_description,long_description,outbound_file_generation_notification_arn,outbound_successful_acknowledgement,current_timezone) VALUES
(8,2,'ACCHIST_TST_BCIINRX_YYMMDDHHMMSS','INBOUND','AHUB','2020-03-26 10:36:35.412',NULL,NULL,NULL,'arn:aws:sns:us-east-1:974356221399:irx_ahub_error_notification',1.00,false,'',0,0,00,'00:00','974356221399:irx_ahub_sla_notification','974356221399:irx_ahub_processing_notification','America/New_York',300,'CVS Accumulation History','Path : CVS → CVS
File Pattern : ACCHIST_TST_BCIINRX_YYMMDDHHMMSS (UAT)
ACCHIST_BCI_INRX_YYMMDDHHMMSS (PROD)','974356221399:irx_ahub_outbound_file_generation_notification',false,'NA')

------ update newly added fields for client_file_id = 8 (inbound CVS ACCHIST file)

update ahub_dw.client_files
set
environment_level = 'PRE-PROD'			--varies as per env
,archive_folder = 'inbound/cvs/archive'
,error_folder = 'inbound/cvs/error'
,structure_specification_file = 'inbound/cvs/specification/cvspositions.csv'
,expected_line_length = 1700
,iam_arn = 'arn:aws:iam::974356221399:role/irx-accum-phi-redshift-glue'			--varies as per env
,file_processing_location = 'inbound/cvs/temp'
,position_specification_file = 'inbound/cvs/specification/accumhist_range.txt'
,validate_file_string = 'FALSE'
,s3_merge_output_path = 'outbound/cvs/history'
,file_type_column_in_outbound_file = 'T'			--varies as per env, its P in Prod and rest all envs T
,input_sender_id = '00489INGENIORX'
,input_receiver_id = '20500BCIDAHO'
,s3_output_file_name_prefix = 'ACCHIST_TSTLD_BCIINRX_'			--varies as per env
,reserved_variable = 'inbound/cvs/specification/crosswalk_file.txt'
,process_name = 'CVS Accum History file to CVS'
,process_description = 'Skip Validation of inbound CVS Accum history file, split, load into redshift database and generate a CVS outbound file'
where client_file_id in (8);


## GOTOPROD
## run below sqls in PROD environment only

-----update newly added fields for client_file_id = 1

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
,process_name = 'BCI Integrated file to CVS'
,process_description = 'Validate inbound BCI Integrated file, split, load into redshift database and generate a CVS outbound file'
where client_file_id in (1);


-----update newly added fields for client_file_id = 7 (inbound BCI error file)

update ahub_dw.client_files
set
environment_level = 'PROD'			--varies as per env
,archive_folder = 'inbound/bci/archive'
,error_folder = 'inbound/bci/error'
,structure_specification_file = 'inbound/bci/specification/bcierrorpositions.csv'
,expected_line_length = 1700
,iam_arn = 'arn:aws:iam::206388457288:role/irx-accum-phi-redshift-glue'			--varies as per env
,file_processing_location = 'inbound/bci/temp'
,position_specification_file = 'inbound/bci/specification/bci_range.txt'
,validate_file_string = 'FALSE'
,validate_detail_records_in_file_structure = 'REQUIRED'   ---this field id used by bci error file only, rest all flow have null value
,input_sender_id = '20500BCIDAHO'
,input_receiver_id = '00489INGENIORX'
,process_name = 'Store BCI Error File in Database'
,process_description = 'Validate inbound BCI Error file, split, load into redshift database'
where client_file_id in (7);


-----update newly added fields for client_file_id = 3 (inbound CVS file)

update ahub_dw.client_files
set
environment_level = 'PROD'			--varies as per env
,archive_folder = 'inbound/cvs/archive'
,error_folder = 'inbound/cvs/error'
,structure_specification_file = 'inbound/cvs/specification/cvspositions.csv'
,expected_line_length = 1700
,iam_arn = 'arn:aws:iam::206388457288:role/irx-accum-phi-redshift-glue'			--varies as per env
,file_processing_location = 'inbound/cvs/temp'
,position_specification_file = 'inbound/cvs/specification/cvs_range.txt'
,validate_file_string = 'TRUE'
,validate_detail_records_in_file_structure = ''
,input_sender_id = '00990CAREMARK'
,input_receiver_id = '00489INGENIORX'
,process_name = 'CVS Integrated file to Database'
,process_description = 'Validate inbound CVS Integrated file, split, load into redshift database'
where client_file_id in (3);

-----update newly added fields for client_file_id = 4 (outbound BCI file)

update ahub_dw.client_files
set
environment_level = 'PROD'			--varies as per env
,iam_arn = 'arn:aws:iam::206388457288:role/irx-accum-phi-redshift-glue'			--varies as per env
,file_processing_location = 'outbound/bci/temp'
,s3_merge_output_path = 'outbound/bci/txtfiles'
,file_type_column_in_outbound_file = 'P'			--varies as per env, its P in Prod and rest all envs T
,input_sender_id = '00990CAREMARK'
,output_sender_id = '00489INGENIORX'
,output_receiver_id = '20500BCIDAHO'
,process_name = 'Export CVS File from Database'
,process_description = 'From redshift database export file to BCI'
where client_file_id in (4);


-----update newly added fields for client_file_id = 6 (inbound CVS Recon file)

update ahub_dw.client_files
set
environment_level = 'PROD'			--varies as per env
,archive_folder = 'inbound/cvs/archive'
,error_folder = 'inbound/cvs/error'
,structure_specification_file = 'inbound/cvs/specification/reconpositions.csv'
,expected_line_length = 800
,iam_arn = 'arn:aws:iam::206388457288:role/irx-accum-phi-redshift-glue'			--varies as per env
,file_processing_location = 'inbound/cvs/temp'
,position_specification_file = 'inbound/cvs/specification/recon_range.txt'
,validate_file_string = 'FALSE'
,validate_detail_records_in_file_structure = ''
,input_sender_id = '00990CAREMARK'
,input_receiver_id = '00489INGENIORX'
,process_name = 'Recon Integrated file to CVS'
,process_description = 'Validate inbound Recon Integrated file, split, load into redshift database'
where client_file_id in (6);

------ inserting new record to accommodate CVS ACCHIST file with client_file_id = 8 (name_pattern varies for prod)

INSERT INTO ahub_dw.client_files (client_file_id,client_id,name_pattern,file_type,created_by,created_timestamp,updated_by,updated_timestamp,import_columns,alert_notification_arn,alert_threshold,is_alert_active,frequency_type,frequency_count,total_files_per_day,grace_period,poll_time_in_24_hour_format,sla_notification_arn,processing_notification_arn,file_timezone,process_duration_threshold,file_description,long_description,outbound_file_generation_notification_arn,outbound_successful_acknowledgement,current_timezone) VALUES
(8,2,'ACCHIST_BCI_INRX_YYMMDDHHMMSS','INBOUND','AHUB','2020-03-26 10:36:35.412',NULL,NULL,NULL,'arn:aws:sns:us-east-1:206388457288:irx_ahub_error_notification',1.00,false,'',0,0,00,'00:00','206388457288:irx_ahub_sla_notification','206388457288:irx_ahub_processing_notification','America/New_York',300,'CVS Accumulation History','Path : CVS → CVS
File Pattern : ACCHIST_TST_BCIINRX_YYMMDDHHMMSS (UAT)
ACCHIST_BCI_INRX_YYMMDDHHMMSS (PROD)','206388457288:irx_ahub_outbound_file_generation_notification',false,'NA')

------ update newly added fields for client_file_id = 8 (inbound CVS ACCHIST file)

update ahub_dw.client_files
set
environment_level = 'PROD'			--varies as per env
,archive_folder = 'inbound/cvs/archive'
,error_folder = 'inbound/cvs/error'
,structure_specification_file = 'inbound/cvs/specification/cvspositions.csv'
,expected_line_length = 1700
,iam_arn = 'arn:aws:iam::206388457288:role/irx-accum-phi-redshift-glue'			--varies as per env
,file_processing_location = 'inbound/cvs/temp'
,position_specification_file = 'inbound/cvs/specification/accumhist_range.txt'
,validate_file_string = 'FALSE'
,s3_merge_output_path = 'outbound/cvs/history'
,file_type_column_in_outbound_file = 'P'			--varies as per env, its P in Prod and rest all envs T
,input_sender_id = '00489INGENIORX'
,input_receiver_id = '20500BCIDAHO'
,s3_output_file_name_prefix = 'ACCHIST_LDBCI_INRX_'			--varies as per env
,reserved_variable = 'inbound/cvs/specification/crosswalk_file.txt'
,process_name = 'CVS Accum History file to CVS'
,process_description = 'Skip Validation of inbound CVS Accum history file, split, load into redshift database and generate a CVS outbound file'
where client_file_id in (8);


---------Create user 'edc_metadata_scan_out' in UAT and PROD

/==============================================================/
/* Create the read only user (business user) */
/==============================================================/

create user edc_metadata_scan_out password  'Ad5153c434b4';

/==============================================================/
/* Alter group add new business user to it */
/==============================================================/

ALTER GROUP business_user_ro_group ADD USER edc_metadata_scan_out;


############################### THE END ################################
