## this whole script updates the columns that got introduced by ahub_390.sql and ahub_437.sql ######
## run below update sqls in SIT/ UAT / PROD environment as environment related variables do not exist in this script

-----drop existing fields(introduced in ahub_390.sql as part of 10/16 release) that convey incorrect meaning or does partial functionality

alter table ahub_dw.client_files
DROP validate_file_string cascade;
alter table ahub_dw.client_files
DROP validate_detail_records_in_file_structure cascade;

-----add below fields that convey correct meaning and does complete functionality (earlier these were strings)

alter table ahub_dw.client_files
ADD validate_file_structure boolean default false;
alter table ahub_dw.client_files
ADD validate_file_detail_columns boolean default false;
alter table ahub_dw.client_files
ADD generate_outbound_file boolean default false;
alter table ahub_dw.client_files
ADD file_category VARCHAR(100) ENCODE lzo;

-----update newly added fields for client_file_id = 1

update ahub_dw.client_files
set
validate_file_structure = True
,validate_file_detail_columns = True
,file_category = 'Commercial'
where client_file_id in (1);

-----update newly added fields for client_file_id = 7 (inbound BCI error file)

update ahub_dw.client_files
set
validate_file_structure = True
,validate_file_detail_columns = False
,process_name = 'BCI Error File to Database'
where client_file_id in (7);

-----update newly added fields for client_file_id = 3 (inbound CVS file)

update ahub_dw.client_files
set
validate_file_structure = True
,validate_file_detail_columns = True
,file_category = 'Integrated'
where client_file_id in (3);

-----update newly added fields for client_file_id = 6 (inbound CVS Recon file)

update ahub_dw.client_files
set
validate_file_structure = True
,validate_file_detail_columns = False
,file_category = 'Integrated'
where client_file_id in (6);

------ update newly added fields for client_file_id = 8 (inbound CVS ACCHIST file)

update ahub_dw.client_files
set
validate_file_structure = False
,validate_file_detail_columns = False
where client_file_id in (8);

-----update newly added fields for client_file_id = 9 in client_files table

update ahub_dw.client_files
set
validate_file_structure = True
,validate_file_detail_columns = True
,file_category = 'Government Programs'
where client_file_id in (9);


------updating existing field names to convey right meaning

alter table ahub_dw.client_files
RENAME is_alert_active to is_data_error_alert_active;

alter table ahub_dw.client_files
RENAME alert_threshold to data_error_threshold;

alter table ahub_dw.client_files
RENAME alert_notification_arn to data_error_notification_arn;

alter table ahub_dw.client_files
RENAME current_timezone to current_timezone_abbreviation;

------update adhoc sqls (prod fixes found as part of 10/16 release)

update ahub_dw.client_files
set
frequency_type = 'Daily'
,frequency_count = 1
where client_file_id in (2);

update ahub_dw.client_files
set
frequency_type = 'Adhoc'
where client_file_id in (8);

update ahub_dw.client_files
set
process_name = 'CVS Recon Integrated file to Database'
where client_file_id in (6);

update ahub_dw.client_files
set
process_name = 'BCI Error File to Database'
where client_file_id in (7);

