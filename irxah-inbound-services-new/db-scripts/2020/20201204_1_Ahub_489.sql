-- ahub_542
-------------add new columns

alter table ahub_dw.client_files ADD file_extraction_error_notification_sns VARCHAR(256) ENCODE lzo;
alter table ahub_dw.client_files ADD is_active boolean default false;
alter table ahub_dw.client_files ADD glue_job_name VARCHAR(100) ENCODE lzo;

-- ahub_543
-------------add new columns

alter table ahub_dw.client_files ADD zip_file boolean default false;
alter table ahub_dw.client_files ADD deliver_files_to_client boolean default false;
alter table ahub_dw.client_files ADD destination_bucket_cfx VARCHAR(100) ENCODE lzo;
alter table ahub_dw.client_files ADD destination_folder_cfx VARCHAR(100) ENCODE lzo;

-----------add new column into client table

alter table ahub_dw.client ADD file_recipient_client_name VARCHAR(100);

-----------update newly added columns

update ahub_dw.client_files
set
file_extraction_error_notification_sns = 'irx_ahub_incoming_file_extraction_error_notification',
s3_merge_output_path = 'outbound/txtfiles',
is_active = True,
zip_file = True;

update ahub_dw.client_files
set glue_job_name = 'irxah_process_incoming_bci_file'
where client_file_id in (1,7,9);

update ahub_dw.client_files
set glue_job_name = 'irxah_process_incoming_cvs_file'
where client_file_id in (3,6,8,10,11);

update ahub_dw.client_files
set
client_id = 2
where client_file_id in (4);

update ahub_dw.client
set file_recipient_client_name = 'CVS'
where client_id in (1);

update ahub_dw.client
set file_recipient_client_name = 'BCI'
where client_id in (2);



## STOP HERE AND READ BELOW.
################# search by key word GOTO<env> to directly search the sqls that to be run as per environment (SIT,UAT,PROD) ###################

## GOTOSIT (to run sqls in SIT environment only)

-------------update values for existing columns

update ahub_dw.client_files
set
s3_output_file_name_prefix = 'ACCDLYINT_TST_INRXBCI_'
where client_file_id in (1);

update ahub_dw.client_files
set
s3_output_file_name_prefix = 'ACCDLYINT_TST_BCI_'
where client_file_id in (4);

update ahub_dw.client_files
set
s3_output_file_name_prefix = 'ACCDLYERR_TST_BCI_'
where client_file_id in (5);

update ahub_dw.client_files
set
s3_output_file_name_prefix = 'ACCHIST_TSTLD_BCIINRX_'
where client_file_id in (8);

update ahub_dw.client_files
set
s3_output_file_name_prefix = 'ACCDLYINT_TST_INRXBCI_'
where client_file_id in (9);


-----------update newly added columns client specific

update ahub_dw.client_files
set
deliver_files_to_client = False,
destination_bucket_cfx = 'irx-nonprod-bci-east-1-sftp-app-outbound',
destination_folder_cfx = 'sit/accums/'
where client_file_id in (4,5);


update ahub_dw.client_files
set
deliver_files_to_client = False,
destination_bucket_cfx = 'irx-nonprod-cvs-east-1-sftp-app-outbound',
destination_folder_cfx = 'CDH_Accumulations/'
where client_file_id in (1,9);


update ahub_dw.client_files
set
deliver_files_to_client = False,
destination_bucket_cfx = 'irx-nonprod-cvs-east-1-sftp-app-outbound',
destination_folder_cfx = 'Vendor_Transition/'
where client_file_id in (8);


## GOTOUAT (to run sqls in UAT environment only)

-------------update values for existing columns

update ahub_dw.client_files
set
s3_output_file_name_prefix = 'ACCDLYINT_TST_INRXBCI_'
where client_file_id in (1);

update ahub_dw.client_files
set
s3_output_file_name_prefix = 'ACCDLYINT_TST_BCI_'
where client_file_id in (4);

update ahub_dw.client_files
set
s3_output_file_name_prefix = 'ACCDLYERR_TST_BCI_'
where client_file_id in (5);

update ahub_dw.client_files
set
s3_output_file_name_prefix = 'ACCHIST_TSTLD_BCIINRX_'
where client_file_id in (8);

update ahub_dw.client_files
set
s3_output_file_name_prefix = 'ACCDLYINT_TST_INRXBCI_'
where client_file_id in (9);


-----------update newly added columns client specific

update ahub_dw.client_files
set
deliver_files_to_client = False,
destination_bucket_cfx = 'irx-prod-bci-east-1-sftp-app-outbound',
destination_folder_cfx = 'preprod/accums/'
where client_file_id in (4,5);


update ahub_dw.client_files
set
deliver_files_to_client = False,
destination_bucket_cfx = 'irx-prod-cvs-cte-east-1-sftp-app-outbound',
destination_folder_cfx = 'CDH_Accumulations/'
where client_file_id in (1,9);


update ahub_dw.client_files
set
deliver_files_to_client = False,
destination_bucket_cfx = 'irx-prod-cvs-cte-east-1-sftp-app-outbound',
destination_folder_cfx = 'Vendor_Transition/'
where client_file_id in (8);


## GOTOPROD (to run sqls in PROD environment only)

-------------update values for existing columns

update ahub_dw.client_files
set
s3_output_file_name_prefix = 'ACCDLYINT_PRD_INRXBCI_'
where client_file_id in (1);

update ahub_dw.client_files
set
s3_output_file_name_prefix = 'ACCDLYINT_PRD_BCI_'
where client_file_id in (4);

update ahub_dw.client_files
set
s3_output_file_name_prefix = 'ACCDLYERR_PRD_BCI_'
where client_file_id in (5);

update ahub_dw.client_files
set
s3_output_file_name_prefix = 'ACCHIST_LDBCI_INRX_'
where client_file_id in (8);

update ahub_dw.client_files
set
s3_output_file_name_prefix = 'ACCDLYINT_PRD_INRXBCI_'
where client_file_id in (9);


-----------update newly added columns client specific

update ahub_dw.client_files
set
deliver_files_to_client = True,
destination_bucket_cfx = 'irx-prod-bci-east-1-sftp-app-outbound',
destination_folder_cfx = 'preprod/accums/'
where client_file_id in (4,5);


update ahub_dw.client_files
set
deliver_files_to_client = True,
destination_bucket_cfx = 'irx-prod-cvs-cte-east-1-sftp-app-outbound',
destination_folder_cfx = 'CDH_Accumulations/'
where client_file_id in (1,9);


update ahub_dw.client_files
set
deliver_files_to_client = True,
destination_bucket_cfx = 'irx-prod-cvs-cte-east-1-sftp-app-outbound',
destination_folder_cfx = 'Vendor_Transition/'
where client_file_id in (8);