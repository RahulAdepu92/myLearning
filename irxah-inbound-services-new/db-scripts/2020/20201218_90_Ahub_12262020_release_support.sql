---RUN BELOW IN PRODUCTION ONLY ---------

-- BCI Inbound Setup..
UPDATE ahub_dw.client_files
SET extract_folder = 'inbound/bci/txtfiles',
archive_folder = 'inbound/bci/archive',
error_folder = 'inbound/bci/error',
file_processing_location ='inbound/bci/temp',
s3_merge_output_path='',
s3_output_file_name_prefix='',
glue_job_name='irxah_process_incoming_file_load_in_database',
destination_bucket_cfx ='' ,  
destination_folder_cfx ='',
deliver_files_to_client=false,
zip_file=false, -- applies only to the outbound file 
validate_file_structure=true,
validate_file_detail_columns=true,
position_specification_file='inbound/specifications/position_specification.txt',
structure_specification_file='inbound/specifications/bci_structure_specification.csv',
is_data_error_alert_active=True,
generate_outbound_file=false,
error_file=false
WHERE client_file_id in 
(
    1 -- BCI Integrated Accumulation File ( Commercial)
    ,7 -- BCI Error File
    ,9  -- BCI Integrated GP File
    
)

-- Special Settings for Inbound BCI Error File ( Client File ID = 7)
UPDATE ahub_dw.client_files
SET
validate_file_detail_columns=false,
position_specification_file='inbound/specifications/bci_position_specification_error.csv',
structure_specification_file='inbound/specifications/bci_structure_specification.csv',
error_file=TRUE
where client_file_id in ( 7 ---- BCI Error File
)
-- TO DO : 
-- 1. Needs a position specification file and structure specification file setup for each 
-- inbound file in form of SQL ( similar to BCI Error file shown above)

--2 . Make sure to update the confluence page with a file.
--3 . We will also eliminate the inbound/bci/specificaiton and inbound/cvs/speification  - Update Confluence page instruction



-- For ALL CVS Inbound ..( Other than Accumulation History File)

UPDATE ahub_dw.client_files
SET extract_folder = 'inbound/cvs/txtfiles',
archive_folder = 'inbound/cvs/archive',
error_folder = 'inbound/cvs/error',
file_processing_location ='inbound/cvs/temp',
s3_merge_output_path='',
s3_output_file_name_prefix='',
glue_job_name='irxah_process_incoming_cvs_file',
destination_bucket_cfx ='' ,  
destination_folder_cfx ='',
deliver_files_to_client=false,
is_data_error_alert_active=True,
generate_outbound_file=false,
zip_file=false, -- applies only to the outbound file 
error_file=false
where  FILE_TYPE='INBOUND' and client_id in 
(
    2  --CVS
)
and client_file_id not in 
(
    8 -- CVS Accumulation History
)

-- CVS Inbound Setup ( Integrated -2 Hourly and Non Integrated Daily)
UPDATE ahub_dw.client_files
SET 
validate_file_structure=true,
validate_file_detail_columns=true,
-- Please make sure that these files are out there..in the respective folder
-- To Do : Update confluence page
position_specification_file='inbound/specifications/cvs_position_specification.txt',
structure_specification_file='inbound/specifications/cvs_structure_specification.csv'

WHERE client_file_id in 
(
    3 -- CVS  Integrated Accumulation File ( Sends every 2 hour to AHUB)
    ,11 -- CVS  Daily Integrated Accumulation File (Sent daily to AHUB)
    
    
);

-- CVS Inbound Setup ( Integrated -Recon and Non Integrated Recon)
UPDATE ahub_dw.client_files
SET 
validate_file_structure=true,
validate_file_detail_columns=true,
-- To Do : Please make sure that these files are out there..in the respective folder
-- To Do : Update confluence page
position_specification_file='inbound/specifications/cvs_recon_specification.txt',
structure_specification_file='inbound/specifications/cvs_recon_structure_specification.csv'
WHERE client_file_id in 
(
    6 -- CVS Recon Integrated File ( Sends every Sundays to AHUB)
    ,10 -- CVS Recon NON Integrated File ( Sends every Sundays to AHUB)
    
    
);



-- All  new inbound files should have the generic extract folder , archive folder
--Ensure that Gila River Inbound Custom File has s3_merge_output_path set to the inbound/txtfiles
-- The reason we do it is that we generate the standard file in s3_merge_output_path 
--and feed it again via inbound/txtfiles
UPDATE ahub_dw.client_files
SET s3_merge_output_path = 'inbound/txtfiles'
where client_file_id in (16);

-- All OUTBOUND ( Non-BCI, Non-CVS) has a zip file False ( because we deliver text file to them), Delivery to Client= False

UPDATE ahub_dw.client_files
SET  zip_file=false  
WHERE file_type='OUTBOUND' and client_id  not in (1,2);


--***** DO NOT ATTEMPT TO RUN THIS IN SIT/UAT -- ALL THE UPDATE BELOW ARE FOR PROD ONLY

--Correct destination bucket folder and destination bucket cox

-- For All Outbound BCI - Clent ID =1
UPDATE AHUB_DW.CLIENT_FILES 
 SET destination_bucket_cfx ='irx-prod-bci-east-1-sftp-app-outbound' ,  
     destination_folder_cfx ='prod/accums/'
Where client_id=1 and client_file_id in (4,5);

-- FOR CVS OUTBOUND , Client ID = 2
UPDATE AHUB_DW.CLIENT_FILES 
 SET destination_bucket_cfx ='irx-prod-cvs-east-1-sftp-app-outbound' ,  
     destination_folder_cfx ='CDH_Accumulations/'
Where client_id=2 and client_file_id in (2,22);

-- We will also hold all the outbound files affecting this release
UPDATE ahub_dw.client_files
SET
deliver_files_to_client=false
WHERE file_type='OUTBOUND'

-- FOR The Health Plan , Client ID = 6
UPDATE AHUB_DW.CLIENT_FILES 
 SET destination_bucket_cfx ='irx-prod-mfg-east-1-sftp-app-outbound' ,  
     destination_folder_cfx ='prod/accums/'
Where  client_file_id in (20,21);


-- Outbound Aetna Melton Accumulation File
UPDATE AHUB_DW.CLIENT_FILES 
 SET destination_bucket_cfx ='irx-prod-melton-aetna-east-1-sftp-app-outbound' ,  
     destination_folder_cfx ='prod/accums/'
Where client_file_id=13;

--Outbound GPA Melton Accumulation File
UPDATE AHUB_DW.CLIENT_FILES 
 SET destination_bucket_cfx ='irx-prod-melton-gpa-east-1-sftp-app-outbound' ,  
     destination_folder_cfx ='prod/accums/'
Where client_file_id=15;

--Outbound Gila River Accumulation File
UPDATE AHUB_DW.CLIENT_FILES 
 SET destination_bucket_cfx ='irx-prod-gila-east-1-sftp-app-outbound' ,  
     destination_folder_cfx ='' -- Intentionally left blank 
Where client_file_id=17;

-- Nilesh spoke with Ramesh on 12/24/2020  about the new files , Ramesh confirmed that accumulator balance 
-- qualifer should be either 04 or 05 in the new inbound files..

-- Run BELOW in PROD/PREPROD/SIT - This is a non intrusive update.
-- No harm done if you run it multiple times.
UPDATE ahub_dw.column_rules cr set list_of_values='04,05' where cr.column_rules_id in
(SELECT cr.column_rules_id
From ahub_dw.file_columns fc
Inner join ahub_dw.column_rules cr
On fc.file_column_id=cr.file_column_id 
Where fc.file_column_name like 'Accumulator Balance Qualifier%'
And cr.validation_type='REQUIRED_AND_RANGE'
And cr.is_active='Y'
And fc.client_file_id in ( 12,--  Aetna Melton Accumulation File	INBOUND
14 ,-- GPA Melton Accumulation File	INBOUND
18,--  The Health Plan Accumulation Integrated File	INBOUND
 23 --  AHUB Generated GILA River Standard File	INBOUND
  ) 
  );

--- setting environment_level in PROD

UPDATE  ahub_dw.client_files  SET
environment_level = 'PROD';

--- update validation flags for THP error
-- We do not validate the in bound error files ( hence file detail column is false) and only way to know that the file is error file is via error_file flag

update ahub_dw.client_files SET
validate_file_detail_columns = False,
error_file = True
where client_file_id in (
    7,  --- BCI Error filw
    19  -- THP Error file
);


---update INBOUND and OUTBOUND specific settings

update ahub_dw.client_files SET
outbound_successful_acknowledgement = false,		-- for inbound files, system doesnt generate success generation alerts
zip_file = false		-- for inbound files, system doesnt need zip file flag. It can be false.
where file_type = 'INBOUND';


update ahub_dw.client_files
SET
data_error_threshold = 0,  -- for outbound files, system dont detect error
is_data_error_alert_active = false,	-- for outbound files, system dont detect error
validate_file_structure = false,		-- for outbound files, validation is not required
validate_file_detail_columns = false	-- for outbound files, validation is not required
where file_type = 'OUTBOUND';

-- dropping unsued cloumn that got introduced through '20201218_1_Ahub_553_miscellaneous.sql' which has no business impact with it
alter table ahub_dw.client_files
DROP active_date ;  -- we are no longer using this field

alter table ahub_dw.client_files
DROP carrier_id ;   -- we are no longer using this field

-- cfx bucket paths no longer needed for INBOUND files. In fact they are driven now from OUTBOUND specific client_file_ids

UPDATE ahub_dw.client_files
SET
destination_bucket_cfx = '',
destination_folder_cfx = ''
WHERE client_file_id in
(
    1 -- BCI Integrated Accumulation File ( Commercial)
    ,9  -- BCI Integrated GP File
);