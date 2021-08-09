## sql to add and update column -AHUB391 ##

alter table ahub_dw.client_files
ADD current_timezone VARCHAR(50)  ENCODE lzo;

update ahub_dw.client_files set current_timezone = 'EDT'
where client_file_id in (1,4,5,7);

update ahub_dw.client_files set current_timezone = 'NA'
where client_file_id in (2,3,6);

## sql to run which was discussed to set unused rows to False -AHUB381 ##

UPDATE ahub_dw.client_files SET outbound_successful_acknowledgement=False
WHERE client_file_id in (3,6,7);


UPDATE ahub_dw.client_files SET is_alert_active=False
WHERE client_file_id in (2,5);


## sql for AHUB-410 ##

UPDATE ahub_dw.client_files set poll_time_in_24_hour_format ='16:00', grace_period=720 where client_file_id=6


## sql for bug fix AHUB-423 ##

UPDATE AHUB_DW.COLUMN_RULES SET ERROR_CODE=636 WHERE FILE_COLUMN_ID=54

## sql for updating poll_time for BCI outbound error file ##

UPDATE  AHUB_DW.CLIENT_FILES SET  poll_time_in_24_hour_format= '09:00' WHERE CLIENT_FILE_ID=5 ;