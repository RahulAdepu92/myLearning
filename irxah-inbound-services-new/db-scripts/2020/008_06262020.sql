/*
Update client_files table to set changed values for POLL_TIME_IN_24_HOUR_FORMAT column
For more information refer to AHUB-317
*/

UPDATE ahub_dw.client_files  SET POLL_TIME_IN_24_HOUR_FORMAT ='14:30' ,  GRACE_PERIOD=90  WHERE CLIENT_FILE_ID IN (1)
UPDATE ahub_dw.client_files  SET POLL_TIME_IN_24_HOUR_FORMAT ='10:30' WHERE CLIENT_FILE_ID IN (6)
UPDATE ahub_dw.client_files  SET POLL_TIME_IN_24_HOUR_FORMAT ='09:00' WHERE CLIENT_FILE_ID IN (4)

/* Following changes via AHUB-319 */
/* AHUB -319 changes commented out becaues of the AHUB -338 
UPDATE ahub_dw.client_files  SET POLL_TIME_IN_24_HOUR_FORMAT ='14:00', GRACE_PERIOD=60  WHERE CLIENT_FILE_ID IN (7)*/

/* Following changes via AHUB-320 */
update ahub_dw.client_files set process_duration_threshold=300 where client_file_id in (1,2,3,4,5,6,7);

/* Following changes via AHUB-325 */

UPDATE ahub_dw.column_rules
   SET is_active = ‘Y’, python_formula = 'try:
  datetime_object=datetime.strptime(current_value,''%Y%m%d'')
  today_datetime = datetime.strptime(datetime.today().strftime(''%Y%m%d''),''%Y%m%d'') 
 
  if (datetime_object >=(today_datetime+ timedelta(days=-730)) and datetime_object <= today_datetime):
    error_occured=False
  else:
    error_occured=True
except ValueError:
  error_occured=True'
WHERE column_rules_id = 210;

/* AHUB 338 - changes */

/* Check for incoming BCI file at 10:30 AM Eastern ( 14:30 UTC)  Duration : Midnight Eastern (04:00 UTC) to 10:30 AM Eastern (14:30 UTC), Frequency : DAILY */
UPDATE ahub_dw.client_files  SET  GRACE_PERIOD=630 , poll_time_in_24_hour_format='14:30' WHERE CLIENT_FILE_ID IN (1) ;
/* Check for incoming BCI Error file at 10:30  AM Eastern  (14:00 UTC)  Duration : Midnight Eastern (04:00 UTC) to 10:00 AM Eastern (14:30 UTC), Frequency : DAILY */
UPDATE ahub_dw.client_files  SET  GRACE_PERIOD=630 , poll_time_in_24_hour_format='14:30' WHERE CLIENT_FILE_ID IN (7) ;
/* Check for incoming Recon file ( Executes on Sunday only) at 06:30 AM Eastern  (10:30 UTC)  Duration : Midnight Eastern (04:00 UTC) to 06:30 AM Eastern (10:30 UTC), Frequency : WEEKLY */
UPDATE ahub_dw.client_files  SET  GRACE_PERIOD=390 , poll_time_in_24_hour_format='10:30' WHERE CLIENT_FILE_ID IN (6) ;