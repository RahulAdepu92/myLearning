---add new columns 'inbound_successful_acknowledgement' and 'inbound_file_arrival_notification_sns'
--- make sure subscribe to the new SNS topic introduced 'irx_ahub_inbound_file_arrival_notification'

ALTER TABLE ahub_dw.client_files
ADD COLUMN inbound_successful_acknowledgement
BOOLEAN NOT NULL DEFAULT false;


ALTER TABLE ahub_dw.client_files
ADD COLUMN inbound_file_arrival_notification_sns
VARCHAR(200) ENCODE lzo;

-----------update values for  newly added above columns

UPDATE ahub_dw.client_files
SET inbound_file_arrival_notification_sns = 'irx_ahub_inbound_file_arrival_notification'
Where  file_type = 'INBOUND';

UPDATE AHUB_DW.CLIENT_FILES
 SET inbound_successful_acknowledgement = True
Where  file_type = 'INBOUND';

--------------- update 'file_category' for cvs outbound in client_files table

update ahub_dw.client_files
set
file_category = 'CVS Consolidated'
where client_file_id in (22);


-------  run only in PROD  on 01/29 -------

-- confirmed with Cindi that, inbound CVS doesnt need inbound file arrival (which occur for every 2 hours interval) alerts in PROD

UPDATE AHUB_DW.CLIENT_FILES
SET inbound_successful_acknowledgement = False
Where  client_file_id in (3);  --from CVS