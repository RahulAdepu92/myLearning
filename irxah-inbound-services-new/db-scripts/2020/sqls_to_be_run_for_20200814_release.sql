## sql to update SNS_ARN to make 'region_name' a dynamic parameter ##

/* Please run the queries for the correct environment only */

/* DEV Environment */
UPDATE ahub_dw.client_files
   SET alert_notification_arn = '474156701944:irx_ahub_error_notification',
       sla_notification_arn = '474156701944:irx_ahub_sla_notification_arn',
       processing_notification_arn = '474156701944:irx_ahub_processing_notification'
WHERE client_file_id = 6;
UPDATE ahub_dw.client_files
   SET alert_notification_arn = '474156701944:irx_ahub_error_notification',
       sla_notification_arn = '474156701944:irx_ahub_sla_notification_arn',
       processing_notification_arn = '474156701944:irx_ahub_processing_notification'
WHERE client_file_id = 2;
UPDATE ahub_dw.client_files
   SET alert_notification_arn = '474156701944:irx_ahub_error_notification',
       sla_notification_arn = '474156701944:irx_ahub_sla_notification_arn',
       processing_notification_arn = '474156701944:irx_ahub_processing_notification'
WHERE client_file_id = 5;
UPDATE ahub_dw.client_files
   SET alert_notification_arn = '474156701944:irx_ahub_error_notification',
       sla_notification_arn = '474156701944:irx_ahub_sla_notification_arn',
       processing_notification_arn = '474156701944:irx_ahub_processing_notification'
WHERE client_file_id = 3;
UPDATE ahub_dw.client_files
   SET alert_notification_arn = '474156701944:irx_ahub_error_notification',
       sla_notification_arn = '474156701944:irx_ahub_sla_notification_arn',
       processing_notification_arn = '474156701944:irx_ahub_processing_notification'
WHERE client_file_id = 4;
UPDATE ahub_dw.client_files
   SET alert_notification_arn = '474156701944:irx_ahub_error_notification',
       sla_notification_arn = '474156701944:irx_ahub_sla_notification_arn',
       processing_notification_arn = '474156701944:irx_ahub_processing_notification'
WHERE client_file_id = 1;
UPDATE ahub_dw.client_files
   SET alert_notification_arn = '474156701944:irx_ahub_error_notification',
       sla_notification_arn = '474156701944:irx_ahub_sla_notification_arn',
       processing_notification_arn = '474156701944:irx_ahub_processing_notification'
WHERE client_file_id = 7;




/* SIT Environment */
UPDATE ahub_dw.client_files
   SET alert_notification_arn = '795038802291:irx_ahub_error_notification',
       sla_notification_arn = '795038802291:irx_ahub_sla_notification',
       processing_notification_arn = '795038802291:irx_ahub_processing_notification'
WHERE client_file_id = 6;
UPDATE ahub_dw.client_files
   SET alert_notification_arn = '795038802291:irx_ahub_error_notification',
       sla_notification_arn = '795038802291:irx_ahub_sla_notification',
       processing_notification_arn = '795038802291:irx_ahub_processing_notification'
WHERE client_file_id = 2;
UPDATE ahub_dw.client_files
   SET alert_notification_arn = '795038802291:irx_ahub_error_notification',
       sla_notification_arn = '795038802291:irx_ahub_sla_notification',
       processing_notification_arn = '795038802291:irx_ahub_processing_notification'
WHERE client_file_id = 5;
UPDATE ahub_dw.client_files
   SET alert_notification_arn = '795038802291:irx_ahub_error_notification',
       sla_notification_arn = '795038802291:irx_ahub_sla_notification',
       processing_notification_arn = '795038802291:irx_ahub_processing_notification'
WHERE client_file_id = 3;
UPDATE ahub_dw.client_files
   SET alert_notification_arn = '795038802291:irx_ahub_error_notification',
       sla_notification_arn = '795038802291:irx_ahub_sla_notification',
       processing_notification_arn = '795038802291:irx_ahub_processing_notification'
WHERE client_file_id = 4;
UPDATE ahub_dw.client_files
   SET alert_notification_arn = '795038802291:irx_ahub_error_notification',
       sla_notification_arn = '795038802291:irx_ahub_sla_notification',
       processing_notification_arn = '795038802291:irx_ahub_processing_notification'
WHERE client_file_id = 1;
UPDATE ahub_dw.client_files
   SET alert_notification_arn = '795038802291:irx_ahub_error_notification',
       sla_notification_arn = '795038802291:irx_ahub_sla_notification',
       processing_notification_arn = '795038802291:irx_ahub_processing_notification'
WHERE client_file_id = 7;

/* PRE-PROD Environment */
UPDATE ahub_dw.client_files
   SET alert_notification_arn = '974356221399:irx_ahub_error_notification',
       sla_notification_arn = '974356221399:irx_ahub_sla_notification',
       processing_notification_arn = '974356221399:irx_ahub_processing_notification'
WHERE client_file_id = 6;
UPDATE ahub_dw.client_files
   SET alert_notification_arn = '974356221399:irx_ahub_error_notification',
       sla_notification_arn = '974356221399:irx_ahub_sla_notification',
       processing_notification_arn = '974356221399:irx_ahub_processing_notification'
WHERE client_file_id = 2;
UPDATE ahub_dw.client_files
   SET alert_notification_arn = '974356221399:irx_ahub_error_notification',
       sla_notification_arn = '974356221399:irx_ahub_sla_notification',
       processing_notification_arn = '974356221399:irx_ahub_processing_notification'
WHERE client_file_id = 5;
UPDATE ahub_dw.client_files
   SET alert_notification_arn = '974356221399:irx_ahub_error_notification',
       sla_notification_arn = '974356221399:irx_ahub_sla_notification',
       processing_notification_arn = '974356221399:irx_ahub_processing_notification'
WHERE client_file_id = 3;
UPDATE ahub_dw.client_files
   SET alert_notification_arn = '974356221399:irx_ahub_error_notification',
       sla_notification_arn = '974356221399:irx_ahub_sla_notification',
       processing_notification_arn = '974356221399:irx_ahub_processing_notification'
WHERE client_file_id = 4;
UPDATE ahub_dw.client_files
   SET alert_notification_arn = '974356221399:irx_ahub_error_notification',
       sla_notification_arn = '974356221399:irx_ahub_sla_notification',
       processing_notification_arn = '974356221399:irx_ahub_processing_notification'
WHERE client_file_id = 1;
UPDATE ahub_dw.client_files
   SET alert_notification_arn = '974356221399:irx_ahub_error_notification',
       sla_notification_arn = '974356221399:irx_ahub_sla_notification',
       processing_notification_arn = '974356221399:irx_ahub_processing_notification'
WHERE client_file_id = 7;



/* PROD Environment */
UPDATE ahub_dw.client_files
   SET alert_notification_arn = '206388457288:irx_ahub_error_notification',
       sla_notification_arn = '206388457288:irx_ahub_sla_notification',
       processing_notification_arn = '206388457288:irx_ahub_processing_notification'
WHERE client_file_id = 6;
UPDATE ahub_dw.client_files
   SET alert_notification_arn = '206388457288:irx_ahub_error_notification',
       sla_notification_arn = '206388457288:irx_ahub_sla_notification',
       processing_notification_arn = '206388457288:irx_ahub_processing_notification'
WHERE client_file_id = 2;
UPDATE ahub_dw.client_files
   SET alert_notification_arn = '206388457288:irx_ahub_error_notification',
       sla_notification_arn = '206388457288:irx_ahub_sla_notification_arn',
       processing_notification_arn = '206388457288:irx_ahub_processing_notification'
WHERE client_file_id = 5;
UPDATE ahub_dw.client_files
   SET alert_notification_arn = '206388457288:irx_ahub_error_notification',
       sla_notification_arn = '206388457288:irx_ahub_sla_notification',
       processing_notification_arn = '206388457288:irx_ahub_processing_notification'
WHERE client_file_id = 3;
UPDATE ahub_dw.client_files
   SET alert_notification_arn = '206388457288:irx_ahub_error_notification',
       sla_notification_arn = '206388457288:irx_ahub_sla_notification',
       processing_notification_arn = '206388457288:irx_ahub_processing_notification'
WHERE client_file_id = 4;
UPDATE ahub_dw.client_files
   SET alert_notification_arn = '206388457288:irx_ahub_error_notification',
       sla_notification_arn = '206388457288:irx_ahub_sla_notification',
       processing_notification_arn = '206388457288:irx_ahub_processing_notification'
WHERE client_file_id = 1;
UPDATE ahub_dw.client_files
   SET alert_notification_arn = '206388457288:irx_ahub_error_notification',
       sla_notification_arn = '206388457288:irx_ahub_sla_notification',
       processing_notification_arn = '206388457288:irx_ahub_processing_notification'
WHERE client_file_id = 7;


## sql to update column rules-AHUB342 ##

UPDATE ahub_dw.column_rules
   SET python_formula = 'if (current_line[202:204]=="DR"):
  if(current_value in ["A","R"]):
    if(current_value == "A"):
      skip=True
    error_occured=False
  else:
    error_occured=True
elif (current_line[202:204]=="DQ"):
  if(len(current_value.strip())==0):
    error_occured=False
  else:
    error_occured=True
else:
  error_occured=True'
WHERE column_rules_id = 1130;
UPDATE ahub_dw.column_rules
   SET python_formula = 'if (current_line[202:204] == "DR" and current_line[270:271]=="R"):
  if(len(current_value.strip()) > 0):
    error_occured=False
  else:
    error_occured=True
  skip=True
else:
  if(len(current_value.strip()) > 0):
    error_occured=True
  else:
    error_occured=False'
WHERE column_rules_id = 1140;



## sql to update column rules-AHUB381 ##

alter table ahub_dw.client_files
ADD outbound_successful_acknowledgement BOOLEAN   ENCODE RAW;

alter table ahub_dw.client_files
ADD outbound_file_generation_notification_arn VARCHAR(200)   ENCODE lzo;

UPDATE ahub_dw.client_files SET outbound_successful_acknowledgement=True
WHERE client_file_id in (1,2,4,5);

/* DEV Environment */
UPDATE ahub_dw.client_files SET outbound_file_generation_notification_arn='474156701944:irx_ahub_outbound_file_generation_notification'
WHERE client_file_id in (1,2,3,4,5,6,7);

/* SIT Environment */
UPDATE ahub_dw.client_files SET outbound_file_generation_notification_arn='795038802291:irx_ahub_outbound_file_generation_notification'
WHERE client_file_id in (1,2,3,4,5,6,7);

/* PRE-PROD Environment */
UPDATE ahub_dw.client_files SET outbound_file_generation_notification_arn='974356221399:irx_ahub_outbound_file_generation_notification'
WHERE client_file_id in (1,2,3,4,5,6,7);

/* PROD Environment */
UPDATE ahub_dw.client_files SET outbound_file_generation_notification_arn='206388457288:irx_ahub_outbound_file_generation_notification'
WHERE client_file_id in (1,2,3,4,5,6,7);


## sql to update client client_file_id for outbound BCI files ##

UPDATE ahub_dw.client_files SET client_id=1 WHERE client_file_id in (4,5);


## sql to update poll_time_in_24_hour_format & grace_period for BCI inbound error file ##

UPDATE ahub_dw.client_files SET poll_time_in_24_hour_format = '14:30' where client_file_id =7;

UPDATE ahub_dw.client_files SET grace_period = 630 where client_file_id =7;