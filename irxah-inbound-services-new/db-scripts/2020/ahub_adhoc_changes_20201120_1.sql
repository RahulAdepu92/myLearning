---renaming the columns

alter table ahub_dw.client_files RENAME data_error_notification_arn to data_error_notification_sns;
alter table ahub_dw.client_files RENAME sla_notification_arn to sla_notification_sns;
alter table ahub_dw.client_files RENAME processing_notification_arn to processing_notification_sns;
alter table ahub_dw.client_files RENAME outbound_file_generation_notification_arn to outbound_file_generation_notification_sns;
alter table ahub_dw.client_files RENAME iam_arn to redshift_glue_iam_role_name;
alter table ahub_dw.client_files RENAME file_type_column_in_outbound_file to outbound_file_type_column;
alter table ahub_dw.client_files RENAME transmission_file_type to outbound_transmission_type_column;

---updating the column values

update ahub_dw.client_files
set
data_error_notification_sns = 'irx_ahub_error_notification',
sla_notification_sns = 'irx_ahub_sla_notification',
processing_notification_sns = 'irx_ahub_processing_notification',
outbound_file_generation_notification_sns = 'irx_ahub_outbound_file_generation_notification',
redshift_glue_iam_role_name = 'irx-accum-phi-redshift-glue';