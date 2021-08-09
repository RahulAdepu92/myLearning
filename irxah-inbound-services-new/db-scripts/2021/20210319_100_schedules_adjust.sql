---changing Gila outbound schedule to adjust Daylight savings
---earlier 'GLUE' schedule cron expression was in local and 'AHUB->OUTBOUND' schedule cron expression was in UTC. So converting earlier to UTC.

update ahub_dw.file_schedule
set
cron_expression = 'utc : 0 9 * * *',
cron_description = 'Everyday at 04:00 AM EST, run a glue job that would send a Gila River file.'
where
processing_type = 'GLUE'
and
client_file_id = 17;


update ahub_dw.file_schedule
set
cron_description = 'Everyday at 06:00 AM EST, check whether the outbound file to Gila River is sent or not.'
where
processing_type = 'AHUB->OUTBOUND'
and
client_file_id = 17;