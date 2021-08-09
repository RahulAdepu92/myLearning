-- Change the file_schedule as below as per the updated schedule received from BCI

UPDATE ahub_dw.file_schedule
SET cron_expression = 'local: 0 7 * * TUE-SAT',
cron_description = 'Every Tuesday to Saturday, between 3 AM and 7 AM local time ( Eastern), AHUB should check for inbound GP file. This check will be executed at 07:00 AM Tuesday to Saturday ( did we receive the file in last 4 hours)',
--poll time below would vary based on where and when you run it.
-- please note that the poll time below is always in UTC
last_poll_time = '2021-01-13 12:00:00.000',
next_poll_time = '2021-01-14 12:00:00.000',
WHERE file_scheldule_id=20 ; -- inbound GP


-- Change the file_schedule as below because the underlying schedule was not running automatically..

-- For Biweekly Glue Jobs for Aetna Melton
UPDATE ahub_dw.file_schedule
SET is_active = true,
next_poll_time = '1/25/2020 10:00',
last_poll_time = '1/11/2020 10:00'
WHERE file_schedule_id=350;

-- For Biweekly Outbound Job ( did we generate the file on-time ) for Aetna Melton
UPDATE ahub_dw.file_schedule
SET is_active = true,
next_poll_time = '1/25/2020 12:00',
last_poll_time = '1/11/2020 12:00'
WHERE file_schedule_id=230;

 -- For Biweekly Inbound  Job ( did we receive the file on-time ) for Aetna Melton
UPDATE ahub_dw.file_schedule
SET is_active = true,
next_poll_time = '1/28/2021 17:00',
last_poll_time = '1/14/2021 17:00'
WHERE file_schedule_id=80;
