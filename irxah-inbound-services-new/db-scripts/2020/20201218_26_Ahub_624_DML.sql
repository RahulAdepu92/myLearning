-- BCI Outbound
-- The precondition field holds a query that checks
-- whether we received any inbound files from CVS
-- from the time we have last exported records to BCI.
-- The intent is to avoid generating empty outbound files
-- when no inbound files have been received.
-- In the case of inbound-from-CVS, this means that
-- we received all twelve files and they have been marked
-- as 'Available' (or a manual process marked some inbound-from-CVS
-- files as Available for export)
update ahub_dw.export_setting
set precondition = 

$$

WITH most_recent_out_job AS (
select coalesce(max(out_job_key), 0) as out_job_key
from ahub_dw.accumulator_detail a
    inner join ahub_dw.job j on a.out_job_key = j.job_key
where
    j.client_file_id = 4 -- outbound to BCI
)

select count(*)
from ahub_dw.job j inner join ahub_dw.client_files cf
    on j.client_file_id = cf.client_file_id
where
    cf.client_id = 2 -- CVS
    and cf.file_type = 'INBOUND'
    and j.file_status = 'Available'
    -- inbound job_key being larger means more recent
    and job_key > (select top 1 out_job_key from most_recent_out_job)

$$

where client_file_id = 4
;

-- BCI Error Outbound
-- The precondition field holds a query that checks
-- whether we received any inbound files from CVS
-- from the time we have last exported records to BCI.
-- The intent is to avoid generating empty outbound files
-- when no inbound files have been received.
-- In the case of inbound-from-CVS, this means that
-- we received all twelve files and they have been marked
-- as 'Available' (or a manual process marked some inbound-from-CVS
-- files as Available for export)
update ahub_dw.export_setting
set precondition = 

$$

WITH most_recent_out_job AS (
select coalesce(max(out_job_key), 0) as out_job_key
from ahub_dw.accumulator_detail a
    inner join ahub_dw.job j on a.out_job_key = j.job_key
where
    j.client_file_id = 5 -- error outbound to BCI
)

select count(*)
from ahub_dw.job j inner join ahub_dw.client_files cf
    on j.client_file_id = cf.client_file_id
where
    cf.client_id = 2 -- CVS
    and cf.file_type = 'INBOUND'
    and j.file_status = 'Available'
    -- inbound job_key being larger means more recent
    and job_key > (select top 1 out_job_key from most_recent_out_job)

$$

where client_file_id = 5
;

-- ATN Outbound
-- The precondition field holds a query that checks
-- whether we received any inbound files from CVS
-- from the time we have last exported records to ATN.
-- The intent is to avoid generating empty outbound files
-- when no inbound files have been received.
-- In the case of inbound-from-CVS, this means that
-- we received all twelve files and they have been marked
-- as 'Available' (or a manual process marked some inbound-from-CVS
-- files as Available for export)
update ahub_dw.export_setting
set precondition = 

$$

WITH most_recent_out_job AS (
select coalesce(max(out_job_key), 0) as out_job_key
from ahub_dw.accumulator_detail a
    inner join ahub_dw.job j on a.out_job_key = j.job_key
where
    j.client_file_id = 13 -- outbound to ATN
)

select count(*)
from ahub_dw.job j inner join ahub_dw.client_files cf
    on j.client_file_id = cf.client_file_id
where
    cf.client_id = 2 -- CVS
    and cf.file_type = 'INBOUND'
    and j.file_status = 'Available'
    -- inbound job_key being larger means more recent
    and job_key > (select top 1 out_job_key from most_recent_out_job)

$$

where client_file_id = 13
;

-- GPA Outbound
-- The precondition field holds a query that checks
-- whether we received any inbound files from CVS
-- from the time we have last exported records to GPA.
-- The intent is to avoid generating empty outbound files
-- when no inbound files have been received.
-- In the case of inbound-from-CVS, this means that
-- we received all twelve files and they have been marked
-- as 'Available' (or a manual process marked some inbound-from-CVS
-- files as Available for export)
update ahub_dw.export_setting
set precondition = 

$$

WITH most_recent_out_job AS (
select coalesce(max(out_job_key), 0) as out_job_key
from ahub_dw.accumulator_detail a
    inner join ahub_dw.job j on a.out_job_key = j.job_key
where
    j.client_file_id = 15 -- outbound to GPA
)

select count(*)
from ahub_dw.job j inner join ahub_dw.client_files cf
    on j.client_file_id = cf.client_file_id
where
    cf.client_id = 2 -- CVS
    and cf.file_type = 'INBOUND'
    -- inbound job_key being larger means more recent
    and job_key > (select top 1 out_job_key from most_recent_out_job)

$$

where client_file_id = 15
;


-- GRG Outbound
-- The precondition field holds a query that checks
-- whether we received any inbound files from CVS
-- from the time we have last exported records to GRG.
-- The intent is to avoid generating empty outbound files
-- when no inbound files have been received.
-- In the case of inbound-from-CVS, this means that
-- we received all twelve files and they have been marked
-- as 'Available' (or a manual process marked some inbound-from-CVS
-- files as Available for export)
update ahub_dw.export_setting
set precondition = 

$$

WITH most_recent_out_job AS (
select coalesce(max(out_job_key), 0) as out_job_key
from ahub_dw.accumulator_detail a
    inner join ahub_dw.job j on a.out_job_key = j.job_key
where
    j.client_file_id = 17 -- outbound to GRG
)

select count(*)
from ahub_dw.job j inner join ahub_dw.client_files cf
    on j.client_file_id = cf.client_file_id
where
    cf.client_id = 2 -- CVS
    and cf.file_type = 'INBOUND'
    and j.file_status = 'Available'
    -- inbound job_key being larger means more recent
    and job_key > (select top 1 out_job_key from most_recent_out_job)

$$

where client_file_id = 17
;


-- THP Outbound
-- The precondition field holds a query that checks
-- whether we received any inbound files from CVS
-- from the time we have last exported records to THP.
-- The intent is to avoid generating empty outbound files
-- when no inbound files have been received.
-- In the case of inbound-from-CVS, this means that
-- we received all twelve files and they have been marked
-- as 'Available' (or a manual process marked some inbound-from-CVS
-- files as Available for export)
update ahub_dw.export_setting
set precondition = 

$$

WITH most_recent_out_job AS (
select coalesce(max(out_job_key), 0) as out_job_key
from ahub_dw.accumulator_detail a
    inner join ahub_dw.job j on a.out_job_key = j.job_key
where
    j.client_file_id = 20 -- outbound to THP
)

select count(*)
from ahub_dw.job j inner join ahub_dw.client_files cf
    on j.client_file_id = cf.client_file_id
where
    cf.client_id = 2 -- CVS
    and cf.file_type = 'INBOUND'
    and j.file_status = 'Available'
    -- inbound job_key being larger means more recent
    and job_key > (select top 1 out_job_key from most_recent_out_job)

$$

where client_file_id = 20
;


-- THP Error
-- The precondition field holds a query that checks
-- whether we received any inbound files from CVS
-- from the time we have last exported records to THP.
-- The intent is to avoid generating empty outbound files
-- when no inbound files have been received.
-- In the case of inbound-from-CVS, this means that
-- we received all twelve files and they have been marked
-- as 'Available' (or a manual process marked some inbound-from-CVS
-- files as Available for export)
update ahub_dw.export_setting
set precondition = 

$$

WITH most_recent_out_job AS (
select coalesce(max(out_job_key), 0) as out_job_key
from ahub_dw.accumulator_detail a
    inner join ahub_dw.job j on a.out_job_key = j.job_key
where
    j.client_file_id = 21 -- outbound error to THP
)

select count(*)
from ahub_dw.job j inner join ahub_dw.client_files cf
    on j.client_file_id = cf.client_file_id
where
    cf.client_id = 2 -- CVS
    and cf.file_type = 'INBOUND'
    and j.file_status = 'Available'
    -- inbound job_key being larger means more recent
    and job_key > (select top 1 out_job_key from most_recent_out_job)

$$

where client_file_id = 21
;

-- CVS Outbound
-- The precondition field holds a query that checks
-- whether we received any inbound files from
-- any of our clients from the time we have
-- last exported records to CVS.
-- The intent is to avoid generating empty outbound files
-- to CVS when no inbound client files have been received.
update ahub_dw.export_setting
set precondition = 

$$

WITH most_recent_out_job AS (
select coalesce(max(out_job_key), 0) as out_job_key
from ahub_dw.accumulator_detail a
    inner join ahub_dw.job j on a.out_job_key = j.job_key
where
    j.client_file_id = 22 -- outbound to CVS
)

select count(*)
from ahub_dw.job j inner join ahub_dw.client_files cf
    on j.client_file_id = cf.client_file_id
where
    cf.client_id <> 2 -- excluded CVS's own files
    and cf.file_type = 'INBOUND'
    and j.status = 'Success' -- only files that loaded successfully
    -- inbound job_key being larger means more recent
    and job_key > (select top 1 out_job_key from most_recent_out_job)

$$

where client_file_id = 22
;

