----update Gila river (client_file_id = 23) cleint file columns

update ahub_dw.client_files
set
file_category = 'Gila River Accumulation File'
where client_file_id in (23);

