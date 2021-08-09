---sql to be run in all environments

update ahub_dw.client_files
set validate_file_detail_columns = TRUE ;

update ahub_dw.client_files
set validate_file_structure  = TRUE ;

-- set specific client settings

update ahub_dw.client_files
set validate_file_detail_columns = FALSE
where client_file_id in (6,10);


update ahub_dw.client_files
set validate_file_detail_columns = TRUE
where client_file_id in (7,19);