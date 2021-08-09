alter table ahub_dw.client_files
add column load_table_name VARCHAR(50)  ENCODE lzo;

update ahub_dw.client_files
set load_table_name = 'accumulator_reconciliation_detail'
where client_file_id in (6,10);

update ahub_dw.client_files
set load_table_name = 'accumulator_detail'
where file_type= 'INBOUND'
and client_file_id not in (6,8,10) ;  -- execluding acchist and recon files