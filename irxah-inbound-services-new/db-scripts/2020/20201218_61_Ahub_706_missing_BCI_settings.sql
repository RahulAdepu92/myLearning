-- update input and output sender id for BCI error file
UPDATE ahub_dw.client_files
SET
  input_sender_id = '00990CAREMARK',
  output_sender_id = '00489INGENIORX'
WHERE client_file_id = 5; -- Outbound BCI Error file