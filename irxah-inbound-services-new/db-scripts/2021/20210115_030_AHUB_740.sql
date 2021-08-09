--- update input_sender_id and output_receiver_id for THP files in client_files table

UPDATE AHUB_DW.CLIENT_FILES
SET input_sender_id = '10500HEALTHPLAN'
Where client_id in (6)  -- THP files (INT and ERR)
and file_type = 'INBOUND';

UPDATE AHUB_DW.CLIENT_FILES
SET output_receiver_id = '10500HEALTHPLAN'
Where client_id in (6)  -- THP files (INT and ERR)
and file_type = 'OUTBOUND';

---- update input_sender_id value for THP INT file in column_rules table

UPDATE ahub_dw.column_rules
SET equal_to = '10500HEALTHPLAN'
WHERE validation_type = 'EQUALTO'
AND file_column_id IN (SELECT file_column_id
FROM ahub_dw.file_columns
WHERE client_file_id = 18  -- THP INT file
AND file_column_name = 'Sender ID');