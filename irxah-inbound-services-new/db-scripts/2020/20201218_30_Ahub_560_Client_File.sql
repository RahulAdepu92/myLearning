-- Column Rules ID is inserted in the file validation result table dynamically through a query.
-- The query must know which rule to pick up , earlier it was hard coded ( in constants.py) , now it is database driven
-- The tranmission ID ( can not be duplicate within a file ) and that validation gets executed using the column rule ID.
-- We can not hard code column rule ID , so it is soft coded in client file table.
ALTER TABLE ahub_dw.client_files 
    ADD COLUMN column_rule_id_for_transmission_id_validation 
    INTEGER NULL ENCODE az64;

ALTER TABLE ahub_dw.client_files 
ADD COLUMN extract_folder VARCHAR
(256) NULL ENCODE lzo;

-- First we set it to zero for all 
UPDATE AHUB_DW.CLIENT_FILES
   SET column_rule_id_for_transmission_id_validation = 0;


-- Use the output of the query below to run the update statement..
-- Column Rule ID is not known in advance, it is based on the how column rules are setup ..
-- The query below detects it run - time.. 
-- You need to run the output of this query ( the update statement)
-- DO NOT RUN THE UPDATE BELOW IF YOU HAVE NOT CONFIGURED THE COLUMN_RULES FOR THE RESPECTIVE INBOUND CLIENTS..
SELECT --cr.column_rules_id, fc.column_position, c.client_file_id,  c.client_id, 
    ' UPDATE AHUB_DW.CLIENT_FILES SET column_rule_id_for_transmission_id_validation= ' || cr.column_rules_id || ' where CLIENT_FILE_ID =' || c.client_file_id || ';'
FROM ahub_dw.column_rules cr
    INNER JOIN ahub_dw.file_columns fc
    ON cr.file_column_id = fc.file_column_id
    INNER JOIN ahub_dw.client_files c
    ON fc.client_file_id = c.client_file_id
WHERE c.file_type = 'INBOUND'
    AND fc.column_position = '357:407'
    AND cr.validation_type = 'EXTERNAL';


-- All inbound files (other than BCI and CVS) will be extracted in the inbound/txtfiles folder..
-- Please note that Gila River ( because of custom file) is treated bit differently.
UPDATE ahub_dw.client_files SET glue_job_name = 'irxah_process_incoming_file_load_in_database',
    extract_folder = 'inbound/txtfiles',
    archive_folder = 'inbound/archive',
    error_folder = 'inbound/error' 
    WHERE client_id IN (3,4,6)
    AND file_type = 'INBOUND';


-- All inbound files from BCI  will be extracted in the inbound/bci/txtfiles folder.. ( Backward Compatability)
UPDATE ahub_dw.client_files SET glue_job_name = 'irxah_process_incoming_file_load_in_database',
    extract_folder = 'inbound/bci/txtfiles',
    archive_folder = 'inbound/bci/archive',
    error_folder = 'inbound/bci/error' 
    WHERE client_id IN (1)
    AND file_type = 'INBOUND';

-- All inbound files from CVS  will be extracted in the inbound/bci/txtfiles folder.. (Backward Compatability)
UPDATE ahub_dw.client_files SET glue_job_name = 'irxah_process_incoming_cvs_file',
    extract_folder = 'inbound/cvs/txtfiles',
    archive_folder = 'inbound/cvs/archive',
    error_folder = 'inbound/cvs/error' 
    WHERE client_id IN (2)
    AND file_type = 'INBOUND';

-- All non BCI/non CVS inbound files  will be extracted in the inbound/txtfiles folder.. (Backward Compatability)
UPDATE ahub_dw.client_files SET
    extract_folder = 'inbound/txtfiles'
    WHERE client_id IN (3,4,5,6)
    AND file_type = 'INBOUND';