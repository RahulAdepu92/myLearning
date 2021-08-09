-- Introduces error_file column (boolean) in the ahub_dw.client_file

ALTER TABLE ahub_dw.client_files ADD COLUMN error_file BOOLEAN NULL;


-- Set Error File to True for  Inbound Health Plan Error  File (19) and BCI  Error File(7)
UPDATE ahub_dw.client_files
   SET error_file = TRUE,
       column_rule_id_for_transmission_id_validation = 0
WHERE client_file_id IN (7,19);

-- Make sure that error file is set to FALSE for rest others
UPDATE ahub_dw.client_files
   SET error_file = FALSE
WHERE client_file_id NOT IN (7,19);
