
-- NON INTRUSIVE DELETE
DELETE FROM AHUB_DW.COLUMN_RULES WHERE COLUMN_RULE_ID IN ( 23351, 23455);

-- Insert Error Code to support the Custom Validation

INSERT INTO ahub_dw.column_error_codes (error_code,error_message,created_by,updated_by,updated_timestamp,created_timestamp) 
VALUES
  (114,'Patient ID has an invalid format','AHUB ETL',NULL,NULL,'2020-04-30 16:28:46.647');


-- Inser column rules to support custom validations
INSERT INTO ahub_dw.column_rules (column_rules_id,file_column_id,priority,validation_type,equal_to,python_formula,list_of_values,error_code,error_level,is_active,created_by,updated_by,updated_timestamp,created_timestamp) 
VALUES
  (23351,23051,2,'MUST_NOT_EQUAL_TO','99999999999999999999',NULL,NULL,114,1,'Y','AHUB ETL',NULL,NULL,'2020-12-28 03:30:37.502'),
  (23455,23059,3,'MUST_NOT_EQUAL_TO','9999999999','
',NULL,137,1,'Y','AHUB ETL',NULL,NULL,'2020-12-28 03:30:37.502');

-- Column Rules ID : 23351 sets the validation rule for the Patient ID
-- Column Rules ID : 23455 sets the validation rule for the Amount