/* this whole script intends to introduce new client details in client table */

----- RUN this global UPDATE statement in any environment SIT/UAT/PROD to add new 2 columns

alter table ahub_dw.client
DROP COLUMN file_recipient_client_name CASCADE;

alter table ahub_dw.client_files
ADD carrier_id VARCHAR(50) ENCODE lzo;

alter table ahub_dw.client_files
ADD active_date DATE;

-- RUN this global INSERT statement in any environment SIT/UAT/PROD

--- insert Aetna Melton

INSERT INTO ahub_dw.client
(
 client_id,
 abbreviated_name,
 name,
 created_by
 )
Values
 (
  3,
  'ATN',
  'Aetna Melton',
  'AHUB ETL'
 );

--- insert GPA Melton

INSERT INTO ahub_dw.client
(
 client_id,
 abbreviated_name,
 name,
 created_by
 )
Values
 (
  4,
  'GPA',
  'GPA Melton',
  'AHUB ETL'
 );

--- insert Gila River

INSERT INTO ahub_dw.client
(
 client_id,
 abbreviated_name,
 name,
 created_by
 )
Values
 (
  5,
  'GRG',
  'Gila River',
  'AHUB ETL'
 );

--- insert Gila River

INSERT INTO ahub_dw.client
(
 client_id,
 abbreviated_name,
 name,
 created_by
 )
Values
 (
  6,
  'THP',
  'The Health Plan',
  'AHUB ETL'
 );


----- ############# RUN this global UPDATE statement in any environment SIT/UAT/PROD for older client_file_ids ############ ------------

update ahub_dw.client_files
set
structure_specification_file = 'inbound/specifications/bci_structure_specification.csv'
where client_file_id in (1,9);  --- for BCI Commercial and GP files


update ahub_dw.client_files
set
structure_specification_file = 'inbound/specifications/bci_structure_specification_error.csv'
where client_file_id in (7); --- for BCI Error file


update ahub_dw.client_files
set
position_specification_file = 'inbound/specifications/position_specification.txt'
where client_file_id in (1,7,9);   --- for all BCI files


--------Update process_name and descriptions

update ahub_dw.client_files
set process_name = 'BCI Integrated file to Database',
process_description = 'Validate inbound BCI Integrated file, split, load into redshift database'
where client_file_id in (1);

update ahub_dw.client_files
set process_name = 'BCI GP Integrated file to Database',
process_description = 'Validate inbound BCI GP Integrated file, split, load into redshift database'
where client_file_id in (9);

update ahub_dw.client_files
set process_name = 'Database to BCI Integrated file',
process_description = 'From database export Integrated outbound file to BCI'
where client_file_id in (4);

update ahub_dw.client_files
set process_name = 'Database to BCI Error file',
process_description = 'From database export Error outbound file to BCI'
where client_file_id in (5);


--------Update process_name and descriptions

update ahub_dw.client_files
set client_id = 1,
archive_folder = 'outbound/archive',
glue_job_name = 'irxah_export_client_file',
file_processing_location = 'outbound/bci/temp'
where client_file_id in (4,5);


update ahub_dw.client_files
set s3_output_file_name_prefix = '',
outbound_transmission_type_column = '',
outbound_file_type_column = ''
where client_file_id in (1,9);


-- ################  setup column rules for all 12 accumulators for BCI files with is_active = 'N' (cfd= 1)
-- ################ For NON-BCI files update it to 'Y' as they need validations for all 12 accumulators

INSERT INTO ahub_dw.column_rules(column_rules_id, file_column_id, priority, validation_type, equal_to, python_formula, list_of_values, error_code, error_level, is_active)
VALUES
(650, 89, 1, 'REQUIRED_AND_RANGE', null, null, '02,04,05,06,07,08,14,15', 635, 2, 'N'),
(660, 90, 1, 'REQUIRED', null, null, null, 178, 1, 'N'             ),
(670, 91, 2, 'NUMBER', null, null, null, 137, 2, 'N'               ),
(680, 92, 1, 'REQUIRED_AND_RANGE', null, null, '+,R'  , 624, 1, 'N'),
(690, 97, 1, 'REQUIRED_AND_RANGE', null, null, '02,04,05,06,07,08,14,15', 635, 2, 'N'),
(700, 98, 1, 'REQUIRED', null, null, null, 178, 1, 'N'             ),
(710, 99, 2, 'NUMBER', null, null, null, 137, 2, 'N'               ),
(720, 100, 1, 'REQUIRED_AND_RANGE', null, null, '+,R'  , 624, 1, 'N'),
(730, 106, 1, 'REQUIRED_AND_RANGE', null, null, '02,04,05,06,07,08,14,15', 635, 2, 'N'),
(740, 107, 1, 'REQUIRED', null, null, null, 178, 1, 'N'             ),
(750, 108, 2, 'NUMBER', null, null, null, 137, 2, 'N'               ),
(760, 109, 1, 'REQUIRED_AND_RANGE', null, null, '+,R'  , 624, 1, 'N'),
(770, 114, 1, 'REQUIRED_AND_RANGE', null, null, '02,04,05,06,07,08,14,15', 635, 2, 'N'),
(780, 115, 1, 'REQUIRED', null, null, null, 178, 1, 'N'             ),
(790, 116, 2, 'NUMBER', null, null, null, 137, 2, 'N'               ),
(805, 117, 1, 'REQUIRED_AND_RANGE', null, null, '+,R'  , 624, 1, 'N'),
(815, 122, 1, 'REQUIRED_AND_RANGE', null, null, '02,04,05,06,07,08,14,15', 635, 2, 'N'),
(825, 123, 1, 'REQUIRED', null, null, null, 178, 1, 'N'             ),
(835, 124, 2, 'NUMBER', null, null, null, 137, 2, 'N'               ),
(845, 125, 1, 'REQUIRED_AND_RANGE', null, null, '+,R'  , 624, 1, 'N'),
(855, 130, 1, 'REQUIRED_AND_RANGE', null, null, '02,04,05,06,07,08,14,15', 635, 2, 'N'),
(865, 131, 1, 'REQUIRED', null, null, null, 178, 1, 'N'             ),
(875, 132, 2, 'NUMBER', null, null, null, 137, 2, 'N'               ),
(885, 133, 1, 'REQUIRED_AND_RANGE', null, null, '+,R'  , 624, 1, 'N'),
(895, 138, 1, 'REQUIRED_AND_RANGE', null, null, '02,04,05,06,07,08,14,15', 635, 2, 'N'),
(905, 139, 1, 'REQUIRED', null, null, null, 178, 1, 'N'             ),
(915, 140, 2, 'NUMBER', null, null, null, 137, 2, 'N'               ),
(925, 141, 1, 'REQUIRED_AND_RANGE', null, null, '+,R'  , 624, 1, 'N'),
(935, 146, 1, 'REQUIRED_AND_RANGE', null, null, '02,04,05,06,07,08,14,15', 635, 2, 'N'),
(945, 147, 1, 'REQUIRED', null, null, null, 178, 1, 'N'             ),
(955, 148, 2, 'NUMBER', null, null, null, 137, 2, 'N'               ),
(965, 149, 1, 'REQUIRED_AND_RANGE', null, null, '+,R'  , 624, 1, 'N');