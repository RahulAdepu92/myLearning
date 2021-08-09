CREATE TABLE IF NOT EXISTS ahub_dw.custom_to_standard_mapping
(
  mapping_id              INTEGER NOT NULL,
  client_file_id          INTEGER NOT NULL,
  row_type                VARCHAR(45) NOT NULL,
  src_col_position        VARCHAR(45),
  dest_col_length         INTEGER NOT NULL,
  src_column_name         VARCHAR(45) NOT NULL,
  dest_col_position       VARCHAR(45) NOT NULL,
  conversion_type         VARCHAR(20) NOT NULL,
  dest_col_value          VARCHAR(45),
  conversion_formula_id   VARCHAR(200),
  description             VARCHAR(1000),
  created_by              VARCHAR(100),
  created_timestamp       TIMESTAMP,
  updated_by              VARCHAR(100),
  updated_timestamp       TIMESTAMP,
  PRIMARY KEY (mapping_id)
);
