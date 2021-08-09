-- updates queries to include BCI GP in error file

UPDATE ahub_dw.export_setting
SET
  detail_query = $$
unload
(
'SELECT ''{routing_id}'' AS processor_routing_identification,
       record_type,
       dr_transmission_file_type,
       version_release_number,
       (select top 1 output_sender_id from ahub_dw.client_files where client_file_id=5) AS sender_identifier,
       (select top 1 output_receiver_id from ahub_dw.client_files where client_file_id=5) AS receiver_identifier,
       submission_number,
       dr_transaction_response_status,
       dr_reject_code,
       record_length,
       reserved_1,
       transmission_date,
       transmission_time,
       date_of_service,
       service_provider_identifier_qualifier,
       service_provider_identifier,
       document_reference_identifier_qualifier,
       document_reference_identifier,
       transmission_identifier,
       benefit_type,
       in_network_indicator,
       formulary_status,
       accumulator_action_code,
       sender_reference_number,
       insurance_code,
       accumulator_balance_benefit_type,
       benefit_effective_date,
       benefit_termination_date,
       accumulator_change_source_code,
       transaction_identifier,
       transaction_identifier_cross_reference,
       adjustment_reason_code,
       accumulator_reference_time_stamp,
       reserved_2,
       cardholder_identifier,
       group_identifier,
       patient_first_name,
       middle_initial,
       patient_last_name,
       patient_relationship_code,
       date_of_birth,
       patient_gender_code,
       patient_state_province_address,
       cardholder_last_name,
       carrier_number,
       contract_number,
       client_pass_through,
       family_identifier_number,
       cardholder_identifier_alternate,
       group_identifier_alternate,
       patient_identifier,
       person_code,
       reserved_3,
       accumulator_balance_count,
       accumulator_specific_category_type,
       reserved_4,
       accumulator_1_balance_qualifier,
       accumulator_1_network_indicator,
       accumulator_1_applied_amount,
       accumulator_1_applied_amount_action_code,
       accumulator_1_benefit_period_amount,
       accumulator_1_benefit_period_amount_action_code,
       accumulator_1_remaining_balance,
       accumulator_1_remaining_balance_action_code,
       accumulator_2_balance_qualifier,
       accumulator_2_network_indicator,
       accumulator_2_applied_amount,
       accumulator_2_applied_amount_action_code,
       accumulator_2_benefit_period_amount,
       accumulator_2_benefit_period_amount_action_code,
       accumulator_2_remaining_balance,
       accumulator_2_remaining_balance_action_code,
       accumulator_3_balance_qualifier,
       accumulator_3_network_indicator,
       accumulator_3_applied_amount,
       accumulator_3_applied_amount_action_code,
       accumulator_3_benefit_period_amount,
       accumulator_3_benefit_period_amount_action_code,
       accumulator_3_remaining_balance,
       accumulator_3_remaining_balance_action_code,
       accumulator_4_balance_qualifier,
       accumulator_4_network_indicator,
       accumulator_4_applied_amount,
       accumulator_4_applied_amount_action_code,
       accumulator_4_benefit_period_amount,
       accumulator_4_benefit_period_amount_action_code,
       accumulator_4_remaining_balance,
       accumulator_4_remaining_balance_action_code,
       '' '' AS reserved_5
FROM
(
  SELECT
    *,
    ROW_NUMBER() OVER (PARTITION BY 
        transmission_identifier,patient_identifier,
        date_of_birth,transaction_identifier,
        accumulator_specific_category_type,
        accumulator_1_balance_qualifier,accumulator_2_balance_qualifier,
        accumulator_3_balance_qualifier,accumulator_4_balance_qualifier,
        accumulator_1_applied_amount_action_code,accumulator_2_applied_amount_action_code,
        accumulator_3_applied_amount_action_code,accumulator_4_applied_amount_action_code,
        accumulator_1_applied_amount,accumulator_2_applied_amount,
        accumulator_3_applied_amount,accumulator_4_applied_amount
        ORDER BY transmission_date) AS RowNumber
  FROM
  (



SELECT -- error records from CVS
  -- we are aliasing some columns to be able to use * and help out the union part and keep length in check
  ''DR'' as dr_transmission_file_type,
  ''R'' as dr_transaction_response_status,
  reject_code as dr_reject_code,
  ROW_NUMBER() OVER (PARTITION BY 
    transmission_identifier,patient_identifier,
    date_of_birth,transaction_identifier,
    accumulator_specific_category_type,
    accumulator_1_balance_qualifier,accumulator_2_balance_qualifier,
    accumulator_3_balance_qualifier,accumulator_4_balance_qualifier,
    accumulator_1_applied_amount_action_code,accumulator_2_applied_amount_action_code,
    accumulator_3_applied_amount_action_code,accumulator_4_applied_amount_action_code,
    accumulator_1_applied_amount,accumulator_2_applied_amount,
    accumulator_3_applied_amount,accumulator_4_applied_amount
    ORDER BY transmission_date) AS Row_Rank,
  *
FROM (
  SELECT acc.*
  FROM ahub_dw.accumulator_detail acc
    JOIN ahub_dw.job j ON acc.job_key = j.job_key
  WHERE 
      client_id = 2 -- CVS
      AND status = ''Success''
      AND file_status = ''Available'' -- all 12 CVS files have been loaded
      AND out_job_key is null -- job has not been exported yet
      AND acc.sender_identifier = (select top 1 input_sender_id from ahub_dw.client_files where client_file_id=5)
      AND transmission_file_type = ''DR''
      AND transaction_response_status = ''R''
      AND client_pass_through in (''INGENIORXBCI00489'')
   )

UNION
SELECT  -- L1 errors in incoming BCI files that AHUB detected
       ''DR'' AS dr_transmission_file_type,
       ''R'' AS dr_transaction_response_status,
       CAST(valerr.error_code AS VARCHAR) AS dr_reject_code,
       1 as Row_Rank,
       a.*
FROM 
  (
    SELECT *
    FROM ahub_dw.accumulator_detail
    WHERE job_key IN (SELECT job_key
                        FROM ahub_dw.job
                        WHERE client_file_id in (1, 9) -- file came from BCI Comm or GP
                     )
    AND out_job_key is null -- was not sent out yet
  ) a
  JOIN 
  (
    SELECT *
    FROM 
    (
      -- selects the records with errors
      SELECT *,
             ROW_NUMBER() OVER (PARTITION BY job_key,line_number ORDER BY error_level ASC,file_column_id ASC) rnk
      FROM 
      (
        SELECT 
          val.validation_result,
          val.job_key,
          val.line_number,
          cr.file_column_id,
          cr.error_level,
          cr.error_code,
          cr.column_rules_id
        FROM ahub_dw.column_rules cr
          JOIN ahub_dw.file_validation_result AS val ON cr.column_rules_id = val.column_rules_id
        WHERE val.validation_result = ''N''
              AND
              val.job_key IN (
                  SELECT job_key FROM ahub_dw.job
                  WHERE client_file_id in (1, 9) -- came from BCI Comm or GP
                  )
      )
    )
    WHERE rnk = 1 AND error_level = 1
  ) valerr
  ON a.job_key = valerr.job_key AND a.line_number = valerr.line_number



  ) z
)
WHERE RowNumber = 1
ORDER BY to_timestamp(transmission_date || (case regexp_substr(transmission_time, ''[^0-9]+'') when '''' then transmission_time else ''000000'' end), ''YYYYMMDDHH24MISSMS'')


')
TO 's3://{s3_out_bucket}/{s3_file_path}' iam_role '{iam_role}'
FIXEDWIDTH
'0:200,1:2,2:2,3:2,4:30,5:30,6:4,7:1,8:3,9:5,10:20,11:8,12:8,13:8,14:2,15:15,16:2,17:15,18:50,19:1,20:1,
21:1,22:2,23:30,24:20,25:1,26:8,27:8,28:1,29:30,30:30,31:1,32:26,33:13,34:20,35:15,36:25,37:1,38:35,39:1,40:8,
41:1,42:2,43:35,44:9,45:15,46:50,47:20,48:20,49:15,50:20,51:3,52:90,53:2,54:2,55:20,56:2,57:1,58:10,59:1,60:10,
61:1,62:10,63:1,64:2,65:1,66:10,67:1,68:10,69:1,70:10,71:1,72:2,73:1,74:10,75:1,76:10,77:1,78:10,79:1,80:2,
81:1,82:10,83:1,84:10,85:1,86:10,87:1,88:567'
ALLOWOVERWRITE
parallel off;


$$
,

trailer_query = $$

UNLOAD
('
select
''{routing_id}'' AS PRCS_ROUT_ID,
''TR'' AS RECD_TYPE,''0000001'' AS BATCH_NBR,
lpad(b.cnt,10,''0'') AS REC_CNT,
'' '' AS MSG_TXT,
'' ''AS RESERVED_SP
from
  (
    SELECT cast(count(1) as varchar) cnt

FROM
(
  SELECT
    accumulator_detail_key,
    ROW_NUMBER() OVER (PARTITION BY 
        transmission_identifier,patient_identifier,
        date_of_birth,transaction_identifier,
        accumulator_specific_category_type,
        accumulator_1_balance_qualifier,accumulator_2_balance_qualifier,
        accumulator_3_balance_qualifier,accumulator_4_balance_qualifier,
        accumulator_1_applied_amount_action_code,accumulator_2_applied_amount_action_code,
        accumulator_3_applied_amount_action_code,accumulator_4_applied_amount_action_code,
        accumulator_1_applied_amount,accumulator_2_applied_amount,
        accumulator_3_applied_amount,accumulator_4_applied_amount
        ORDER BY transmission_date) AS RowNumber
  FROM
  (
SELECT -- error records from CVS
  -- we are aliasing some columns to be able to use * and help out the union part and keep length in check
  ''DR'' as dr_transmission_file_type,
  ''R'' as dr_transaction_response_status,
  reject_code as dr_reject_code,
  ROW_NUMBER() OVER (PARTITION BY 
    transmission_identifier,patient_identifier,
    date_of_birth,transaction_identifier,
    accumulator_specific_category_type,
    accumulator_1_balance_qualifier,accumulator_2_balance_qualifier,
    accumulator_3_balance_qualifier,accumulator_4_balance_qualifier,
    accumulator_1_applied_amount_action_code,accumulator_2_applied_amount_action_code,
    accumulator_3_applied_amount_action_code,accumulator_4_applied_amount_action_code,
    accumulator_1_applied_amount,accumulator_2_applied_amount,
    accumulator_3_applied_amount,accumulator_4_applied_amount
    ORDER BY transmission_date) AS Row_Rank,
  *
FROM (
  SELECT acc.*
  FROM ahub_dw.accumulator_detail acc
    JOIN ahub_dw.job j ON acc.job_key = j.job_key
  WHERE 
      client_id = 2 -- CVS
      AND status = ''Success''
      AND file_status = ''Available'' -- all 12 CVS files have been loaded
      AND out_job_key is null -- job has not been exported yet
      AND acc.sender_identifier = (select top 1 input_sender_id from ahub_dw.client_files where client_file_id=5)
      AND transmission_file_type = ''DR''
      AND transaction_response_status = ''R''
      AND client_pass_through in (''INGENIORXBCI00489'')
   )

UNION
SELECT  -- L1 errors in incoming BCI files that AHUB detected
       ''DR'' AS dr_transmission_file_type,
       ''R'' AS dr_transaction_response_status,
       CAST(valerr.error_code AS VARCHAR) AS dr_reject_code,
       1 as Row_Rank,
       a.*
FROM 
  (
    SELECT *
    FROM ahub_dw.accumulator_detail
    WHERE job_key IN (SELECT job_key
                        FROM ahub_dw.job
                        WHERE client_file_id in (1, 9) -- file came from BCI Comm or GP
                     )
    AND out_job_key is null -- was not sent out yet
  ) a
  JOIN 
  (
    SELECT *
    FROM 
    (
      -- selects the records with errors
      SELECT *,
             ROW_NUMBER() OVER (PARTITION BY job_key,line_number ORDER BY error_level ASC,file_column_id ASC) rnk
      FROM 
      (
        SELECT 
          val.validation_result,
          val.job_key,
          val.line_number,
          cr.file_column_id,
          cr.error_level,
          cr.error_code,
          cr.column_rules_id
        FROM ahub_dw.column_rules cr
          JOIN ahub_dw.file_validation_result AS val ON cr.column_rules_id = val.column_rules_id
        WHERE val.validation_result = ''N''
              AND
              val.job_key IN (
                  SELECT job_key FROM ahub_dw.job
                  WHERE client_file_id in (1, 9) -- came from BCI Comm or GP
                  )
      )
    )
    WHERE rnk = 1 AND error_level = 1
  ) valerr
  ON a.job_key = valerr.job_key AND a.line_number = valerr.line_number



  ) z
)
WHERE RowNumber = 1
)b'
)
TO 's3://{s3_out_bucket}/{s3_file_path}' iam_role '{iam_role}'
FIXEDWIDTH
'0:200,1:2,2:7,3:10,4:80,5:1401'
ALLOWOVERWRITE
parallel off;

$$
,

-- UPDATE out_job_key query

update_query = $$

UPDATE ahub_dw.accumulator_detail
SET out_job_key = :job_key
WHERE
accumulator_detail_key in
(
    SELECT accumulator_detail_key

FROM
(
  SELECT
    accumulator_detail_key,
    ROW_NUMBER() OVER (PARTITION BY 
        transmission_identifier,patient_identifier,
        date_of_birth,transaction_identifier,
        accumulator_specific_category_type,
        accumulator_1_balance_qualifier,accumulator_2_balance_qualifier,
        accumulator_3_balance_qualifier,accumulator_4_balance_qualifier,
        accumulator_1_applied_amount_action_code,accumulator_2_applied_amount_action_code,
        accumulator_3_applied_amount_action_code,accumulator_4_applied_amount_action_code,
        accumulator_1_applied_amount,accumulator_2_applied_amount,
        accumulator_3_applied_amount,accumulator_4_applied_amount
        ORDER BY transmission_date) AS RowNumber
  FROM
  (
SELECT -- error records from CVS
  -- we are aliasing some columns to be able to use * and help out the union part and keep length in check
  'DR' as dr_transmission_file_type,
  'R' as dr_transaction_response_status,
  reject_code as dr_reject_code,
  ROW_NUMBER() OVER (PARTITION BY 
    transmission_identifier,patient_identifier,
    date_of_birth,transaction_identifier,
    accumulator_specific_category_type,
    accumulator_1_balance_qualifier,accumulator_2_balance_qualifier,
    accumulator_3_balance_qualifier,accumulator_4_balance_qualifier,
    accumulator_1_applied_amount_action_code,accumulator_2_applied_amount_action_code,
    accumulator_3_applied_amount_action_code,accumulator_4_applied_amount_action_code,
    accumulator_1_applied_amount,accumulator_2_applied_amount,
    accumulator_3_applied_amount,accumulator_4_applied_amount
    ORDER BY transmission_date) AS Row_Rank,
  *
FROM (
  SELECT acc.*
  FROM ahub_dw.accumulator_detail acc
    JOIN ahub_dw.job j ON acc.job_key = j.job_key
  WHERE 
      client_id = 2 -- CVS
      AND status = 'Success'
      AND file_status = 'Available' -- all 12 CVS files have been loaded
      AND out_job_key is null -- job has not been exported yet
      AND acc.sender_identifier = (select top 1 input_sender_id from ahub_dw.client_files where client_file_id=5)
      AND transmission_file_type = 'DR'
      AND transaction_response_status = 'R'
      AND client_pass_through in ('INGENIORXBCI00489')
   )

UNION
SELECT  -- L1 errors in incoming BCI files that AHUB detected
       'DR' AS dr_transmission_file_type,
       'R' AS dr_transaction_response_status,
       CAST(valerr.error_code AS VARCHAR) AS dr_reject_code,
       1 as Row_Rank,
       a.*
FROM 
  (
    SELECT *
    FROM ahub_dw.accumulator_detail
    WHERE job_key IN (SELECT job_key
                        FROM ahub_dw.job
                        WHERE client_file_id in (1, 9) -- file came from BCI Comm or GP
                     )
    AND out_job_key is null -- was not sent out yet
  ) a
  JOIN 
  (
    SELECT *
    FROM 
    (
      -- selects the records with errors
      SELECT *,
             ROW_NUMBER() OVER (PARTITION BY job_key,line_number ORDER BY error_level ASC,file_column_id ASC) rnk
      FROM 
      (
        SELECT 
          val.validation_result,
          val.job_key,
          val.line_number,
          cr.file_column_id,
          cr.error_level,
          cr.error_code,
          cr.column_rules_id
        FROM ahub_dw.column_rules cr
          JOIN ahub_dw.file_validation_result AS val ON cr.column_rules_id = val.column_rules_id
        WHERE val.validation_result = 'N'
              AND
              val.job_key IN (
                  SELECT job_key FROM ahub_dw.job
                  WHERE client_file_id in (1, 9) -- came from BCI Comm or GP
                  )
      )
    )
    WHERE rnk = 1 AND error_level = 1
  ) valerr
  ON a.job_key = valerr.job_key AND a.line_number = valerr.line_number
  ) z
)
WHERE RowNumber = 1
)

$$

WHERE client_file_id = 5
;


---------------- THE END ----------------
