-- delete from ahub_dw.export_setting where client_file_id=22;

-- insert queries for CVS Outbound

insert into ahub_dw.export_setting (client_file_id, create_empty_file, header_query, detail_query, trailer_query, update_query)
values (22,
false, -- don't create "empty" export files

-- HEADER QUERY
$$

UNLOAD ('select ''{routing_id}'' AS PRCS_ROUT_ID,
''HD'' AS RECD_TYPE,
''T'' AS TRANSMISSION_FILE_TYP,
to_char(sysdate, ''YYYYMMDD'') AS SRC_CREATE_DT,
to_char(sysdate, ''HHMI'') AS SRC_CREATE_TS,
cf.output_sender_id as SENDER_IDENTIFIER,
cf.output_receiver_id as RECEIVER_IDENTIFIER,
''0000001'' AS BATCH_NBR,
cf.outbound_file_type_column as FILE_TYP,
''10'' AS VER_RELEASE_NBR,
'' '' AS RESERVED_SP
from ahub_dw.client_files cf
where client_file_id=22')
TO 's3://{s3_out_bucket}/{s3_file_path}' iam_role '{iam_role}'
            FIXEDWIDTH 
            '0:200,1:2,2:1,3:8,4:4,5:30,6:30,7:7,8:1,9:2,10:1415' 
            ALLOWOVERWRITE 
            parallel off;

$$,

-- DETAIL QUERY 
$$

unload 
('
SELECT 
    ''{routing_id}'' AS processor_routing_identification,
    record_type,transmission_file_type,version_release_number,
    (select top 1 output_sender_id from ahub_dw.client_files where client_file_id=22) AS sender_identifier,
    (select top 1 output_receiver_id from ahub_dw.client_files where client_file_id=22) AS receiver_identifier,
    submission_number,transaction_response_status,reject_code,
    record_length,reserved_1,transmission_date,transmission_time,
    date_of_service,
    service_provider_identifier_qualifier,service_provider_identifier,
    document_reference_identifier_qualifier,document_reference_identifier,transmission_identifier, 
    benefit_type,in_network_indicator,formulary_status,accumulator_action_code,sender_reference_number,insurance_code,
    accumulator_balance_benefit_type,benefit_effective_date,benefit_termination_date,accumulator_change_source_code,
    transaction_identifier,transaction_identifier_cross_reference,
    adjustment_reason_code,accumulator_reference_time_stamp,reserved_2,
    cardholder_identifier,group_identifier,patient_first_name,
    middle_initial,patient_last_name,patient_relationship_code,date_of_birth,patient_gender_code,
    patient_state_province_address,cardholder_last_name,carrier_number,
    contract_number,client_pass_through,family_identifier_number,
    cardholder_identifier_alternate,group_identifier_alternate,patient_identifier,person_code,reserved_3,
    accumulator_balance_count,accumulator_specific_category_type, reserved_4,
    accumulator_1_balance_qualifier,accumulator_1_network_indicator,
    accumulator_1_applied_amount,accumulator_1_applied_amount_action_code,
    accumulator_1_benefit_period_amount,accumulator_1_benefit_period_amount_action_code,
    accumulator_1_remaining_balance,accumulator_1_remaining_balance_action_code,
    accumulator_2_balance_qualifier,accumulator_2_network_indicator,
    accumulator_2_applied_amount,accumulator_2_applied_amount_action_code,
    accumulator_2_benefit_period_amount,accumulator_2_benefit_period_amount_action_code,
    accumulator_2_remaining_balance,accumulator_2_remaining_balance_action_code,
    accumulator_3_balance_qualifier,accumulator_3_network_indicator,
    accumulator_3_applied_amount,accumulator_3_applied_amount_action_code,
    accumulator_3_benefit_period_amount,accumulator_3_benefit_period_amount_action_code,
    accumulator_3_remaining_balance,accumulator_3_remaining_balance_action_code,
    accumulator_4_balance_qualifier,accumulator_4_network_indicator,
    accumulator_4_applied_amount,accumulator_4_applied_amount_action_code,
    accumulator_4_benefit_period_amount,accumulator_4_benefit_period_amount_action_code,
    accumulator_4_remaining_balance,accumulator_4_remaining_balance_action_code,
    accumulator_5_balance_qualifier,accumulator_5_network_indicator,
    accumulator_5_applied_amount,accumulator_5_applied_amount_action_code,
    accumulator_5_benefit_period_amount,accumulator_5_benefit_period_amount_action_code,
    accumulator_5_remaining_balance,accumulator_5_remaining_balance_action_code,
    accumulator_6_balance_qualifier,accumulator_6_network_indicator,
    accumulator_6_applied_amount,accumulator_6_applied_amount_action_code,
    accumulator_6_benefit_period_amount,accumulator_6_benefit_period_amount_action_code,
    accumulator_6_remaining_balance,accumulator_6_remaining_balance_action_code,
    '' '' as reserved_5, 
    accumulator_7_balance_qualifier,accumulator_7_network_indicator,
    accumulator_7_applied_amount,accumulator_7_applied_amount_action_code,
    accumulator_7_benefit_period_amount,accumulator_7_benefit_period_amount_action_code,
    accumulator_7_remaining_balance,accumulator_7_remaining_balance_action_code,
    accumulator_8_balance_qualifier,accumulator_8_network_indicator,
    accumulator_8_applied_amount,accumulator_8_applied_amount_action_code,
    accumulator_8_benefit_period_amount,accumulator_8_benefit_period_amount_action_code,
    accumulator_8_remaining_balance,accumulator_8_remaining_balance_action_code,
    accumulator_9_balance_qualifier,accumulator_9_network_indicator,
    accumulator_9_applied_amount,accumulator_9_applied_amount_action_code,
    accumulator_9_benefit_period_amount,accumulator_9_benefit_period_amount_action_code,
    accumulator_9_remaining_balance,accumulator_9_remaining_balance_action_code,
    accumulator_10_balance_qualifier,accumulator_10_network_indicator,
    accumulator_10_applied_amount,accumulator_10_applied_amount_action_code,
    accumulator_10_benefit_period_amount,accumulator_10_benefit_period_amount_action_code,
    accumulator_10_remaining_balance,accumulator_10_remaining_balance_action_code,
    accumulator_11_balance_qualifier,accumulator_11_network_indicator,
    accumulator_11_applied_amount,accumulator_11_applied_amount_action_code,
    accumulator_11_benefit_period_amount,accumulator_11_benefit_period_amount_action_code,
    accumulator_11_remaining_balance,accumulator_11_remaining_balance_action_code,
    accumulator_12_balance_qualifier,accumulator_12_network_indicator,
    accumulator_12_applied_amount,accumulator_12_applied_amount_action_code,
    accumulator_12_benefit_period_amount,accumulator_12_benefit_period_amount_action_code,
    accumulator_12_remaining_balance,accumulator_12_remaining_balance_action_code,
    
    '' '' as optional_data_indicator,
    ''0000000000'' as total_amount_paid,'' '' as total_amount_paid_action_code,
    ''0000000000'' as amount_of_copay,'' '' as amount_of_copay_action_code,
    ''0000000000'' as patient_pay_amount,'' '' as patient_pay_amount_action_code,
    ''0000000000'' as amount_attributed_to_product_selection_brand,
    '' '' as amount_attributed_to_product_selection_brand_action_code,
    ''0000000000'' as amount_attributed_to_sales_tax,
    '' '' as amount_attributed_to_sales_tax_action_code,
    ''0000000000'' as amount_attributed_to_processor_fee,
    '' '' as amount_attributed_to_processor_fee_action_code,
    ''0000000000'' as gross_amount_due, '' '' as gross_amount_due_action_code,
    ''0000000000'' as invoiced_amount,'' '' as invoiced_amount_action_code,
    ''0000000000'' as penalty_amount,'' '' as penalty_amount_action_code,
    '' '' as reserved_6,
    '' '' as product_service_identifier_qualifier, '' '' as product_service_identifier,
    ''000'' as days_supply,''0000000000'' as quantity_dispensed,
    '' '' as product_service_name,'' '' as brand_generic_indicator,
    '' '' as therapeutic_class_code_qualifier,
    '' '' as therapeutic_class_code,
    '' '' as dispensed_as_written,
    '' '' as reserved_7 
  FROM 
    (
      select *
      from ahub_dw.accumulator_detail
      where 

        out_job_key is null -- records not yet exported
        and transmission_file_type =''DQ''
        and transaction_response_status = ''''
        and job_key in
          (
            SELECT job_key
            FROM ahub_dw.job j
              INNER JOIN ahub_dw.client_files cf
                ON j.client_file_id = cf.client_file_id
            where
              j.status = ''Success''
              and cf.file_type = ''INBOUND''
              and cf.client_id not in (2) -- exclude CVS inbound files
          )
    ) a
    LEFT JOIN 
    (
      SELECT *
      FROM
        (
          SELECT
            *,
            rank () over (partition by job_key, line_number order by error_level asc, file_column_id asc ) rnk 
          FROM
          (
            SELECT
              d.validation_result,
              d.job_key,
              d.line_number,
              c.file_column_id,
              c.error_level, 
              c.column_rules_id
            FROM
              ahub_dw.column_rules c 
                INNER JOIN ahub_dw.file_validation_result as d 
                  ON c.column_rules_id = d.column_rules_id  
                INNER JOIN ahub_dw.job j
                  ON d.job_key = j.job_key
              WHERE
                j.status = ''Success''
                and d.validation_result=''N''
                and j.client_id not in (2) -- no CVS
          )A
        )Z
      where rnk=1 and error_level=1
    ) b 
    ON a.job_key = b.job_key and a.line_number=b.line_number 
  WHERE
    b.error_level is null
  ORDER BY a.accumulator_detail_key
') 
TO 's3://{s3_out_bucket}/{s3_file_path}' iam_role '{iam_role}' 
FIXEDWIDTH 
'0:200,1:2,2:2,3:2,4:30,5:30,6:4,7:1,8:3,9:5,10:20,11:8,12:8,13:8,14:2,15:15,16:2,17:15,18:50,19:1,20:1,21:1,
22:2,23:30,24:20,25:1,26:8,27:8,28:1,29:30,30:30,31:1,32:26,33:13,34:20,35:15,36:25,37:1,38:35,39:1,40:8,41:1,
42:2,43:35,44:9,45:15,46:50,47:20,48:20,49:15,50:20,51:3,52:90,53:2,54:2,55:20,56:2,57:1,58:10,59:1,60:10,61:1,
62:10,63:1,64:2,65:1,66:10,67:1,68:10,69:1,70:10,71:1,72:2,73:1,74:10,75:1,76:10,77:1,78:10,79:1,80:2,81:1,
82:10,83:1,84:10,85:1,86:10,87:1,88:2,89:1,90:10,91:1,92:10,93:1,94:10,95:1,96:2,97:1,98:10,99:1,100:10,101:1,
102:10,103:1,104:24,105:2,106:1,107:10,108:1,109:10,110:1,111:10,112:1,113:2,114:1,115:10,116:1,117:10,118:1,
119:10,120:1,121:2,122:1,123:10,124:1,125:10,126:1,127:10,128:1,129:2,130:1,131:10,132:1,133:10,134:1,135:10,
136:1,137:2,138:1,139:10,140:1,141:10,142:1,143:10,144:1,145:2,146:1,147:10,148:1,149:10,150:1,151:10,152:1,
153:1,154:10,155:1,156:10,157:1,158:10,159:1,160:10,161:1,162:10,163:1,164:10,165:1,166:10,167:1,168:10,169:1,
170:10,171:1,172:23,173:2,174:19,175:3,176:10,177:30,178:1,179:1,180:17,181:1,182:48' 
ALLOWOVERWRITE 
parallel off;
  

$$
, 

-- TRAILER QUERY
$$

UNLOAD 
('select 
''{routing_id}'' AS PRCS_ROUT_ID,
''TR'' AS RECD_TYPE,
''0000001'' AS BATCH_NBR,
lpad(b.cnt,10,''0'') AS REC_CNT,
'' '' AS MSG_TXT,
'' ''AS RESERVED_SP
from 
  (
    SELECT cast(count(1) as varchar) cnt

    FROM 
      (
        select *
        from ahub_dw.accumulator_detail
        where 
          out_job_key is null -- records not yet exported
          and transmission_file_type =''DQ''
          and transaction_response_status = ''''
          and job_key in
            (
              SELECT job_key
              FROM ahub_dw.job j
                INNER JOIN ahub_dw.client_files cf
                  ON j.client_file_id = cf.client_file_id
              where
                j.status = ''Success''
                and cf.file_type = ''INBOUND''
                and cf.client_id not in (2) -- exclude CVS inbound files
            )
      ) a
      LEFT JOIN 
      (
        SELECT *
        FROM
          (
            SELECT
              *,
              rank () over (partition by job_key, line_number order by error_level asc, file_column_id asc ) rnk 
            FROM
            (
              SELECT
                d.validation_result,
                d.job_key,
                d.line_number,
                c.file_column_id,
                c.error_level, 
                c.column_rules_id
              FROM
                ahub_dw.column_rules c 
                  INNER JOIN ahub_dw.file_validation_result as d 
                    ON c.column_rules_id = d.column_rules_id  
                  INNER JOIN ahub_dw.job j
                    ON d.job_key = j.job_key
                WHERE
                  j.status = ''Success''
                  and d.validation_result=''N''
                  and j.client_id not in (2) -- no CVS
            )A
          )Z
        where rnk=1 and error_level=1
      ) b 
      ON a.job_key = b.job_key and a.line_number=b.line_number 
    WHERE
      b.error_level is null
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

$$

UPDATE ahub_dw.accumulator_detail
SET out_job_key = :job_key
WHERE 
accumulator_detail_key in 
(
  SELECT accumulator_detail_key

  FROM 
    (
      select *
      from ahub_dw.accumulator_detail
      where 
        out_job_key is null -- records not yet exported
        and transmission_file_type ='DQ'
        and transaction_response_status = ''
        and job_key in
          (
            SELECT job_key
            FROM ahub_dw.job j
              INNER JOIN ahub_dw.client_files cf
                ON j.client_file_id = cf.client_file_id
            where
              j.status = 'Success'
              and cf.file_type = 'INBOUND'
              and cf.client_id not in (2) -- exclude CVS inbound files
          )
    ) a
    LEFT JOIN 
    (
      SELECT *
      FROM
        (
          SELECT
            *,
            rank () over (partition by job_key, line_number order by error_level asc, file_column_id asc ) rnk 
          FROM
          (
            SELECT
              d.validation_result,
              d.job_key,
              d.line_number,
              c.file_column_id,
              c.error_level, 
              c.column_rules_id
              FROM
                ahub_dw.column_rules c 
                  INNER JOIN ahub_dw.file_validation_result as d 
                    ON c.column_rules_id = d.column_rules_id  
                  INNER JOIN ahub_dw.job j
                    ON d.job_key = j.job_key
                WHERE
                  j.status = 'Success'
                  and d.validation_result='N'
                  and j.client_id not in (2) -- no CVS
          )A
        )Z
      where rnk=1 and error_level=1
    ) b 
    ON a.job_key = b.job_key and a.line_number=b.line_number 
  WHERE
    b.error_level is null
  -- per requirements, export to CVS is ordered by load order,
  -- whereas export to the clients is by timestamp order
  ORDER BY a.accumulator_detail_key
)

$$
)
;


