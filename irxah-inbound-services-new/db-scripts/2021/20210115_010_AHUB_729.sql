-- Already implemented in prod ( AHUB-730) , documented here for reference , running the  update again will not hurt anything.
UPDATE ahub_dw.client_files

   SET output_sender_id = '00489'

WHERE client_file_id = 22;
-- Already implemented in prod ( AHUB-728) , documented here for reference , running the  update again will not hurt anything.
UPDATE ahub_dw.custom_to_standard_mapping
SET dest_col_value = ' ',
description = ' Fills spaace for the standard detail row for the position from 733 to 747 '
WHERE mapping_id = 69;

-- Function used in the query
-- Returns value as is  if it is NOT EMPTY ( first argument)
-- Otherwise returns the value specified in the second argument as a default
create or replace function f_sql_setdefault (varchar, varchar)
  returns varchar
stable
as $$
  select case when len(trim($1)) > 0  then $1
    else $2
  end
$$ language sql;

-- Defaults for Outbound CVS

UPDATE ahub_dw.export_setting
   SET detail_query = '

unload 
(''
SELECT 
    ''''{routing_id}'''' AS processor_routing_identification,
    record_type,transmission_file_type,version_release_number,
    ''''00489INGENIORX'''' AS sender_identifier,
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
f_sql_setdefault(accumulator_1_balance_qualifier, '''' ''''),  f_sql_setdefault(accumulator_1_network_indicator, '''' ''''),
  f_sql_setdefault(accumulator_1_applied_amount,''''0000000000''''),  f_sql_setdefault(accumulator_1_applied_amount_action_code,'''' ''''),
  f_sql_setdefault(accumulator_1_benefit_period_amount,''''0000000000''''),  f_sql_setdefault(accumulator_1_benefit_period_amount_action_code,'''' ''''),
  f_sql_setdefault(accumulator_1_remaining_balance,''''0000000000''''),    f_sql_setdefault(accumulator_1_remaining_balance_action_code,'''' ''''),
f_sql_setdefault(accumulator_2_balance_qualifier, '''' ''''),  f_sql_setdefault(accumulator_2_network_indicator, '''' ''''),
f_sql_setdefault(accumulator_2_applied_amount,''''0000000000''''),  f_sql_setdefault(accumulator_2_applied_amount_action_code,'''' ''''),
f_sql_setdefault(accumulator_2_benefit_period_amount,''''0000000000''''),  f_sql_setdefault(accumulator_2_benefit_period_amount_action_code,'''' ''''),
f_sql_setdefault(accumulator_2_remaining_balance,''''0000000000''''),    f_sql_setdefault(accumulator_2_remaining_balance_action_code,'''' ''''),
f_sql_setdefault(accumulator_3_balance_qualifier, '''' ''''),  f_sql_setdefault(accumulator_3_network_indicator, '''' ''''),
f_sql_setdefault(accumulator_3_applied_amount,''''0000000000''''),  f_sql_setdefault(accumulator_3_applied_amount_action_code,'''' ''''),
f_sql_setdefault(accumulator_3_benefit_period_amount,''''0000000000''''),  f_sql_setdefault(accumulator_3_benefit_period_amount_action_code,'''' ''''),
f_sql_setdefault(accumulator_3_remaining_balance,''''0000000000''''),    f_sql_setdefault(accumulator_3_remaining_balance_action_code,'''' ''''),
f_sql_setdefault(accumulator_4_balance_qualifier, '''' ''''),  f_sql_setdefault(accumulator_4_network_indicator, '''' ''''),
f_sql_setdefault(accumulator_4_applied_amount,''''0000000000''''),  f_sql_setdefault(accumulator_4_applied_amount_action_code,'''' ''''),
f_sql_setdefault(accumulator_4_benefit_period_amount,''''0000000000''''),  f_sql_setdefault(accumulator_4_benefit_period_amount_action_code,'''' ''''),
f_sql_setdefault(accumulator_4_remaining_balance,''''0000000000''''),    f_sql_setdefault(accumulator_4_remaining_balance_action_code,'''' ''''),
f_sql_setdefault(accumulator_5_balance_qualifier, '''' ''''),  f_sql_setdefault(accumulator_5_network_indicator, '''' ''''),
f_sql_setdefault(accumulator_5_applied_amount,''''0000000000''''),  f_sql_setdefault(accumulator_5_applied_amount_action_code,'''' ''''),
f_sql_setdefault(accumulator_5_benefit_period_amount,''''0000000000''''),  f_sql_setdefault(accumulator_5_benefit_period_amount_action_code,'''' ''''),
f_sql_setdefault(accumulator_5_remaining_balance,''''0000000000''''),    f_sql_setdefault(accumulator_5_remaining_balance_action_code,'''' ''''),
f_sql_setdefault(accumulator_6_balance_qualifier, '''' ''''),  f_sql_setdefault(accumulator_6_network_indicator, '''' ''''),
f_sql_setdefault(accumulator_6_applied_amount,''''0000000000''''),  f_sql_setdefault(accumulator_6_applied_amount_action_code,'''' ''''),
f_sql_setdefault(accumulator_6_benefit_period_amount,''''0000000000''''),  f_sql_setdefault(accumulator_6_benefit_period_amount_action_code,'''' ''''),
f_sql_setdefault(accumulator_6_remaining_balance,''''0000000000''''),    f_sql_setdefault(accumulator_6_remaining_balance_action_code,'''' ''''),
    '''' '''' as reserved_5, 
f_sql_setdefault(accumulator_7_balance_qualifier, '''' ''''),  f_sql_setdefault(accumulator_7_network_indicator, '''' ''''),
f_sql_setdefault(accumulator_7_applied_amount,''''0000000000''''),  f_sql_setdefault(accumulator_7_applied_amount_action_code,'''' ''''),
f_sql_setdefault(accumulator_7_benefit_period_amount,''''0000000000''''),  f_sql_setdefault(accumulator_7_benefit_period_amount_action_code,'''' ''''),
f_sql_setdefault(accumulator_7_remaining_balance,''''0000000000''''),    f_sql_setdefault(accumulator_7_remaining_balance_action_code,'''' ''''),
f_sql_setdefault(accumulator_8_balance_qualifier, '''' ''''),  f_sql_setdefault(accumulator_8_network_indicator, '''' ''''),
f_sql_setdefault(accumulator_8_applied_amount,''''0000000000''''),  f_sql_setdefault(accumulator_8_applied_amount_action_code,'''' ''''),
f_sql_setdefault(accumulator_8_benefit_period_amount,''''0000000000''''),  f_sql_setdefault(accumulator_8_benefit_period_amount_action_code,'''' ''''),
f_sql_setdefault(accumulator_8_remaining_balance,''''0000000000''''),    f_sql_setdefault(accumulator_8_remaining_balance_action_code,'''' ''''),
f_sql_setdefault(accumulator_9_balance_qualifier, '''' ''''),  f_sql_setdefault(accumulator_9_network_indicator, '''' ''''),
f_sql_setdefault(accumulator_9_applied_amount,''''0000000000''''),  f_sql_setdefault(accumulator_9_applied_amount_action_code,'''' ''''),
f_sql_setdefault(accumulator_9_benefit_period_amount,''''0000000000''''),  f_sql_setdefault(accumulator_9_benefit_period_amount_action_code,'''' ''''),
f_sql_setdefault(accumulator_9_remaining_balance,''''0000000000''''),    f_sql_setdefault(accumulator_9_remaining_balance_action_code,'''' ''''),
f_sql_setdefault(accumulator_10_balance_qualifier, '''' ''''),  f_sql_setdefault(accumulator_10_network_indicator, '''' ''''),
f_sql_setdefault(accumulator_10_applied_amount,''''0000000000''''),  f_sql_setdefault(accumulator_10_applied_amount_action_code,'''' ''''),
f_sql_setdefault(accumulator_10_benefit_period_amount,''''0000000000''''),  f_sql_setdefault(accumulator_10_benefit_period_amount_action_code,'''' ''''),
f_sql_setdefault(accumulator_10_remaining_balance,''''0000000000''''),    f_sql_setdefault(accumulator_10_remaining_balance_action_code,'''' ''''),
f_sql_setdefault(accumulator_11_balance_qualifier, '''' ''''),  f_sql_setdefault(accumulator_11_network_indicator, '''' ''''),
f_sql_setdefault(accumulator_11_applied_amount,''''0000000000''''),  f_sql_setdefault(accumulator_11_applied_amount_action_code,'''' ''''),
f_sql_setdefault(accumulator_11_benefit_period_amount,''''0000000000''''),  f_sql_setdefault(accumulator_11_benefit_period_amount_action_code,'''' ''''),
f_sql_setdefault(accumulator_11_remaining_balance,''''0000000000''''),    f_sql_setdefault(accumulator_11_remaining_balance_action_code,'''' ''''),
f_sql_setdefault(accumulator_12_balance_qualifier, '''' ''''),  f_sql_setdefault(accumulator_12_network_indicator, '''' ''''),
f_sql_setdefault(accumulator_12_applied_amount,''''0000000000''''),  f_sql_setdefault(accumulator_12_applied_amount_action_code,'''' ''''),
f_sql_setdefault(accumulator_12_benefit_period_amount,''''0000000000''''),  f_sql_setdefault(accumulator_12_benefit_period_amount_action_code,'''' ''''),
f_sql_setdefault(accumulator_12_remaining_balance,''''0000000000''''),    f_sql_setdefault(accumulator_12_remaining_balance_action_code,'''' ''''),
    
    '''' '''' as optional_data_indicator,
    ''''0000000000'''' as total_amount_paid,'''' '''' as total_amount_paid_action_code,
    ''''0000000000'''' as amount_of_copay,'''' '''' as amount_of_copay_action_code,
    ''''0000000000'''' as patient_pay_amount,'''' '''' as patient_pay_amount_action_code,
    ''''0000000000'''' as amount_attributed_to_product_selection_brand,
    '''' '''' as amount_attributed_to_product_selection_brand_action_code,
    ''''0000000000'''' as amount_attributed_to_sales_tax,
    '''' '''' as amount_attributed_to_sales_tax_action_code,
    ''''0000000000'''' as amount_attributed_to_processor_fee,
    '''' '''' as amount_attributed_to_processor_fee_action_code,
    ''''0000000000'''' as gross_amount_due, '''' '''' as gross_amount_due_action_code,
    ''''0000000000'''' as invoiced_amount,'''' '''' as invoiced_amount_action_code,
    ''''0000000000'''' as penalty_amount,'''' '''' as penalty_amount_action_code,
    '''' '''' as reserved_6,
    '''' '''' as product_service_identifier_qualifier, '''' '''' as product_service_identifier,
    ''''000'''' as days_supply,''''0000000000'''' as quantity_dispensed,
    '''' '''' as product_service_name,'''' '''' as brand_generic_indicator,
    '''' '''' as therapeutic_class_code_qualifier,
    '''' '''' as therapeutic_class_code,
    '''' '''' as dispensed_as_written,
    '''' '''' as reserved_7 
  FROM 
    (
      select *
      from ahub_dw.accumulator_detail
      where 

        out_job_key is null -- records not yet exported
        and transmission_file_type =''''DQ''''
        and transaction_response_status = ''''''''
        and job_key in
          (
            SELECT job_key
            FROM ahub_dw.job j
              INNER JOIN ahub_dw.client_files cf
                ON j.client_file_id = cf.client_file_id
            where
              j.status = ''''Success''''
              and cf.file_type = ''''INBOUND''''
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
                j.status = ''''Success''''
                and d.validation_result=''''N''''
                and j.client_id not in (2) -- no CVS
          )A
        )Z
      where rnk=1 and error_level=1
    ) b 
    ON a.job_key = b.job_key and a.line_number=b.line_number 
  WHERE
    b.error_level is null
  order by to_timestamp(transmission_date || (case regexp_substr(transmission_time, ''''[^0-9]+'''') when '''''''' then transmission_time else ''''000000'''' end), ''''YYYYMMDDHH24MISSMS'''')
'') 
TO ''s3://{s3_out_bucket}/{s3_file_path}'' iam_role ''{iam_role}'' 
FIXEDWIDTH 
''0:200,1:2,2:2,3:2,4:30,5:30,6:4,7:1,8:3,9:5,10:20,11:8,12:8,13:8,14:2,15:15,16:2,17:15,18:50,19:1,20:1,21:1,
22:2,23:30,24:20,25:1,26:8,27:8,28:1,29:30,30:30,31:1,32:26,33:13,34:20,35:15,36:25,37:1,38:35,39:1,40:8,41:1,
42:2,43:35,44:9,45:15,46:50,47:20,48:20,49:15,50:20,51:3,52:90,53:2,54:2,55:20,56:2,57:1,58:10,59:1,60:10,61:1,
62:10,63:1,64:2,65:1,66:10,67:1,68:10,69:1,70:10,71:1,72:2,73:1,74:10,75:1,76:10,77:1,78:10,79:1,80:2,81:1,
82:10,83:1,84:10,85:1,86:10,87:1,88:2,89:1,90:10,91:1,92:10,93:1,94:10,95:1,96:2,97:1,98:10,99:1,100:10,101:1,
102:10,103:1,104:24,105:2,106:1,107:10,108:1,109:10,110:1,111:10,112:1,113:2,114:1,115:10,116:1,117:10,118:1,
119:10,120:1,121:2,122:1,123:10,124:1,125:10,126:1,127:10,128:1,129:2,130:1,131:10,132:1,133:10,134:1,135:10,
136:1,137:2,138:1,139:10,140:1,141:10,142:1,143:10,144:1,145:2,146:1,147:10,148:1,149:10,150:1,151:10,152:1,
153:1,154:10,155:1,156:10,157:1,158:10,159:1,160:10,161:1,162:10,163:1,164:10,165:1,166:10,167:1,168:10,169:1,
170:10,171:1,172:23,173:2,174:19,175:3,176:10,177:30,178:1,179:1,180:17,181:1,182:48'' 
ALLOWOVERWRITE 
parallel off;


'
WHERE client_file_id= 22;

-- Following are left over from the other bug fixes that were fixed in PROD -- It is specified here for documentation purpose only..

-- Inbound GP File Checking

UPDATE ahub_dw.file_schedule
   SET last_poll_time = '2020-01-03 05:00:00.000',
       next_poll_time = '2020-01-05 05:00:00.000',
       cron_expression ='local: 0 0 * * TUE-SAT',
       cron_description='Every Monday to Friday , between 8 PM and 12 AM local time ( Eastern) , AHUB should check for inbound GP file. This check will be executed at 12:00 AM Tuesday ( did we receive the file in last 4 hours) which would be between 8 PM (Monday) to 12 AM , hence the schedule has been setup as TUE-SAT.',
       grace_period=240
WHERE file_schedule_id = 20;

-- Removing spaces after the word local in the cron_expression
update ahub_dw.file_schedule set cron_expression= substring(cron_expression,1,5)|| substring(cron_expression,7,20) where cron_expression like 'local %';

