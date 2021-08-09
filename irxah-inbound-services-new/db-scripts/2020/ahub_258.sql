/*
View which is used in the Accum History File Processing.
For more information refer to AHUB-258
*/

CREATE OR REPLACE VIEW ahub_stg.vw_accum_hist_dtl
AS SELECT a.recd_typ, a.member_id, a.carrier_nbr, a.account_nbr, a.group_id, a.adj_typ, a.adj_amt, a.accum_cd, a.adj_dt, a.adj_cd, a.plan_cd, a.care_facility, a.member_state_cd, a.patient_first_nm, a.patient_last_nm, a.patient_dob, a.patient_relationship_cd, a.reserved_sp, a.line_number, a.job_key, 
        CASE
            WHEN "right"(a.adj_amt::text, 1) = '{'::character varying::text THEN to_number("left"(a.adj_amt::text, 11) || '0'::character varying::text, '999999999999'::character varying::text)
            WHEN "right"(a.adj_amt::text, 1) = 'A'::character varying::text THEN to_number("left"(a.adj_amt::text, 11) || '1'::character varying::text, '999999999999'::character varying::text)
            WHEN "right"(a.adj_amt::text, 1) = 'B'::character varying::text THEN to_number("left"(a.adj_amt::text, 11) || '2'::character varying::text, '999999999999'::character varying::text)
            WHEN "right"(a.adj_amt::text, 1) = 'C'::character varying::text THEN to_number("left"(a.adj_amt::text, 11) || '3'::character varying::text, '999999999999'::character varying::text)
            WHEN "right"(a.adj_amt::text, 1) = 'D'::character varying::text THEN to_number("left"(a.adj_amt::text, 11) || '4'::character varying::text, '999999999999'::character varying::text)
            WHEN "right"(a.adj_amt::text, 1) = 'E'::character varying::text THEN to_number("left"(a.adj_amt::text, 11) || '5'::character varying::text, '999999999999'::character varying::text)
            WHEN "right"(a.adj_amt::text, 1) = 'F'::character varying::text THEN to_number("left"(a.adj_amt::text, 11) || '6'::character varying::text, '999999999999'::character varying::text)
            WHEN "right"(a.adj_amt::text, 1) = 'G'::character varying::text THEN to_number("left"(a.adj_amt::text, 11) || '7'::character varying::text, '999999999999'::character varying::text)
            WHEN "right"(a.adj_amt::text, 1) = 'H'::character varying::text THEN to_number("left"(a.adj_amt::text, 11) || '8'::character varying::text, '999999999999'::character varying::text)
            WHEN "right"(a.adj_amt::text, 1) = 'I'::character varying::text THEN to_number("left"(a.adj_amt::text, 11) || '9'::character varying::text, '999999999999'::character varying::text)
            WHEN "right"(a.adj_amt::text, 1) = '}'::character varying::text THEN to_number("left"(a.adj_amt::text, 11) || '0'::character varying::text, '999999999999'::character varying::text) * (- 1::numeric::numeric(18,0))
            WHEN "right"(a.adj_amt::text, 1) = 'J'::character varying::text THEN to_number("left"(a.adj_amt::text, 11) || '1'::character varying::text, '999999999999'::character varying::text) * (- 1::numeric::numeric(18,0))
            WHEN "right"(a.adj_amt::text, 1) = 'K'::character varying::text THEN to_number("left"(a.adj_amt::text, 11) || '2'::character varying::text, '999999999999'::character varying::text) * (- 1::numeric::numeric(18,0))
            WHEN "right"(a.adj_amt::text, 1) = 'L'::character varying::text THEN to_number("left"(a.adj_amt::text, 11) || '3'::character varying::text, '999999999999'::character varying::text) * (- 1::numeric::numeric(18,0))
            WHEN "right"(a.adj_amt::text, 1) = 'M'::character varying::text THEN to_number("left"(a.adj_amt::text, 11) || '4'::character varying::text, '999999999999'::character varying::text) * (- 1::numeric::numeric(18,0))
            WHEN "right"(a.adj_amt::text, 1) = 'N'::character varying::text THEN to_number("left"(a.adj_amt::text, 11) || '5'::character varying::text, '999999999999'::character varying::text) * (- 1::numeric::numeric(18,0))
            WHEN "right"(a.adj_amt::text, 1) = 'O'::character varying::text THEN to_number("left"(a.adj_amt::text, 11) || '6'::character varying::text, '999999999999'::character varying::text) * (- 1::numeric::numeric(18,0))
            WHEN "right"(a.adj_amt::text, 1) = 'P'::character varying::text THEN to_number("left"(a.adj_amt::text, 11) || '7'::character varying::text, '999999999999'::character varying::text) * (- 1::numeric::numeric(18,0))
            WHEN "right"(a.adj_amt::text, 1) = 'Q'::character varying::text THEN to_number("left"(a.adj_amt::text, 11) || '8'::character varying::text, '999999999999'::character varying::text) * (- 1::numeric::numeric(18,0))
            WHEN "right"(a.adj_amt::text, 1) = 'R'::character varying::text THEN to_number("left"(a.adj_amt::text, 11) || '9'::character varying::text, '999999999999'::character varying::text) * (- 1::numeric::numeric(18,0))
            ELSE to_number(a.adj_amt::text, '999999999999'::text)
        END AS int_amount, "right"(a.adj_amt::text, 7) AS adj_amount_revised
   FROM ahub_stg.stg_accum_hist_dtl a;