/*==============================================================*/
/* Script : IRX_ACCUMHUB_Cleanup_MVP2.sql					    */
/* Author : Sanjay Sharma (ag61753)							    */
/* Project : AHUB (MVP2)                                        */
/* Production Release Date: 05/22/2020                          */
/* Purpose : Cleanup script to delete data from following tables*/
/*            AHUB_DW.ACCUMULATOR_DETAIL                        */
/*            AHUB_DW.ACCUMULATOR_RECONCILIATION_DETAIL			*/
/*            AHUB_DW.FILE_VALIDATION_RESULT           			*/
/*            AHUB_DW.JOB_DETAIL                       			*/
/*            AHUB_DW.JOB                              			*/
/*==============================================================*/

/*==============================================================*/
/* Schema: AHUB_DW									            */
/*==============================================================*/

/* Below DML search for Smoke files with word SMOKE word and respectively find the JOB_KEY and perorm the delete operation*/

DELETE FROM ahub_dw.file_validation_result            WHERE JOB_KEY IN (SELECT DISTINCT JOB_KEY FROM ahub_dw.job WHERE UPPER(file_name) LIKE '%SMOKE%');

DELETE FROM ahub_dw.ACCUMULATOR_DETAIL                WHERE JOB_KEY IN (SELECT DISTINCT JOB_KEY FROM ahub_dw.job WHERE UPPER(file_name) LIKE '%SMOKE%');
DELETE FROM ahub_dw.ACCUMULATOR_RECONCILIATION_DETAIL WHERE JOB_KEY IN (SELECT DISTINCT JOB_KEY FROM ahub_dw.job WHERE UPPER(file_name) LIKE '%SMOKE%');

DELETE FROM ahub_dw.job_detail                        WHERE JOB_KEY IN (SELECT DISTINCT JOB_KEY FROM ahub_dw.job WHERE UPPER(file_name) LIKE '%SMOKE%');
DELETE FROM ahub_dw.job                               WHERE JOB_KEY IN (SELECT DISTINCT JOB_KEY FROM ahub_dw.job WHERE UPPER(file_name) LIKE '%SMOKE%');