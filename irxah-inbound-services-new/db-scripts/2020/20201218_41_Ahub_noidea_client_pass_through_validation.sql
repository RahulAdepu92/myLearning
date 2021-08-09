UPDATE ahub_dw.column_rules
   SET validation_type = 'RANGE',
       equal_to = '',
       list_of_values = 'INGENIORXBCI00489,INGENIORXMELTON00489,INGENIORXGILA00489,INGENIORXMOLDFIB00489'
WHERE column_rules_id = 1340;
