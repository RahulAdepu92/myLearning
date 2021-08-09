alter table ahub_dw.accumulator_detail
add column out_job_key bigint
default NULL;

;

CREATE TABLE IF NOT EXISTS ahub_dw.export_setting (
    export_setting_id   integer identity(1,1) not null,
    client_file_id      integer NOT NULL,
    effective_from      date DEFAULT '2000-01-01',
    effective_to        date DEFAULT '2999-01-01',
    header_query        varchar(max) NOT NULL,
    detail_query        varchar(max) NOT NULL,
    trailer_query       varchar(max) NOT NULL,
    update_query        varchar(max) NOT NULL,
	PRIMARY KEY (export_setting_id),
	CONSTRAINT FK_EXPORT_SETTING_CLIENT_FILES foreign key(client_file_id) references AHUB_DW.CLIENT_FILES(client_file_id)
) distkey(client_file_id);

;





