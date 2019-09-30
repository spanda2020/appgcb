create database appgcb_dev;

use appgcb_dev;

create table app_all_tables (
app_id varchar(50),
app_desc varchar(100),
db_name varchar(50),
tbl_name varchar(50),
create_tbl_sql_fl varchar(255),
insert_tbl_sql_fl varchar(255),
last_updt_tm DATETIME
);

create table app_prc_run_cntrl (
prc_run_id int,
app_id varchar(50),
db_name varchar(50),
tbl_name varchar(50),
process_name varchar(20),
process_seq int,
prc_start_dt date,
prc_run_dt date,
prc_run_status varchar(20),
prc_exec_time DATETIME
);

create table app_job_detl (
app_id varchar(50),
db_name varchar(50),
tbl_name varchar(50),
process_name varchar(20),
process_seq int,
wrapper_file_name varchar(255),
code_file_name varchar(255),
last_updt_tm DATETIME
);

create table etl_app_config (
etl_app_id int NOT Null,
etl_app_name varchar(50) NOT Null,
app_batch_id varchar(50)NOT Null,
active_flag VARCHAR(1)NOT Null,
last_updt_tm DATETIME NOT Null,
PRIMARY KEY (etl_app_id));


create table etl_group_config (
etl_group_id int NOT Null,
etl_app_name varchar(50) NOT Null,
app_batch_id varchar(50)NOT Null,
active_flag VARCHAR(1)NOT Null,
last_updt_tm DATETIME NOT Null,
PRIMARY KEY (etl_app_id));
