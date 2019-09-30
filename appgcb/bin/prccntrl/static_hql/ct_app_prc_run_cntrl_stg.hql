use app_gcb_stg;
CREATE EXTERNAL TABLE app_prc_run_cntrl_stg (
prc_run_id bigint,
app_id varchar(100),
db_name varchar(100),
tbl_name varchar(100),
prc_job_name string,
prc_job_seq int,
prc_start_dt varchar(10),
prc_run_dt varchar(10),
prc_run_status varchar(100),
prc_exec_time varchar(50) )
partitioned by (prc_name string)
ROW FORMAT DELIMITED
FIELDS TERMINATED BY '|'
STORED AS TEXTFILE;
