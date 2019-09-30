use app_gcb_stg;
CREATE EXTERNAL TABLE app_job_detl (
  app_id varchar(50),
  db_name varchar(50),
  tbl_name varchar(50),
  prc_name string,
  prc_job_name string,
  prc_job_seq int,
  wrapper_path string,
  wrapper_file_name string,
  code_path string,
  code_file_name  string,
  params string,
  last_updt_tm varchar(255))
ROW FORMAT DELIMITED
FIELDS TERMINATED BY '|'
STORED AS TEXTFILE;
