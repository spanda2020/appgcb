use app_gcb_stg;
CREATE EXTERNAL TABLE app_hive_tbl_detl (
  db_name varchar(50),
  db_dir string,
  tbl_name varchar(50),
  partition_col string,
  create_tbl_sql_path string,
  create_tbl_sql_fl string,
  last_updt_tm varchar(255))
  partitioned by (app_id varchar(50))
  ROW FORMAT DELIMITED
  FIELDS TERMINATED BY '|'
  STORED AS TEXTFILE;
