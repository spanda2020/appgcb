use app_gcb_stg;
CREATE EXTERNAL TABLE dim_all_keys
        (primary_key_name string,
         primary_key_val string,
         active_flag varchar(1),
         load_tm string)
         partitioned by (dim_tbl_name VARCHAR(50),prc_run_id bigint,run_dt varchar(10))
        STORED AS PARQUET TBLPROPERTIES ("parquet.compression"="SNAPPY");
