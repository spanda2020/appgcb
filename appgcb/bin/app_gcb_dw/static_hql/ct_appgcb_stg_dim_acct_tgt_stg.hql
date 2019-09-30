use app_gcb_stg;
CREATE EXTERNAL TABLE dim_account_tgt_stg
        (acct_nbr varchar(20),
         country varchar(3),
         lvid varchar(20),
         open_dt varchar(10),
         load_tm string)
         partitioned by (prc_run_id bigint,run_dt VARCHAR(8))
     STORED AS PARQUET TBLPROPERTIES ("parquet.compression"="SNAPPY");
