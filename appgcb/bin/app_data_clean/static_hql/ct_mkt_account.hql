use l1_gcb_ref;
CREATE EXTERNAL TABLE account
        (acct_nbr varchar(20),
         country varchar(3),
         open_dt varchar(10))
         partitioned by (source_dt VARCHAR(8))
     ROW FORMAT DELIMITED FIELDS TERMINATED BY ','
     STORED AS PARQUET TBLPROPERTIES ("parquet.compression"="SNAPPY");
