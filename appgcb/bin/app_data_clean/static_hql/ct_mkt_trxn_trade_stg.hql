use l1_gcb_trxn_stg;
CREATE EXTERNAL TABLE trade
        (tran_id varchar(30),
         acct_nbr varchar(20),
         firm_cd int,
         symbol varchar(10),
         trade_size bigint,
         trade_dt varchar(10),
         settle_dt varchar(10),
         exec_dt varchar(10))
         partitioned by (source_dt VARCHAR(10))
     ROW FORMAT DELIMITED FIELDS TERMINATED BY ','
     STORED AS TEXTFILE;
