use app_gcb_dw;

CREATE EXTERNAL TABLE customer
        (cust_id varchar(20),
        cust_first_name varchar(20),
        cust_last_name varchar(20),
        country varchar(3))
         partitioned by (src_system varchar(3),source_dt VARCHAR(8))
     ROW FORMAT DELIMITED FIELDS TERMINATED BY ','
     stored as textFile;
