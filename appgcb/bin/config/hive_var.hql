

-- DB Path
set hivevar:l1_gcb_trxn_db_path="hdfs://quickstart.cloudera:8020/user/hive/l1_app_gcb/process/l1_gcb_trxn";
set hivevar:l1_gcb_ref_db_path="hdfs://quickstart.cloudera:8020/user/hive/l1_app_gcb/process/l1_gcb_ref";
set hivevar:l1_gcb_trxn_stg_db_path="hdfs://quickstart.cloudera:8020/user/hive/l1_app_gcb/process/l1_gcb_trxn_stg";
set hivevar:l1_gcb_ref_stg_db_path="hdfs://quickstart.cloudera:8020/user/hive/l1_app_gcb/process/l1_gcb_ref_stg";
set hivevar:app_gcb_dw_db_path="hdfs://quickstart.cloudera:8020/user/hive/app_gcb_data/app_gcb_dw";
set hivevar:app_gcb_stg_db_path="hdfs://quickstart.cloudera:8020/user/hive/app_gcb_data/app_gcb_stg";


set hivevar:prc_cntrl_db=app_gcb_stg;
set hivevar:app_data_clean_trxn_db =l1_gcb_trxn;
set hivevar:app_data_clean_trxn_stg_db =l1_gcb_trxn_stg;
set hivevar:app_data_clean_ref_db =l1_gcb_ref;
set hivevar:app_data_clean_ref_stg_db =l1_gcb_ref;
-- DB details
