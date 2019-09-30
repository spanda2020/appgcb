drop database app_gcb_stg cascade;
drop database app_gcb_dw cascade;
drop database l1_gcb_trxn cascade;
drop database l1_gcb_ref cascade;
drop database l1_gcb_trxn_stg cascade;
drop database l1_gcb_ref_stg cascade;

drop table app_prc_run_cntrl_stg;
hdfs://quickstart.cloudera:8020/user/hive/app_gcb_data/app_gcb_stg/sampletbl1
hdfs dfs -mkdir -p hdfs://quickstart.cloudera:8020/user/hive/app_gcb_data/app_gcb_stg/sampletbl1/load_dt=20190101/
hdfs dfs -put -f /data/1/appgcb/data/20190101_sampleTbl.csv hdfs://quickstart.cloudera:8020/user/hive/app_gcb_data/app_gcb_stg/sampletbl1/load_dt=20190101/
Msck repair table app_gcb_stg.sampletbl1


hdfs dfs -cp /user/hive/app_gcb_data/app_gcb_stg/sampletbl2/* /user/hive/app_gcb_data/app_gcb_stg/sampletbl3/
Msck repair table app_gcb_stg.sampletbl3

spark-submit /data/1/appgcb/python/gen_src_tgt_hive_load.py  "${srcTbl}" "${tgtTbl}" $insertOverwriteFlag
spark-submit /data/1/appgcb/python/gen_src_tgt_hive_load.py  "${srcTbl}" "${tgtTbl}" $insertOverwriteFlag $filterCol ${startDt} ${runDt}

hdfs dfs -put -f /data/1/appgcb/bin/prccntrl/data/ct_app_job_detl_src_ld.txt hdfs://quickstart.cloudera:8020/user/hive/app_gcb_data/app_gcb_stg/app_job_detl/
hdfs dfs -mkdir -p hdfs://quickstart.cloudera:8020/user/hive/l1_app_gcb/process/l1_gcb_ref_stg/customer/src_system=mcx/source_dt=20190101/
hdfs dfs -mkdir -p hdfs://quickstart.cloudera:8020/user/hive/l1_app_gcb/process/l1_gcb_ref_stg/customer/src_system=mcx/source_dt=20190102/
hdfs dfs -mkdir -p hdfs://quickstart.cloudera:8020/user/hive/l1_app_gcb/process/l1_gcb_ref_stg/customer/src_system=mcx/source_dt=20190103/
hdfs dfs -put -f /data/1/appgcb/data/20190101_amc_cutomer.csv hdfs://quickstart.cloudera:8020/user/hive/l1_app_gcb/process/l1_gcb_ref_stg/customer/src_system=mcx/source_dt=20190101/
hdfs dfs -put -f /data/1/appgcb/data/20190102_amc_cutomer.csv hdfs://quickstart.cloudera:8020/user/hive/l1_app_gcb/process/l1_gcb_ref_stg/customer/src_system=mcx/source_dt=20190102/
hdfs dfs -put -f /data/1/appgcb/data/20190103_amc_cutomer.csv hdfs://quickstart.cloudera:8020/user/hive/l1_app_gcb/process/l1_gcb_ref_stg/customer/src_system=mcx/source_dt=20190103/
Msck repair table l1_gcb_ref_stg.customer
Msck repair table app_gcb_dw.customer
hdfs dfs -put -f /data/1/appgcb/data/20190101_trade_acx.csv hdfs://quickstart.cloudera:8020/user/hive/l1_app_gcb/process/l1_gcb_trxn_stg/trade/source_dt=20190101/

alter table l1_gcb_trxn_stg.trade add partition(source_dt='20190101')

hdfs dfs -rm -r -skipTrash hdfs://quickstart.cloudera:8020/user/hive/app_gcb_data/app_gcb_stg/app_prc_run_cntrl_stg/*
hdfs dfs -rm -r -skipTrash hdfs://quickstart.cloudera:8020/user/hive/app_gcb_data/app_gcb_stg/app_prc_run_cntrl/*
sh -x gen_create_db.sh dev create_all_db_file_list.txt
sh -x gen_create_db.sh dev ct_l0_l1_tbl_file_list.txt

sh -x /data/1/appgcb/bin/wrapper/gen_app_batch_start_close.sh app_data_clean app_data_clean_trxn_ld_batch_start prc_l1_trxn_ld dev

sh -x /data/1/appgcb/bin/wrapper/gen_job_exec.sh app_data_clean prc_l1_trxn_ld app_data_clean_trade_trxn_ld_stg  dev
sh -x /data/1/appgcb/bin/wrapper/gen_job_exec.sh app_data_clean  prc_l1_ref_ld app_data_clean_account_ref_ld_stg dev

sh -x /data/1/appgcb/bin/wrapper/gen_app_batch_start_close.sh app_data_clean prc_l1_trxn_ld app_data_clean_trxn_ld_app_start W
sh -x /data/1/appgcb/bin/wrapper/gen_app_batch_start_close.sh app_data_clean prc_l1_trxn_ld app_data_clean_trxn_ld_prc_start W
sh -x /data/1/appgcb/bin/wrapper/gen_job_exec.sh app_data_clean prc_l1_trxn_ld app_data_clean_trade_trxn_ld_stg
sh -x /data/1/appgcb/bin/wrapper/gen_app_batch_start_close.sh app_data_clean prc_l1_trxn_ld app_data_clean_trxn_ld_prc_complete W
sh -x /data/1/appgcb/bin/wrapper/gen_app_batch_start_close.sh app_data_clean prc_l1_trxn_ld app_data_clean_trxn_ld_prc_cntrl_load  W
sh -x /data/1/appgcb/bin/wrapper/gen_app_batch_start_close.sh app_data_clean prc_l1_trxn_ld app_data_clean_trxn_ld_app_complete W

D W BW M

sh -x /data/1/appgcb/bin/wrapper/gen_app_batch_start_close.sh app_data_clean app_data_clean_ref_ld_batch_start prc_l1_ref_ld dev
sh -x /data/1/appgcb/bin/wrapper/gen_app_batch_start_close.sh app_data_clean app_data_clean_ref_ld_batch_complete prc_l1_ref_ld dev
sh -x /data/1/appgcb/bin/wrapper/gen_app_batch_start_close.sh app_data_clean app_data_clean_ref_ld_prc_cntrl_load prc_l1_ref_ld dev


 spark-submit /data/1/appgcb/python/gen_create_job_exec_detl.py app_data_clean prc_l1_trxn_ld 1234545
 sh -x /data/1/appgcb/bin/wrapper/gen_hive_tbl_ld.sh app_data_clean prc_l1_trxn_ld /data/1/appgcb/bin/app_data_clean/hql/gcb_clean_trxn_trade_ld.hql dev

 /* source /data/1/appgcb/tmp/hive_env_var.hql;
 source /data/1/appgcb/config/dev_env.hql;

 use ${app_data_clean_trxn_db};

 insert overwrite table trade partition (source_dt)
 select * from ${app_data_clean_trxn_stg_db}.trade where source_dt >="${prc_start_dt}"  and source_dt <= "${prc_run_dt}"; */


/Users/spanda/Documents/Learning/cloud/gcp/appgcb/bin/app_data_clean/etl/pyspark/gen_create_job_exec_detl.py
