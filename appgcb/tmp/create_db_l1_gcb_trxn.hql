
--SET hivevar:hql_env_file=/data/1/appgcb/config/dev_env.hql;
source ${hql_env_file};

create database l1_gcb_trxn
location ${l1_gcb_trxn_db_path} ;
