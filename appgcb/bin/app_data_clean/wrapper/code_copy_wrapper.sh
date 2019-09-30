gsutil mb -c regional -l us-central1 gs://l1_app_gcb

gsutil cp -R /Users/spanda/Documents/Learning/cloud/gcp/appgcb/* gs://app_gcb_code/appgcb/
gsutil cp -R /Users/spanda/Documents/Learning/cloud/gcp/appgcb/bin/app_data_clean/config/* gs://app_gcb_code/appgcb/bin/app_data_clean/config/
gsutil cp -R /Users/spanda/Documents/Learning/cloud/gcp/appgcb/bin/app_data_clean/wrapper/* gs://app_gcb_code/appgcb/bin/app_data_clean/wrapper/
gsutil cp -R /Users/spanda/Documents/Learning/cloud/gcp/appgcb/bin/app_data_clean/util/* gs://app_gcb_code/appgcb/bin/app_data_clean/util/
gsutil cp -R /Users/spanda/Documents/Learning/cloud/gcp/appgcb/bin/app_data_clean/static_hql/* gs://app_gcb_code/appgcb/bin/app_data_clean/static_hql/


gsutil cp -R gs://app_gcb_code/appgcb/* /data/1/appgcb/
gsutil cp -R gs://app_gcb_code/appgcb/bin/app_data_clean/config/* /data/1/appgcb/bin/app_data_clean/config/
gsutil cp -R gs://app_gcb_code/appgcb/bin/app_data_clean/wrapper/* /data/1/appgcb/bin/app_data_clean/wrapper/
gsutil cp -R gs://app_gcb_code/appgcb/bin/app_data_clean/util/* /data/1/appgcb/bin/app_data_clean/util/
gsutil cp -R gs://app_gcb_code/appgcb/bin/app_data_clean/static_hql/* /data/1/appgcb/bin/app_data_clean/static_hql/


beeline -u jdbc:hive2://amlgcbdev-m:10000/default  -f /data/1/appgcb/bin/app_data_clean/static_hql/create_db_l1_gcb_ref.hql
beeline -u jdbc:hive2://amlgcbdev-m:10000/default  -f /data/1/appgcb/bin/app_data_clean/static_hql/create_db_l1_gcb_trxn.hql

beeline -u jdbc:hive2://amlgcbdev-m:10000/default  -f /data/1/appgcb/bin/app_data_clean/static_hql/create_db_l1_gcb_ref.hql
beeline -u jdbc:hive2://amlgcbdev-m:10000/default  -f /data/1/appgcb/bin/app_data_clean/static_hql/create_db_l1_gcb_trxn.hql

Msck repair table l1_gcb_ref.account;

alter table l1_gcb_ref.account add partition (source_dt >0)
Execution:

sh -x /data/1/appgcb/bin/app_data_clean/wrapper/gen_gcs_file_copy_wrapper.sh  trade acx l1_gcb_trxn 20190101
sh -x /data/1/appgcb/bin/app_data_clean/wrapper/gen_gcs_file_copy_wrapper.sh  account mcx l1_gcb_ref 20190101
sh -x /data/1/appgcb/bin/app_data_clean/wrapper/gen_gcs_file_copy_wrapper.sh  account mcx l1_gcb_ref 20190102
sh -x /data/1/appgcb/bin/app_data_clean/wrapper/gen_gcs_file_copy_wrapper.sh  account mcx l1_gcb_ref 20190103
