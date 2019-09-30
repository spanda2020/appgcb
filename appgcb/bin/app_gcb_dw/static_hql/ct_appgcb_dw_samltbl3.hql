create table if not exists sampleTbl3 (
acct1 int,
tranId string
)
partitioned by (refNbr int,load_dt string)
stored as parquet;
