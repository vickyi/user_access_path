use test;

-- ${date}
LOAD DATA INPATH 'hdfs://nameservice1/user/hadoop/dw_realtime/test/path_list_offline/date=${date}/gu_hash=0' OVERWRITE INTO TABLE fct_path_list_test PARTITION (date='${date}');

LOAD DATA INPATH 'hdfs://nameservice1/user/hadoop/dw_realtime/test/path_list_offline/date=${date}/gu_hash=1' INTO TABLE fct_path_list_test PARTITION (date='${date}');
LOAD DATA INPATH 'hdfs://nameservice1/user/hadoop/dw_realtime/test/path_list_offline/date=${date}/gu_hash=2' INTO TABLE fct_path_list_test PARTITION (date='${date}');
LOAD DATA INPATH 'hdfs://nameservice1/user/hadoop/dw_realtime/test/path_list_offline/date=${date}/gu_hash=3' INTO TABLE fct_path_list_test PARTITION (date='${date}');
LOAD DATA INPATH 'hdfs://nameservice1/user/hadoop/dw_realtime/test/path_list_offline/date=${date}/gu_hash=4' INTO TABLE fct_path_list_test PARTITION (date='${date}');
LOAD DATA INPATH 'hdfs://nameservice1/user/hadoop/dw_realtime/test/path_list_offline/date=${date}/gu_hash=5' INTO TABLE fct_path_list_test PARTITION (date='${date}');
LOAD DATA INPATH 'hdfs://nameservice1/user/hadoop/dw_realtime/test/path_list_offline/date=${date}/gu_hash=6' INTO TABLE fct_path_list_test PARTITION (date='${date}');
LOAD DATA INPATH 'hdfs://nameservice1/user/hadoop/dw_realtime/test/path_list_offline/date=${date}/gu_hash=7' INTO TABLE fct_path_list_test PARTITION (date='${date}');
LOAD DATA INPATH 'hdfs://nameservice1/user/hadoop/dw_realtime/test/path_list_offline/date=${date}/gu_hash=8' INTO TABLE fct_path_list_test PARTITION (date='${date}');
LOAD DATA INPATH 'hdfs://nameservice1/user/hadoop/dw_realtime/test/path_list_offline/date=${date}/gu_hash=9' INTO TABLE fct_path_list_test PARTITION (date='${date}');
LOAD DATA INPATH 'hdfs://nameservice1/user/hadoop/dw_realtime/test/path_list_offline/date=${date}/gu_hash=a' INTO TABLE fct_path_list_test PARTITION (date='${date}');
LOAD DATA INPATH 'hdfs://nameservice1/user/hadoop/dw_realtime/test/path_list_offline/date=${date}/gu_hash=b' INTO TABLE fct_path_list_test PARTITION (date='${date}');
LOAD DATA INPATH 'hdfs://nameservice1/user/hadoop/dw_realtime/test/path_list_offline/date=${date}/gu_hash=c' INTO TABLE fct_path_list_test PARTITION (date='${date}');
LOAD DATA INPATH 'hdfs://nameservice1/user/hadoop/dw_realtime/test/path_list_offline/date=${date}/gu_hash=d' INTO TABLE fct_path_list_test PARTITION (date='${date}');
LOAD DATA INPATH 'hdfs://nameservice1/user/hadoop/dw_realtime/test/path_list_offline/date=${date}/gu_hash=e' INTO TABLE fct_path_list_test PARTITION (date='${date}');
LOAD DATA INPATH 'hdfs://nameservice1/user/hadoop/dw_realtime/test/path_list_offline/date=${date}/gu_hash=f' INTO TABLE fct_path_list_test PARTITION (date='${date}');