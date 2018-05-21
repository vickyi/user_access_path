use dw;

ALTER TABLE dw.fct_path_list_off_6levels DROP IF EXISTS PARTITION (date='${date}');
LOAD DATA INPATH 'hdfs://nameservice1/user/hadoop/dw_realtime/test/path_list_offline_6levels/date=${date}/gu_hash=0' INTO TABLE dw.fct_path_list_off_6levels PARTITION (date='${date}');
LOAD DATA INPATH 'hdfs://nameservice1/user/hadoop/dw_realtime/test/path_list_offline_6levels/date=${date}/gu_hash=1' INTO TABLE dw.fct_path_list_off_6levels PARTITION (date='${date}');
LOAD DATA INPATH 'hdfs://nameservice1/user/hadoop/dw_realtime/test/path_list_offline_6levels/date=${date}/gu_hash=2' INTO TABLE dw.fct_path_list_off_6levels PARTITION (date='${date}');
LOAD DATA INPATH 'hdfs://nameservice1/user/hadoop/dw_realtime/test/path_list_offline_6levels/date=${date}/gu_hash=3' INTO TABLE dw.fct_path_list_off_6levels PARTITION (date='${date}');
LOAD DATA INPATH 'hdfs://nameservice1/user/hadoop/dw_realtime/test/path_list_offline_6levels/date=${date}/gu_hash=4' INTO TABLE dw.fct_path_list_off_6levels PARTITION (date='${date}');
LOAD DATA INPATH 'hdfs://nameservice1/user/hadoop/dw_realtime/test/path_list_offline_6levels/date=${date}/gu_hash=5' INTO TABLE dw.fct_path_list_off_6levels PARTITION (date='${date}');
LOAD DATA INPATH 'hdfs://nameservice1/user/hadoop/dw_realtime/test/path_list_offline_6levels/date=${date}/gu_hash=6' INTO TABLE dw.fct_path_list_off_6levels PARTITION (date='${date}');
LOAD DATA INPATH 'hdfs://nameservice1/user/hadoop/dw_realtime/test/path_list_offline_6levels/date=${date}/gu_hash=7' INTO TABLE dw.fct_path_list_off_6levels PARTITION (date='${date}');
LOAD DATA INPATH 'hdfs://nameservice1/user/hadoop/dw_realtime/test/path_list_offline_6levels/date=${date}/gu_hash=8' INTO TABLE dw.fct_path_list_off_6levels PARTITION (date='${date}');
LOAD DATA INPATH 'hdfs://nameservice1/user/hadoop/dw_realtime/test/path_list_offline_6levels/date=${date}/gu_hash=9' INTO TABLE dw.fct_path_list_off_6levels PARTITION (date='${date}');
LOAD DATA INPATH 'hdfs://nameservice1/user/hadoop/dw_realtime/test/path_list_offline_6levels/date=${date}/gu_hash=a' INTO TABLE dw.fct_path_list_off_6levels PARTITION (date='${date}');
LOAD DATA INPATH 'hdfs://nameservice1/user/hadoop/dw_realtime/test/path_list_offline_6levels/date=${date}/gu_hash=b' INTO TABLE dw.fct_path_list_off_6levels PARTITION (date='${date}');
LOAD DATA INPATH 'hdfs://nameservice1/user/hadoop/dw_realtime/test/path_list_offline_6levels/date=${date}/gu_hash=c' INTO TABLE dw.fct_path_list_off_6levels PARTITION (date='${date}');
LOAD DATA INPATH 'hdfs://nameservice1/user/hadoop/dw_realtime/test/path_list_offline_6levels/date=${date}/gu_hash=d' INTO TABLE dw.fct_path_list_off_6levels PARTITION (date='${date}');
LOAD DATA INPATH 'hdfs://nameservice1/user/hadoop/dw_realtime/test/path_list_offline_6levels/date=${date}/gu_hash=e' INTO TABLE dw.fct_path_list_off_6levels PARTITION (date='${date}');
LOAD DATA INPATH 'hdfs://nameservice1/user/hadoop/dw_realtime/test/path_list_offline_6levels/date=${date}/gu_hash=f' INTO TABLE dw.fct_path_list_off_6levels PARTITION (date='${date}');