use test;
hive> show create table fct_path_list_test;
OK
CREATE TABLE `fct_path_list_test`(
  `gu_id` string,
  `endtime` bigint,
  `last_entrance_page_id` int,
  `last_guide_page_id` int,
  `last_before_goods_page_id` int,
  `last_entrance_page_value` string,
  `last_guide_page_value` string,
  `last_before_goods_page_value` string,
  `last_entrance_event_id` int,
  `last_guide_event_id` int,
  `last_before_goods_event_id` int,
  `last_entrance_event_value` string,
  `last_guide_event_value` string,
  `last_before_goods_event_value` string,
  `last_entrance_timestamp` bigint,
  `last_guide_timestamp` bigint,
  `last_before_goods_timestamp` bigint,
  `guide_lvl2_page_id` int,
  `guide_lvl2_page_value` string,
  `guide_lvl2_event_id` int,
  `guide_lvl2_event_value` string,
  `guide_lvl2_timestamp` bigint,
  `guide_is_del` int,
  `guide_lvl2_is_del` int,
  `before_goods_is_del` int,
  `entrance_page_lvl2_value` string,
  `guide_page_lvl2_value` string,
  `guide_lvl2_page_lvl2_value` string,
  `before_goods_page_lvl2_value` string,
  `entrance_event_lvl2_value` string,
  `guide_event_lvl2_value` string,
  `guide_lvl2_event_lvl2_value` string,
  `before_goods_event_lvl2_value` string,
  `rule_id` string,
  `test_id` string,
  `select_id` string,
  `last_entrance_pit_type` int,
  `last_entrance_sortdate` string,
  `last_entrance_sorthour` int,
  `last_entrance_lplid` int,
  `last_entrance_ptplid` int,
  `last_entrance_ug_id` int)
PARTITIONED BY (
  `date` string)
ROW FORMAT SERDE
  'org.apache.hadoop.hive.ql.io.orc.OrcSerde'
STORED AS INPUTFORMAT
  'org.apache.hadoop.hive.ql.io.orc.OrcInputFormat'
OUTPUTFORMAT
  'org.apache.hadoop.hive.ql.io.orc.OrcOutputFormat'
LOCATION
  'hdfs://nameservice1/user/hive/warehouse/test.db/fct_path_list_test'
TBLPROPERTIES (
  'transient_lastDdlTime'='1492510579')
Time taken: 0.033 seconds, Fetched: 55 row(s)