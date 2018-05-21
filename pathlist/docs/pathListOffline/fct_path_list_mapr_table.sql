use dw;
drop table if EXISTS fct_path_list_off_for_mapr;
CREATE TABLE `fct_path_list_off_for_mapr`(
page_level_id string,
gu_id string,
page_id string,
page_value string,
page_lvl2_value string,
event_id string,
event_value string,
event_lvl2_value string,
starttime string,
loadtime string,
pit_type string,
-- pit_value string,
-- pit_no string,
sortdate string,
sorthour string,
lplid string,
ptplid string,
gid string,
test_id string,
select_id string,
ug_id string,
rule_id string,
x_page_value string,
ref_x_page_value string,
session_id string,
user_id string,
terminal_id string,
site_id string,
ugroup string,
utm_id string
  )
PARTITIONED BY (`gu_hash` string);

-- 非层级字段
-- gu_id
-- user_id
-- session_id
-- terminal_id
-- site_id,
-- ugroup,
-- page_id,
-- page_value,
-- page_level_id
-- utmid