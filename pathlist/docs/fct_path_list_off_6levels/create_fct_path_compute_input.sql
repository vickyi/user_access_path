-- alter table fct_page_ref_reg add columns (scene_value int comment '场景值') cascade
-- alter table fct_page_ref_reg add columns (openid string comment '微信openid') cascade

use temp;
drop table if EXISTS fct_path_compute_input_6levels;
CREATE TABLE if NOT EXISTS `fct_path_compute_input_6levels`(
page_level_id int,
gu_id string,
page_id int,
page_value string,
page_lvl2_value string,
event_id int,
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
user_id int,
terminal_id int,
site_id int,
ugroup string,
utm_id int,
app_version string,
scene_value int,
openid string
  )
PARTITIONED BY (date string, gu_hash string);