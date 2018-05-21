set hive.auto.convert.join=false;
set hive.ignore.mapjoin.hint=false;
set hive.groupby.skewindata=true;
set hive.exec.dynamic.partition.mode=nonstrict;
set hive.exec.max.dynamic.partitions.pernode=2000;

-- alter table fct_page_ref_reg add columns (scene_value int comment '场景值') cascade
-- alter table fct_page_ref_reg add columns (openid string comment '微信openid') cascade

use temp;
-- 删除时间分区
alter table fct_path_compute_input_6levels DROP if exists partition (date="${date}", gu_hash="0");
alter table fct_path_compute_input_6levels DROP if exists partition (date="${date}", gu_hash="1");
alter table fct_path_compute_input_6levels DROP if exists partition (date="${date}", gu_hash="2");
alter table fct_path_compute_input_6levels DROP if exists partition (date="${date}", gu_hash="3");
alter table fct_path_compute_input_6levels DROP if exists partition (date="${date}", gu_hash="4");
alter table fct_path_compute_input_6levels DROP if exists partition (date="${date}", gu_hash="5");
alter table fct_path_compute_input_6levels DROP if exists partition (date="${date}", gu_hash="6");
alter table fct_path_compute_input_6levels DROP if exists partition (date="${date}", gu_hash="7");
alter table fct_path_compute_input_6levels DROP if exists partition (date="${date}", gu_hash="8");
alter table fct_path_compute_input_6levels DROP if exists partition (date="${date}", gu_hash="9");
alter table fct_path_compute_input_6levels DROP if exists partition (date="${date}", gu_hash="a");
alter table fct_path_compute_input_6levels DROP if exists partition (date="${date}", gu_hash="b");
alter table fct_path_compute_input_6levels DROP if exists partition (date="${date}", gu_hash="c");
alter table fct_path_compute_input_6levels DROP if exists partition (date="${date}", gu_hash="d");
alter table fct_path_compute_input_6levels DROP if exists partition (date="${date}", gu_hash="e");
alter table fct_path_compute_input_6levels DROP if exists partition (date="${date}", gu_hash="f");

insert overwrite table fct_path_compute_input_6levels partition (date="${date}", gu_hash)
select
  x.page_level_id,
  x.gu_id,
  x.page_id,
  x.page_value,
  x.page_lvl2_value,
  x.event_id,
  x.event_value,
  x.event_lvl2_value,
  x.starttime,
  x.loadtime,
  x.pit_type,
  x.sortdate,
  x.sorthour,
  x.lplid,
  x.ptplid,
  x.gid,
  x.test_id,
  x.select_id,
  x.ug_id,
  x.rule_id,
  x.x_page_value,
  x.ref_x_page_value,
  x.session_id,
  x.user_id,
  x.terminal_id,
  x.site_id,
  x.ugroup,
  x.utm_id,
  x.app_version,
  x.scene_value,
  x.openid,
  x.gu_hash
from (
    SELECT
        case when page_id = 219 then 1 when page_level_id > 0 then page_level_id + 1 else 0 end page_level_id,
        gu_id,
        page_id,
        nvl(page_value, "") page_value,
        nvl(page_lvl2_value, "") page_lvl2_value,
        event_id,
        nvl(event_value, "") event_value,
        nvl(event_lvl2_value, "") event_lvl2_value,
        nvl(starttime, "") starttime,
        nvl(loadtime, "") loadtime,
        nvl(pit_type, "") pit_type,
        nvl(sortdate, "") sortdate,
        nvl(sorthour, "") sorthour,
        nvl(lplid, "") lplid,
        nvl(ptplid, "") ptplid,
        nvl(gid, "") gid,
        nvl(test_id, "") test_id,
        nvl(select_id, "") select_id,
        nvl(ug_id, "") ug_id,
        nvl(rule_id, "") rule_id,
        nvl(x_page_value, "") x_page_value,
        nvl(ref_x_page_value, "") ref_x_page_value,
        nvl(session_id, "") session_id,
        nvl(user_id,0) user_id,
        nvl(terminal_id, -1) terminal_id,
        nvl(site_id, -1) site_id,
        nvl(ugroup, "") ugroup,
        nvl(utm_id, -1) utm_id,
        case when (app_version is null or app_version = "") then "0" else app_version end app_version,
        nvl(scene_value, -1) scene_value,
        case when (openid is null or openid = "") then "0" else openid end openid,
        -- 以gu_id 最后一个字母作为分区字段
      case when substring(gu_id, -1) rlike '[A-Fa-f0-9]' then lower(substring(gu_id, -1))
      else 'g' end gu_hash
      FROM  dw.fct_event_reg
    where date = '${date}'
        and gu_id is not null
        and gu_id <> ""
        and event_id > 0
--         and substring(gu_id, -1) rlike '[A-Fa-f0-9]'

    UNION ALL

    SELECT
      case when page_id = 219 then 1 when page_level_id > 0 then page_level_id + 1 else 0 end page_level_id,
      gu_id,
      page_id,
      nvl(page_value,"") page_value,
      nvl(page_lvl2_value, "") page_lvl2_value,
      -1 event_id,
      "" event_value,
      "" event_lvl2_value,
      nvl(starttime, "") starttime,
      "" loadtime,
      nvl(pit_type, "") pit_type,
      "" sortdate,
      "" sorthour,
      "" lplid,
      "" ptplid,
      nvl(gid, "") gid,
      nvl(test_id, "") test_id,
      nvl(select_id, "") select_id,
      "" ug_id,
      nvl(rule_id, "") rule_id,
      nvl(x_page_value, "") x_page_value,
      nvl(ref_x_page_value, "") ref_x_page_value,
      nvl(session_id, "") session_id,
      nvl(user_id, -1) user_id,
      nvl(terminal_id, -1) terminal_id,
      nvl(site_id, -1) site_id,
      nvl(ugroup, "") ugroup,
      nvl(utm_id, -1) utm_id,
      case when (app_version is null or app_version = "") then "0" else app_version end app_version,
      nvl(scene_value, -1) scene_value,
      case when (openid is null or openid = "") then "0" else openid end openid,
      -- 以gu_id 最后一个字母作为分区字段
      case when substring(gu_id, -1) rlike '[A-Fa-f0-9]' then lower(substring(gu_id, -1))
      else 'g' end gu_hash
    FROM  dw.fct_page_ref_reg
    where date = '${date}'
      and gu_id is not null
      and gu_id <> ""
--       and substring(gu_id, -1) rlike '[A-Fa-f0-9]'
    ) x
    where length(x.starttime) > 0
    and length(x.gu_id) > 0;