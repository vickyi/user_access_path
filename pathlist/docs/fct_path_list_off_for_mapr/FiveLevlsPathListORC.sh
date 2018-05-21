#!/bin/sh

. /etc/profile
. ~/.bash_profile

if [ $# == 1 ]; then
   dt=$1
else
   dt=`date -d -1days '+%Y-%m-%d'`
fi

THIS="$0"
THIS_DIR=`dirname "$THIS"`
cd ${THIS_DIR}

curDt=`date '+%Y-%m-%d %H:%M'`

mergeBegin=$(date +%s)

hive  -d date=$dt -f ./fct_path_list_off_for_mapr.sql
if test $? -ne 0
then
exit 11
fi
mergeEnd=$(date +%s)
echo "=====>> 合并 dw.fct_page_ref_reg + dw.fct_event_event 两张表date=$dt 的数据完成。Merge 耗时: $(($mergeEnd-$mergeBegin)) 秒"


echo "=====>> OfflinePathList 开始，待处理的数据日期周期为：$dt......"
yarn jar ./pathlist-1.0.jar com.imeijia.bi.path.PathComputeDriver $dt fct_path_list_off_for_mapr test/path_list_offline 4

if test $? -ne 0
then
exit 11
fi

mapREnd=$(date +%s)
echo "=====>> OfflinePathList 完成，处理日期为：$dt, MapReduce 耗时: $(($mapREnd-$mergeEnd)) 秒!!!"

## 删除数据
hadoop fs -rm -r /user/hive/warehouse/dw.db/fct_path_list_off_5levels/date=$dt

echo "=====>> load_to_hive 开始，将 hdfs 文件load至$DB.$TABLE ......."
hive -d date=$dt -f ./load_to_hive.sql

loadEnd=$(date +%s)
echo "=====>> load_to_hive 完成，处理日期为：$dt, 耗时: $(($loadEnd-$mapREnd)) 秒!!!"

echo "=====>> 当前时间: $curDt, 处理 OfflinePathList 全部完成，刚刚处理的数据的日期为：$dt, all_total耗时: $(($loadEnd-$mergeBegin)) 秒!!!"

if test $? -ne 0
then
exit 11
fi