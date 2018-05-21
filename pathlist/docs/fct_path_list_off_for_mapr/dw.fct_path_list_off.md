dw.fct_path_list_off
=======

## 说明
此作业为mapreduce+hive作业，对用户的访问（点击和浏览）进行排序和归类，通过mapreduce计算，并将结果写入hive ORC 外表

hive临时表：temp.fct_path_list_off_for_mapr

mapreduce输入目录：hdfs://nameservice1/user/hive/warehouse/temp.db/fct_path_list_off_for_mapr
mapreduce输出目录：hdfs://nameservice1/user/hadoop/dw_realtime/test/path_list_offline/

最终目标表：dw.fct_path_list_off_5levels

## mapreduce 二次排序

### 项目解析
#### 项目结构
#### GuIdDatePair
访客id和访问时间组成的key
#### GuIdPartitioner
分区类会根据map输出的key来决定数据
#### PathComputeDriver
计算主类
#### PathMapper
map 过程，对访问记录进行初步的处理，生成新的key pair 和value
#### PathGroupingComparator
定制如何对键进行分组，判断哪些记录会分配到同一个reduce处理
#### PathReducer
#### TextArrayWritable

### 项目地址
https://github.com/vickyi/VisitPath
