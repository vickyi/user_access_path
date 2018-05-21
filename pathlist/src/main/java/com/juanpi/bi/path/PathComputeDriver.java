package com.imeijia.bi.path;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.jobcontrol.ControlledJob;
import org.apache.hadoop.mapreduce.lib.jobcontrol.JobControl;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;
import org.apache.orc.mapred.OrcStruct;
import org.apache.orc.mapreduce.OrcOutputFormat;

import java.io.IOException;
import java.text.MessageFormat;

/**
 * Created by vkzhang@yeah.net on 2017/7/13.
 */
public class PathComputeDriver extends Configured implements Tool {

    // hdfs://nameservice1/user/hive/warehouse/dw.db/fct_path_list_mapr
    private static String base = "hdfs://nameservice1/user/hive";

    /**
     * hdfs://nameservice1/user/hive/warehouse/dw.db/fct_path_list_mapr/gu_hash=a/
     * @param SOURCE_DIR
     * @param guStr
     * @return
     */
    private String getInputPath(String SOURCE_DIR, String dateStr, String guStr) {
        // warehouse/dw.db/fct_path_list_mapr
        String patternStr = "{0}/warehouse/{1}/{2}/date={3}/gu_hash={4}/";
        return MessageFormat.format(patternStr, base, "temp.db", SOURCE_DIR, dateStr, guStr);
    }

    /**
     * hdfs://nameservice1/user/hadoop/dw_realtime/fct_for_path_list_offline/gu_hash=a/
     * @param TARGET_DIR
     * @param dateStr
     * @param guStr
     * @return
     */
    private String getOutputPath(String TARGET_DIR, String dateStr, String guStr) {
        String patternStr = "{0}/{1}/date={2}/gu_hash={3}/";
        return MessageFormat.format(patternStr, "hdfs://nameservice1/user/hadoop/dw_realtime", TARGET_DIR, dateStr, guStr);
    }

    /**
     * 清楚输出路径下的文件
     * @param basePath
     * @param outPath
     * @param conf
     */
    private void cleanOutPut(String basePath, String outPath, Configuration conf) {

        conf.set("fs.hdfs.impl", "org.apache.hadoop.hdfs.DistributedFileSystem");

        try {
            FileSystem fs = FileSystem.get(new Path(basePath).toUri(), conf);
            // 清理待存放数据的目录
            if (fs.exists(new Path(outPath))) {
                fs.delete(new Path(outPath), true);
            }
        } catch (IOException e) {
            System.out.println(("初始化FileSystem失败！"));
            System.out.println(e.getMessage());
        }
    }

    public ControlledJob lastGuString(String SOURCE_DIR, String guStr, String TARGET_DIR, String dateStr, Configuration conf) throws Exception {
        // 文件输入路径
        String inputPath = getInputPath(SOURCE_DIR, dateStr, guStr);
        System.out.println("PathList文件输入路径:" + inputPath);

        // PathList文件落地路径
        String outputPath = getOutputPath(TARGET_DIR, dateStr, guStr);

        System.out.println("PathList文件落地路径:" + outputPath);

        cleanOutPut(base, outputPath, conf);

        Job job = jobConstructor(inputPath, outputPath, conf, guStr);

        ControlledJob controlledJob = new ControlledJob(conf);

        controlledJob.setJob(job);

        return controlledJob;
    }

    /**
     *
     * @param inputPath 输入
     * @param outputPath 输出
     * @param conf
     * @return Job job
     * @throws Exception
     */
    private Job jobConstructor(String inputPath, String outputPath, Configuration conf, String guStr) throws Exception {

        String entrance0 = "entrance0_page_id:int,entrance0_page_value:string,entrance0_page_lvl2_value:string,entrance0_event_id:int,entrance0_event_value:string,entrance0_event_lvl2_value:string,entrance0_starttime:string,entrance0_loadtime:string,entrance0_pit_type:string,entrance0_pit_value:string,entrance0_pit_no:string,entrance0_sortdate:string,entrance0_sorthour:string,entrance0_lplid:string,entrance0_ptplid:string,entrance0_gid:string,entrance0_testid:string,entrance0_selectid:string,entrance0_ug_id:string,entrance0_rule_id:string,entrance0_x_page_value:string,entrance0_ref_x_page_value:string,entrance0_scene_value:int";
        String entrance  = "entrance_page_id:int,entrance_page_value:string,entrance_page_lvl2_value:string,entrance_event_id:int,entrance_event_value:string,entrance_event_lvl2_value:string,entrance_starttime:string,entrance_loadtime:string,entrance_pit_type:string,entrance_pit_value:string,entrance_pit_no:string,entrance_sortdate:string,entrance_sorthour:string,entrance_lplid:string,entrance_ptplid:string,entrance_gid:string,entrance_testid:string,entrance_selectid:string,entrance_ug_id:string,entrance_rule_id:string,entrance_x_page_value:string,entrance_ref_x_page_value:string,entrance_scene_value:int";
        String entrance2 = "entrance2_page_id:int,entrance2_page_value:string,entrance2_page_lvl2_value:string,entrance2_event_id:int,entrance2_event_value:string,entrance2_event_lvl2_value:string,entrance2_starttime:string,entrance2_loadtime:string,entrance2_pit_type:string,entrance2_pit_value:string,entrance2_pit_no:string,entrance2_sortdate:string,entrance2_sorthour:string,entrance2_lplid:string,entrance2_ptplid:string,entrance2_gid:string,entrance2_testid:string,entrance2_selectid:string,entrance2_ug_id:string,entrance2_rule_id:string,entrance2_x_page_value:string,entrance2_ref_x_page_value:string,entrance2_scene_value:int";
        String guide     = "guide_page_id:int,guide_page_value:string,guide_page_lvl2_value:string,guide_event_id:int,guide_event_value:string,guide_event_lvl2_value:string,guide_starttime:string,guide_loadtime:string,guide_pit_type:string,guide_pit_value:string,guide_pit_no:string,guide_sortdate:string,guide_sorthour:string,guide_lplid:string,guide_ptplid:string,guide_gid:string,guide_testid:string,guide_selectid:string,guide_ug_id:string,guide_rule_id:string,guide_x_page_value:string,guide_ref_x_page_value:string,guide_scene_value:int";
        String guide2    = "guide2_page_id:int,guide2_page_value:string,guide2_page_lvl2_value:string,guide2_event_id:int,guide2_event_value:string,guide2_event_lvl2_value:string,guide2_starttime:string,guide2_loadtime:string,guide2_pit_type:string,guide2_pit_value:string,guide2_pit_no:string,guide2_sortdate:string,guide2_sorthour:string,guide2_lplid:string,guide2_ptplid:string,guide2_gid:string,guide2_testid:string,guide2_selectid:string,guide2_ug_id:string,guide2_rule_id:string,guide2_x_page_value:string,guide2_ref_x_page_value:string,guide2_scene_value:int";
        String before_goods = "before_goods_page_id:int,before_goods_page_value:string,before_goods_page_lvl2_value:string,before_goods_event_id:int,before_goods_event_value:string,before_goods_event_lvl2_value:string,before_goods_starttime:string,before_goods_loadtime:string,before_goods_pit_type:string,before_goods_pit_value:string,before_goods_pit_no:string,before_goods_sortdate:string,before_goods_sorthour:string,before_goods_lplid:string,before_goods_ptplid:string,before_goods_gid:string,before_goods_testid:string,before_goods_selectid:string,before_goods_ug_id:string,before_goods_rule_id:string,before_goods_x_page_value:string,before_goods_ref_x_page_value:string,before_goods_scene_value:int";

        String nonlevl = "gu_id:string,user_id:int,start_time:string,session_id:string,terminal_id:int,site_id:int,ugroup:string,page_id:int,page_value:string,page_level_id:int,utm_id:int,event_id:int,event_value:string,app_version:string,openid:string";

        // 定义 schema
        String schema = "struct<" +
                entrance0
                + "," + entrance
                + "," + entrance2
                + "," + guide
                + "," + guide2
                + "," + before_goods
                + "," + nonlevl
                + ">";

        conf.set("orc.mapred.output.schema", schema);

        String jobName = PathComputeDriver.class.getName() + "_" + guStr;

        Job job = Job.getInstance(conf, jobName);

        System.out.println(jobName + " start >>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>");

        // !! http://stackoverflow.com/questions/21373550/class-not-found-exception-in-mapreduce-wordcount-job
        job.setJarByClass(PathComputeDriver.class);

        // 指定输入文件路径
        FileInputFormat.setInputPaths(job, inputPath);
        /**
         * 使用job.setInputFormatClass定义的InputFormat将输入的数据集分割成小数据块splites，同时InputFormat提供一个RecordReder的实现。
         * 本例子中使用的是TextInputFormat，他提供的RecordReder会将文本的一行的行号作为key，这一行的文本作为value。
         * 这就是自定义Map的输入是<LongWritable, Text>的原因
         */
        job.setInputFormatClass(TextInputFormat.class);//指定哪个类用来格式化输入文件

        /**
         * 然后调用自定义Map的map方法，将一个个<LongWritable, Text>对输入给Map的map方法。
         * 最终是生成一个List<GuIdDatePair, TextArrayWritable>
         * 在map阶段的最后，会先调用job.setPartitionerClass对这个List进行分区，每个分区映射到一个reducer。
         */
        job.setMapperClass(PathMapper.class);

        // 指定输出<k2,v2>的类型
        job.setMapOutputKeyClass(GuIdDatePair.class);
        job.setMapOutputValueClass(TextArrayWritable.class);

        // 在map阶段的最后，会先调用job.setPartitionerClass对这个List进行分区，每个分区映射到一个reduce
        job.setPartitionerClass(GuIdPartitioner.class);
        // reduce会输出到一个文件中
        job.setNumReduceTasks(1);

        // 控制哪些键分到一个reduce
        // 要么继承WritableComparator
        job.setGroupingComparatorClass(PathGroupingComparator.class);
        // 要么实现 RawComparator
        // job.setGroupingComparatorClass(RawGroupingComparator.class);

        //2.2 指定自定义的reduce类
        job.setReducerClass(PathReducer.class);

        //设置最终输出结果<key,value>类型；
        job.setOutputKeyClass(NullWritable.class);
        job.setOutputValueClass(OrcStruct.class);

        //2.3 指定输出到哪里
        FileOutputFormat.setOutputPath(job, new Path(outputPath));

        //设定输出文件的格式化类
        // job.setOutputFormatClass(TextOutputFormat.class);
        job.setOutputFormatClass(OrcOutputFormat.class);

        return job;
    }

    @Override
    public int run(String[] args) throws Exception {

        if(args.length != 4) {
            System.err.println("Usage: yarn jar path_to_jar dateStr inputPath outputPath step");
            System.exit(2);
        }

        String dateStr = args[0];

        if (dateStr.length() < 10) {
            System.out.println("我们约定时间参数的格式为：yyyy-mm-dd.例如：2017-04-01");
        }

        String SOURCE_DIR = args[1];

        String TARGET_DIR = args[2];

        String stepStr = args[3];

        Configuration conf = getConf();

        int start = 0x0;
        int end   = 0xf;

        int step = Integer.valueOf(stepStr);

        int tmp = start + step;

        int succ = 0;

        while (start <= end) {
            // 遍历16个分区

            String groupName = "PathComputeDriver_" + start;

            System.out.println(groupName + " start >>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>");

            JobControl jobControl = new JobControl(groupName);

            for (int i = start; i <= tmp; i++) {
                String guStr = String.format("%x", i);

                // 文件输入路径
                String inputPath = getInputPath(SOURCE_DIR, dateStr, guStr);
                System.out.println("PathList文件输入路径:" + inputPath);

                // PathList文件落地路径
                String outputPath = getOutputPath(TARGET_DIR, dateStr, guStr);

                System.out.println("PathList文件落地路径:" + outputPath);

                cleanOutPut(base, outputPath, conf);

                Job job = jobConstructor(inputPath, outputPath, conf, guStr);

                ControlledJob controlledJob = new ControlledJob(conf);

                controlledJob.setJob(job);

                jobControl.addJob(controlledJob);

            }

            start = tmp + 1;
            tmp += step;

            if(tmp >= end) {
                tmp = end;
            }

            Thread jcThread = new Thread(jobControl);
            jcThread.start();

            System.out.println("循环判断jobControl运行状态 >>>>>>>>>>>>>>>>");

            // https://stackoverflow.com/questions/12374928/hadoop-mapreduce-chain-jobs-jobcontrol-doesnt-stop
            while (true) {

                if (jobControl.allFinished()) {
                    System.out.println("====>> jobControl.allFinished=" + jobControl.getSuccessfulJobList());
                    jobControl.stop();
                    // 如果不加 break 或者 return，程序会一直循环
                    break;
                }

                if (jobControl.getFailedJobList().size() > 0) {
                    succ = 0;
                    System.out.println("====>> jobControl.getFailedJobList=" + jobControl.getFailedJobList());
                    jobControl.stop();

                    // 如果不加 break 或者 return，程序会一直循环
                    break;
                }
            }
        }

        return succ;
    }

    public static void main(String[] args) throws Exception {
        int res = ToolRunner.run(new PathComputeDriver(), args);
        System.out.println(">>>>>>>>>>>>>>>> PathComputeDriver Finish! <<<<<<<<<<<<<<<<");
        System.exit(res);
//        System.out.println(PathComputeDriver.class.getName());
    }
}
