package com.imeijia.bi.path;

import com.google.common.base.Joiner;
import com.imeijia.bi.utils.LogPrinter;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.orc.TypeDescription;
import org.apache.orc.mapred.OrcStruct;

/**
 * Reducer 是所有用户定制 Reducer 类的基类，和 Mapper 类似，它也有setup，reduce，cleanup和run方法，
 * 其中setup和cleanup含义和Mapper相同，reduce是真正合并Mapper结果的地方，它的输入是key和这个key对应的所有value的一个迭代器，同时还包括Reducer的上下文。
 * Created by vkzhang@yeah.net on 2017/7/13.
 */
public class PathReducer extends Reducer<GuIdDatePair, TextArrayWritable, NullWritable, OrcStruct> {

    String entrance0  = "entrance0_page_id:int,entrance0_page_value:string,entrance0_page_lvl2_value:string,entrance0_event_id:int,entrance0_event_value:string,entrance0_event_lvl2_value:string,entrance0_starttime:string,entrance0_loadtime:string,entrance0_pit_type:string,entrance0_pit_value:string,entrance0_pit_no:string,entrance0_sortdate:string,entrance0_sorthour:string,entrance0_lplid:string,entrance0_ptplid:string,entrance0_gid:string,entrance0_testid:string,entrance0_selectid:string,entrance0_ug_id:string,entrance0_rule_id:string,entrance0_x_page_value:string,entrance0_ref_x_page_value:string,entrance0_scene_value:int";
    String entrance  = "entrance_page_id:int,entrance_page_value:string,entrance_page_lvl2_value:string,entrance_event_id:int,entrance_event_value:string,entrance_event_lvl2_value:string,entrance_starttime:string,entrance_loadtime:string,entrance_pit_type:string,entrance_pit_value:string,entrance_pit_no:string,entrance_sortdate:string,entrance_sorthour:string,entrance_lplid:string,entrance_ptplid:string,entrance_gid:string,entrance_testid:string,entrance_selectid:string,entrance_ug_id:string,entrance_rule_id:string,entrance_x_page_value:string,entrance_ref_x_page_value:string,entrance_scene_value:int";
    String entrance2 = "entrance2_page_id:int,entrance2_page_value:string,entrance2_page_lvl2_value:string,entrance2_event_id:int,entrance2_event_value:string,entrance2_event_lvl2_value:string,entrance2_starttime:string,entrance2_loadtime:string,entrance2_pit_type:string,entrance2_pit_value:string,entrance2_pit_no:string,entrance2_sortdate:string,entrance2_sorthour:string,entrance2_lplid:string,entrance2_ptplid:string,entrance2_gid:string,entrance2_testid:string,entrance2_selectid:string,entrance2_ug_id:string,entrance2_rule_id:string,entrance2_x_page_value:string,entrance2_ref_x_page_value:string,entrance2_scene_value:int";
    String guide     = "guide_page_id:int,guide_page_value:string,guide_page_lvl2_value:string,guide_event_id:int,guide_event_value:string,guide_event_lvl2_value:string,guide_starttime:string,guide_loadtime:string,guide_pit_type:string,guide_pit_value:string,guide_pit_no:string,guide_sortdate:string,guide_sorthour:string,guide_lplid:string,guide_ptplid:string,guide_gid:string,guide_testid:string,guide_selectid:string,guide_ug_id:string,guide_rule_id:string,guide_x_page_value:string,guide_ref_x_page_value:string,guide_scene_value:int";
    String guide2    = "guide2_page_id:int,guide2_page_value:string,guide2_page_lvl2_value:string,guide2_event_id:int,guide2_event_value:string,guide2_event_lvl2_value:string,guide2_starttime:string,guide2_loadtime:string,guide2_pit_type:string,guide2_pit_value:string,guide2_pit_no:string,guide2_sortdate:string,guide2_sorthour:string,guide2_lplid:string,guide2_ptplid:string,guide2_gid:string,guide2_testid:string,guide2_selectid:string,guide2_ug_id:string,guide2_rule_id:string,guide2_x_page_value:string,guide2_ref_x_page_value:string,guide2_scene_value:int";
    String before_goods = "before_goods_page_id:int,before_goods_page_value:string,before_goods_page_lvl2_value:string,before_goods_event_id:int,before_goods_event_value:string,before_goods_event_lvl2_value:string,before_goods_starttime:string,before_goods_loadtime:string,before_goods_pit_type:string,before_goods_pit_value:string,before_goods_pit_no:string,before_goods_sortdate:string,before_goods_sorthour:string,before_goods_lplid:string,before_goods_ptplid:string,before_goods_gid:string,before_goods_testid:string,before_goods_selectid:string,before_goods_ug_id:string,before_goods_rule_id:string,before_goods_x_page_value:string,before_goods_ref_x_page_value:string,before_goods_scene_value:int";

    String nonlevl = "gu_id:string,user_id:int,start_time:string,session_id:string,terminal_id:int,site_id:int,ugroup:string,page_id:int,page_value:string,page_level_id:int,utm_id:int,event_id:int,event_value:string,app_version:string,openid:string";

    private final NullWritable nw = NullWritable.get();

    // 定义 schema
    private TypeDescription schema = TypeDescription.fromString(
            "struct<" +
                    entrance0
                    + "," + entrance
                    + "," + entrance2
                    + "," + guide
                    + "," + guide2
                    + "," + before_goods
                    + "," + nonlevl
                    + ">"
    );

    // createValue creates the correct value type for the schema
    private OrcStruct pair = (OrcStruct) OrcStruct.createValue(schema);

    /**
     * @param k2
     * @param v2s
     * @param context
     */
    protected void reduce(GuIdDatePair k2,
                          Iterable<TextArrayWritable> v2s,
                          Context context) {
        initLevl("entrance0");
        initLevl("entrance");
        initLevl("entrance2");
        initLevl("guide");
        initLevl("guide2");
        initLevl("before_goods");

        for (TextArrayWritable v2 : v2s) {
            try {
                String pageLvlIdStr = v2.toStrings()[0];
                String pageLvl = v2.toStrings()[1];
                int pageLvlId = Integer.parseInt(pageLvlIdStr);

                String[] lvls = pageLvl.split("#");

                // 实际字段来源为starttime，只后来创建对应表时，字段名字写成了endtime
                pair.setFieldValue("page_id", new IntWritable(Integer.valueOf(lvls[0])));
                pair.setFieldValue("page_value", new Text(lvls[1]));
                pair.setFieldValue("start_time", new Text(lvls[6]));
                pair.setFieldValue("session_id", new Text(lvls[22]));
                pair.setFieldValue("user_id", new IntWritable(Integer.valueOf(lvls[23])));
                pair.setFieldValue("terminal_id", new IntWritable(Integer.valueOf(lvls[24])));
                pair.setFieldValue("site_id", new IntWritable(Integer.valueOf(lvls[25])));
                pair.setFieldValue("ugroup", new Text(lvls[26]));
                pair.setFieldValue("page_level_id", new IntWritable(pageLvlId));
                pair.setFieldValue("utm_id", new IntWritable(Integer.valueOf(lvls[27])));
                pair.setFieldValue("gu_id", new Text(lvls[28]));
                pair.setFieldValue("event_id", new IntWritable(Integer.valueOf(lvls[3])));
                pair.setFieldValue("event_value", new Text(lvls[4]));
                pair.setFieldValue("app_version", new Text(lvls[29]));

                pair.setFieldValue("openid", new Text(lvls[31]));

                if (pageLvlId == 1) {
                    setLevl("entrance0", lvls);

                    initLevl("entrance");
                    initLevl("entrance2");
                    initLevl("guide");
                    initLevl("guide2");
                    initLevl("before_goods");

                } else if (pageLvlId == 2) {
                    setLevl("entrance", lvls);

                    initLevl("entrance2");
                    initLevl("guide");
                    initLevl("guide2");
                    initLevl("before_goods");

                } else if (pageLvlId == 3) {
                    setLevl("entrance2", lvls);

                    initLevl("guide");
                    initLevl("guide2");
                    initLevl("before_goods");

                } else if (pageLvlId == 4) {
                    setLevl("guide", lvls);

                    initLevl("guide2");
                    initLevl("before_goods");

                } else if (pageLvlId == 5) {
                    setLevl("guide2", lvls);

                    initLevl("before_goods");
                } else if (pageLvlId == 6) {
                    setLevl("before_goods", lvls);
                }

                try {
                    context.write(nw, pair);
                } catch (Exception e) {
                    System.out.println("======>>Reduce Exception: " + e.getClass() + "\n==>" + LogPrinter.printStackTraceStr(e) + "\n==>data=" + Joiner.on("-").join(v2.toStrings()));
                }
            } catch (Exception e) {
                System.out.println("======>>Reduce Exception: " + e.getClass() + "\n==>" + LogPrinter.printStackTraceStr(e) + "\n==>data=" + Joiner.on("-").join(v2.toStrings()));
            }
        }
    }

    /**
     * 初始化层级
     * @param key
     */
    protected void initLevl(String key) {
        Text txt = new Text("");
        IntWritable intw = new IntWritable(0);

        pair.setFieldValue(key + "_" + "page_id", intw);
        pair.setFieldValue(key + "_" + "page_value", txt);
        pair.setFieldValue(key + "_" + "page_lvl2_value", txt);
        pair.setFieldValue(key + "_" + "event_id", intw);
        pair.setFieldValue(key + "_" + "event_value", txt);
        pair.setFieldValue(key + "_" + "event_lvl2_value", txt);
        pair.setFieldValue(key + "_" + "starttime", txt);
        pair.setFieldValue(key + "_" + "loadtime", txt);
        pair.setFieldValue(key + "_" + "pit_type", txt);
        pair.setFieldValue(key + "_" + "pit_value", txt);
        pair.setFieldValue(key + "_" + "pit_no", txt);
        pair.setFieldValue(key + "_" + "sortdate", txt);
        pair.setFieldValue(key + "_" + "sorthour", txt);
        pair.setFieldValue(key + "_" + "lplid", txt);
        pair.setFieldValue(key + "_" + "ptplid", txt);
        pair.setFieldValue(key + "_" + "gid", txt);
        pair.setFieldValue(key + "_" + "testid", txt);
        pair.setFieldValue(key + "_" + "selectid", txt);
        pair.setFieldValue(key + "_" + "ug_id", txt);
        pair.setFieldValue(key + "_" + "rule_id", txt);
        pair.setFieldValue(key + "_" + "x_page_value", txt);
        pair.setFieldValue(key + "_" + "ref_x_page_value", txt);
        pair.setFieldValue(key + "_" + "scene_value", intw);
    }

    /**
     * 对层级赋值
     * @param key
     * @param lvls
     */
    protected void setLevl(String key, String[] lvls) {
        pair.setFieldValue(key + "_" + "page_id", new IntWritable(Integer.valueOf(lvls[0])));
        pair.setFieldValue(key + "_" + "page_value", new Text(lvls[1]));
        pair.setFieldValue(key + "_" + "page_lvl2_value", new Text(lvls[2]));
        pair.setFieldValue(key + "_" + "event_id", new IntWritable(Integer.valueOf(lvls[3])));
        pair.setFieldValue(key + "_" + "event_value", new Text(lvls[4]));
        pair.setFieldValue(key + "_" + "event_lvl2_value", new Text(lvls[5]));
        pair.setFieldValue(key + "_" + "starttime", new Text(lvls[6]));
        pair.setFieldValue(key + "_" + "loadtime", new Text(lvls[7]));
        pair.setFieldValue(key + "_" + "pit_type", new Text(lvls[8]));
        pair.setFieldValue(key + "_" + "pit_value", new Text(lvls[9]));
        pair.setFieldValue(key + "_" + "pit_no", new Text(lvls[10]));
        pair.setFieldValue(key + "_" + "sortdate", new Text(lvls[11]));
        pair.setFieldValue(key + "_" + "sorthour", new Text(lvls[12]));
        pair.setFieldValue(key + "_" + "lplid", new Text(lvls[13]));
        pair.setFieldValue(key + "_" + "ptplid", new Text(lvls[14]));
        pair.setFieldValue(key + "_" + "gid", new Text(lvls[15]));
        pair.setFieldValue(key + "_" + "testid", new Text(lvls[16]));
        pair.setFieldValue(key + "_" + "selectid", new Text(lvls[17]));
        pair.setFieldValue(key + "_" + "ug_id", new Text(lvls[18]));
        pair.setFieldValue(key + "_" + "rule_id", new Text(lvls[19]));
        pair.setFieldValue(key + "_" + "x_page_value", new Text(lvls[20]));
        pair.setFieldValue(key + "_" + "ref_x_page_value", new Text(lvls[21]));
        pair.setFieldValue(key + "_" + "scene_value", new IntWritable(Integer.valueOf(lvls[30])));
    }
}