package com.imeijia.bi.path;

import com.alibaba.fastjson.JSON;
import com.alibaba.fastjson.JSONObject;
import com.imeijia.bi.utils.LogPrinter;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;
import java.util.Arrays;
import java.util.List;

/**
 * Mapper 是所有用户定制 Mapper 类的基类，有setup，reduce，cleanup和run方法
 * LongWritable: key in
 * Text:value in
 * GuIdDatePair: key out
 * TextArrayWritable: value out
 * Created by vkzhang@yeah.net on 2017/7/13.
 */
public class PathMapper extends Mapper<LongWritable, Text, GuIdDatePair, TextArrayWritable> {
    int xx = 0;

    List<String> eventIds = Arrays.asList("90","91","92","93","94","95","349","350","351","352","433","479","480","481","482","635","10002","10034","10045","10049");

    protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException, ArrayIndexOutOfBoundsException, NumberFormatException {

        String val = value.toString().replaceAll("\\\\N", "0").replaceAll("#", "");
        final String[] splited = val.split("\001");

        try {
            // gu_id 和 starttime_origin 作为联合主
            String gu_id = splited[1];
            if (!gu_id.isEmpty() && !gu_id.equals("0")) {
                final GuIdDatePair pair = new GuIdDatePair(splited[1], Long.parseLong(splited[8]));

                String pageLevelId = splited[0];
                String page_id = splited[2];
                String page_value = splited[3];
                String page_lvl2_value = splited[4];
                String event_id = splited[5];
                String event_value = splited[6];
//                if(pageLevelId.equals("1")) {
//                    if( event_id.equals("95") ||  event_id.equals("91")) {
//                        System.out.println("val=" + val);
//                        System.out.println("pageLevelId=" +pageLevelId + ",,,event_id=" + event_id + ",,,event_value=" + event_value);
//                    }
//                }
                String event_lvl2_value = splited[7];
                String starttime = splited[8];
                String loadtime = splited[9];
                // 从event表出来得pit_type是不完整的，此处自己解析
                String pit_type = "0";
                String pit_value = "0";
                String pit_no = "0";
                String sortdate = splited[11];
                String sorthour = splited[12];
                String lplid = splited[13];
                String ptplid = splited[14];
                String gid = splited[15];
                String test_id = splited[16];
                String select_id = splited[17];
                String ug_id = splited[18];
                String rule_id = splited[19];
                String x_page_value = splited[20];
                String ref_x_page_value = splited[21];

                String session_id = splited[22];
                String user_id = splited[23];
                String terminal_id = splited[24];
                String site_id = splited[25];
                String ugroup = splited[26];
                String utm_id = splited[27];
                String app_version = splited[28];
                String scene_value = splited[29];
                String openid = splited[30];

                if(eventIds.contains(event_id)) {

                    //
                    if(event_value.contains("%7B%22") && event_value.contains("%22%7D")){
                        String valueDecode = DecodeURLParam.decode(event_value);
                        JSONObject obj = JSON.parseObject(valueDecode);
                        if(obj.containsKey("pit_info")) {
                            event_value = obj.getString("pit_info");
                        }
                    }

                    if(event_value.contains("::")){
                        pit_type = event_value.split("::")[0];
                        if("goods".equals(pit_type)
                                || "brand".equals(pit_type)
                                || "ad_small_goods".equals(pit_type)
                                || "ad_big_goods".equals(pit_type)
                                || "ad_long_goods".equals(pit_type)
                                || "shop".equals(pit_type)){
                            pit_value = event_value.split("::")[1];
                        }

                        int len = event_value.split("::").length;
                        String pitInfo = event_value.split("::")[len-1];

                        if(pitInfo.contains("_")){
                            int i = Integer.parseInt(pitInfo.split("_")[1]);
                            int j = Integer.parseInt(pitInfo.split("_")[0]);
                            pit_no = String.valueOf(i + (j-1)*20);
                        }
                    }
                }

                // 推荐点击为入口页(购物袋页、品牌页、商祥页底部)
                String pageLvlId = pageLevelId;

                String str[] = {
                        pageLvlId,
                        page_id
                                + "#" + page_value
                                + "#" + page_lvl2_value
                                + "#" + event_id
                                + "#" + event_value
                                + "#" + event_lvl2_value
                                + "#" + starttime
                                + "#" + loadtime
                                + "#" + pit_type
                                + "#" + pit_value
                                + "#" + pit_no
                                + "#" + sortdate
                                + "#" + sorthour
                                + "#" + lplid
                                + "#" + ptplid
                                + "#" + gid
                                + "#" + test_id
                                + "#" + select_id
                                + "#" + ug_id
                                + "#" + rule_id
                                + "#" + x_page_value
                                + "#" + ref_x_page_value
                                + "#" + session_id
                                + "#" + user_id
                                + "#" + terminal_id
                                + "#" + site_id
                                + "#" + ugroup
                                + "#" + utm_id
                                + "#" + gu_id
                                + "#" + app_version
                                + "#" + scene_value
                                + "#" + openid,

                        value.toString().replace("\001", "#")
                };

                final TextArrayWritable theGuIdText = new TextArrayWritable(str);

                xx++;

                context.write(pair, theGuIdText);
            }
        } catch (Exception e) {
            System.out.println("======>>Map Exception: " + e.getClass() + "\n==>" + LogPrinter.printStackTraceStr(e) + "\ndata_length=" + splited.length + "====>data=" + value.toString());
        }
    }
}