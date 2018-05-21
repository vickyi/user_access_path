package com.imeijia.bi.test;

import com.alibaba.fastjson.JSON;
import com.alibaba.fastjson.JSONObject;
import com.imeijia.bi.path.DecodeURLParam;

import java.util.Arrays;
import java.util.List;

/**
 * Created by vkzhang@yeah.net on 2017/5/10.
 */
public class Test {

    public static void testStr() {
        String str = "00000000-0b52-2be9-ffff-ffff87a6e340\u000143421560\u0001101218\u0001\\N\u00011481658101979_jiu_1492665635847\u00012\u00014.2.1\u00012\u00012\u0001C2\u0001四川省德阳市广汉市大件路靠近广汉市骨科医院-门诊大厅\u00010\u0001668_669_684_487_17_486_450_667_728_457_703_627_572_517_578_629_602_639_499_631_478_496_630_493_736_738_612_520_546_714_711\u00012017-05-07\u000118\u0001289\u0001\\N\u0001-1\u0001\\N\u00010\u00010\u00010\u00011494152427593\u00011494074212349\u0001\\N\u0001\\N\u0001\\N\u00010\u0001\\N\u0001\\N\u0001\\N\u0001\\N\u0001C2\u0001mb_page\u0001未知\u0001119.4.175.242\u0001\\N\u0001\\N\u0001861708030114223\u00011\u0001\\N\u0001\\N\u0001\\N\u0001\\N\u0001\\N\u0001\\N\u0001\\N\u0001\\N\u00010\u00010";
        System.out.println(str.replaceAll("\\\\N", "0"));
        // replaceAll("\\\\N", "0")
        String[] splited = str.toString().split("\001");
        String timeStr = (splited[22] == null) ? "\\N" : splited[22];

        System.out.println("timeStr:" + timeStr);
        System.out.println(splited[3]);
        if(splited[3] == null) {
            System.out.println("---------");
        }

        if(splited[3] == "\\N") {
            System.out.println("11111111");
        }

        for(String s : splited) {
            System.out.println(s);
        }
        List<String> ids = Arrays.asList("349","350","351","352","433","479","480","481","482");
        String event_id = "481";
        String event_value = "goods::49887223::1_6";
        String pit_type = "";
        String pit_value = "";
        String pit_no = "0";
        if(ids.contains(event_id)) {
            if(event_value.contains("::")){
                pit_type = event_value.split("::")[0];
                if("goods".equals(pit_type)|| "brand".equals(pit_type)){
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

        System.out.println(pit_no);
        System.out.println(pit_type);
        System.out.println(pit_value);

    }

    public static void main(String[] args) {

        String event_value = "%7B%22pit_info%22:%22brand::1000571::10_5%22,%22cid%22:-1,%22_t%22:1499748929,%22_gsort_key%22:%22ZXG_SORT_36_20170711_12_16_13%7C3659645911c81ed%22,%22_pit_type%22:8,%22_z%22:%2218_51_5%22,%22sub_md%22:%2242839725::5%22,%22hot_goods_id%22:%2242839725%22%7D";
        if(event_value.contains("%7B%22") && event_value.contains("%22%7D")){
            String valueDecode = DecodeURLParam.decode(event_value);
            JSONObject obj = JSON.parseObject(valueDecode);
            if(obj.containsKey("pit_info")) {
                event_value = obj.getString("pit_info");
            }
        }
        System.out.println(event_value);
    }
}
