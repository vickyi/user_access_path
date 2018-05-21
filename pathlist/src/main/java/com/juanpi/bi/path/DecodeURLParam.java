package com.imeijia.bi.path;

import java.net.URLDecoder;

/**
 * Created by vkzhang@yeah.net on 2017/8/16.
 */
public class DecodeURLParam {

    public static String decode(String str){
        String res = recursiveDecode(str.trim());
        return res.trim();
    }

    public static String recursiveDecode(String code) {
        String temp = null;
        String res = code;
        try {
            do {
                temp = res;
                res = URLDecoder.decode(temp, "UTF-8");
            } while (!temp.equals(res));
        } catch (Exception ex) {
        }
        return res;
    }

}
