package com.imeijia.bi.utils;

import java.io.PrintWriter;
import java.io.StringWriter;

/**
 * Created by vkzhang@yeah.net on 2017/7/13.
 */
public class LogPrinter {
    public static String printStackTraceStr(Exception e) {
        StringWriter sw = new StringWriter();
        PrintWriter pw = new PrintWriter(sw);
        e.printStackTrace(pw);
        return sw.toString();
    }

}
