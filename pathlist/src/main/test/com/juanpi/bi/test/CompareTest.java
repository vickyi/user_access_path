package com.imeijia.bi.test;

/**
 * Created by vkzhang@yeah.net on 2017/7/11.
 */
public class CompareTest {
    public static void main(String[] args) {
        CompareTest ct = new CompareTest();
        System.out.println(">>>>>>>>>>>>>>>> FiveLevlsPathListORC Finish! <<<<<<<<<<<<<<<<");
        String gu1 = "abc";
        String gu2 = "bcd";
        long minus = gu1.compareTo(gu2);
        System.out.println(minus);
        if (minus != 0) {
            System.out.println((int) minus);
        }
        Long second = 12321321321323L;
        Long second2 = 22321321321323L;
        System.out.println(second.compareTo(second2));

    }
}
