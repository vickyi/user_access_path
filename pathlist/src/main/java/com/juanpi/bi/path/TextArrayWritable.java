package com.imeijia.bi.path;

import org.apache.hadoop.io.ArrayWritable;
import org.apache.hadoop.io.Text;

/**
 * Created by vkzhang@yeah.net on 2017/7/13.
 */
public class TextArrayWritable extends ArrayWritable {
    public TextArrayWritable() {
        super(Text.class);
    }

    public TextArrayWritable(String[] strings) {
        super(Text.class);
        Text[] texts = new Text[strings.length];
        for (int i = 0; i < strings.length; i++) {
            texts[i] = new Text(strings[i]);
        }
        set(texts);
    }


}