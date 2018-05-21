package com.imeijia.bi.path;

import org.apache.hadoop.io.WritableComparable;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

/**
 * Created by vkzhang@yeah.net on 2017/7/13.
 */
public class GuIdDatePair implements WritableComparable<GuIdDatePair> {
    String guId;
    Long startTime;

    public GuIdDatePair() {}

    public GuIdDatePair(String guId, long startTime) {
        this.guId = guId;
        this.startTime = startTime;
    }

    @Override
    public void readFields(DataInput in) throws IOException {
        this.guId = in.readUTF();
        this.startTime = in.readLong();
    }

    @Override
    public void write(DataOutput out) throws IOException {
        out.writeUTF(guId);
        out.writeLong(startTime);
    }

    /**
     * 如果没有通过job.setSortComparatorClass设置key比较函数类，则使用此类中实现的compareTo方法
     * 当第一列不同时，升序；当第一列相同时，第二列升序
     */
    @Override
    public int compareTo(GuIdDatePair o) {
        // 相同的时候返回0
        // 如果字典序排序在前的，返回结果<0;否则>0
        int compareValue = this.guId.compareTo(o.getGuId());
        if (compareValue == 0) {
            compareValue = this.startTime.compareTo(o.getStartTime());
        }
        return compareValue;
    }

    @Override
    public int hashCode() {
        // guId 或者 startTime可能为null，则不需要判断是否为hull的情况
        int codeGuid = guId != null ? guId.hashCode() : 0;
        int codeTime = startTime != null ? startTime.hashCode() : 0;

        return codeGuid + codeTime;
        // guId 或者 startTime为null时，都已经转为空串了因此不需要判断是否为hull的情况
        // return this.guId.hashCode() + this.startTime.hashCode();
    }

    @Override
    public boolean equals(Object obj) {
        if(this == obj) {
            return true;
        }

        if(obj == null || getClass() != obj.getClass()) {
            return false;
        }

        GuIdDatePair that = (GuIdDatePair) obj;
        if(guId != null ? !guId.equals(that.guId) : that.guId != null){
            return false;
        }
        if(startTime != null ? !startTime.equals(that.startTime) : that.startTime != null){
            return false;
        }

       return true;
    }

    public String getGuId() {
        return guId;
    }

    public void setGuId(String guId) {
        this.guId = guId;
    }

    public Long getStartTime() {
        return startTime;
    }

    public void setStartTime(Long startTime) {
        this.startTime = startTime;
    }
}