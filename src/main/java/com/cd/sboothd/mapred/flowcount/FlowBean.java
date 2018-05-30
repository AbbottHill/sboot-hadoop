package com.cd.sboothd.mapred.flowcount;

import org.apache.hadoop.io.WritableComparable;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

public class FlowBean implements WritableComparable<FlowBean> {
    private String phoneNbr;
    private Long upFlow;

    public void write(DataOutput out) throws IOException {
        out.writeUTF(phoneNbr);
        out.writeLong(upFlow);
    }

    /**
     * 读写的顺序要保持一致
     */
    public void readFields(DataInput in) throws IOException {
        phoneNbr = in.readUTF();
        upFlow = in.readLong();
    }

    public void set(String phoneNbr, Long upFlow){
        this.phoneNbr = phoneNbr;
        this.upFlow = upFlow;
    }

    @Override
    public String toString() {
        return phoneNbr + " upFlow: " + upFlow;
    }

    public String getPhoneNbr() {
        return phoneNbr;
    }

    public void setPhoneNbr(String phoneNbr) {
        this.phoneNbr = phoneNbr;
    }

    public Long getUpFlow() {
        return upFlow;
    }

    public void setUpFlow(Long upFlow) {
        this.upFlow = upFlow;
    }

    public int compareTo(FlowBean o) {
        return (upFlow > o.getUpFlow()? -1: 1);
    }

}
