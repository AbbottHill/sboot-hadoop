package com.cd.sboothd.mapred.flowcount;

import lombok.ToString;
import org.apache.hadoop.io.Writable;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

public class FlowBean implements Writable {
    private String phoneNbr;
    private Long upFlow;

    @Override
    public void write(DataOutput out) throws IOException {
        out.writeUTF(phoneNbr);
        out.writeLong(upFlow);
    }

    /**
     * 读写的顺序要保持一致
     */
    @Override
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
}
