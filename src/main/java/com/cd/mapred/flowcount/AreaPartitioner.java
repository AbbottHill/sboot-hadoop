package com.cd.mapred.flowcount;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Partitioner;

import java.util.HashMap;
import java.util.Map;

public class AreaPartitioner extends Partitioner<Text, FlowBean> {
    private static Map<String, Integer> areaMap;
    static {
        areaMap = new HashMap();
        areaMap.put("182", 0);
        areaMap.put("189", 1);
        areaMap.put("143", 2);
    }

    @Override
    public int getPartition(Text key, FlowBean bean, int numPartitions) {
        Integer provinceCode = areaMap.get(key.toString().substring(0, 3));
        return provinceCode == null ? 4 : provinceCode;
    }
}
    