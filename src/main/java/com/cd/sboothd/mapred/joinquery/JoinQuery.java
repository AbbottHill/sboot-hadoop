package com.cd.sboothd.mapred.joinquery;

import org.apache.commons.lang.StringUtils;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.InputSplit;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.lib.input.FileSplit;

import java.io.IOException;

public class JoinQuery {

    public static class JoinQueryMapper extends Mapper<LongWritable, Text, Text, Text> {
        Text outkey = new Text();
        Text outValue = new Text();
        @Override
        protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
            String line = value.toString();
            String[] fields = StringUtils.split(line, ' ');
            FileSplit inputSplit = (FileSplit) context.getInputSplit();
            String name = inputSplit.getPath().getName();
            outkey.set(fields[0]);
            outValue.set(name + "-->" + fields[0]);
            context.write(outkey, outValue);
        }
    }

}
    