package com.cd.sboothd.mapred.wordcount;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;

public class WordCountReducer extends Reducer<Text, LongWritable, Text, LongWritable> {
    @Override
    protected void reduce(Text key, Iterable<LongWritable> values, Reducer<Text, LongWritable, Text, LongWritable>.Context context) throws IOException,
            InterruptedException {
        //统计工作
        long count = 0;
        //遍历values，累加到计数器中
        for (LongWritable v : values) {
            count+=v.get();
        }
        //输出一个单词key及其总次数
        context.write(new Text(key), new LongWritable(count));
    }
}
    