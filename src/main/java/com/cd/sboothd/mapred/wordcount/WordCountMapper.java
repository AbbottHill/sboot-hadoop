package com.cd.sboothd.mapred.wordcount;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;

public class WordCountMapper extends Mapper<LongWritable, Text, Text, LongWritable> {
    // 偏移量、读取到的文本、处理之后的每个单词、单词出现的次数(输出一次为1)
    @Override
    protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
        String line = value.toString();
        String[] words = line.split(" "); // 切分数据
        for (String word : words) {
            context.write(new Text(word), new LongWritable(1));
        }
    }
}
    