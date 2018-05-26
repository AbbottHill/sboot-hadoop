package com.cd.sboothd.mapred;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

public class WordCountDriver {
    public static void main(String[] args) throws Exception {
        Configuration conf = new Configuration();
        //1:指定为hdfs文件系统
        conf.set("fs.defaultFS", "hdfs://hdsm1:9000");
        //2:指定jar包位置
        conf.set("mapreduce.job.jar", "D:\\IdeaWorkSpace\\web-trunk\\out\\artifacts\\mrjob_jar\\mrjob.jar");

        // 构造一个job对象来封装本mapreduce业务到所有信息
        Job wcjob = Job.getInstance(conf);

        // 指定本job工作用到的jar包位置
        wcjob.setJarByClass(WordCountDriver.class);
        // 指定本job用到的mapper类
        wcjob.setMapperClass(WordCountMapper.class);
        // 指定本job用到的reducer类
        wcjob.setReducerClass(WordCountReducer.class);

        // 指定mapper输出的kv类型
        wcjob.setMapOutputKeyClass(Text.class);
        wcjob.setMapOutputValueClass(LongWritable.class);

        // 指定reducer输出到kv数据类型，（setOutputKeyClass
        // 会对mapper和reducer都起作用,如果上面mapper不设置的话）
        wcjob.setOutputKeyClass(Text.class);
        wcjob.setOutputValueClass(LongWritable.class);

        // 指定程序处理到输入数据所在的路径
        FileInputFormat.setInputPaths(wcjob, new Path("/wordcount/data/"));
        // 指定程序处理到输出结果所在路径
        FileOutputFormat.setOutputPath(wcjob, new Path("/wordcount/output1/"));

        // 将该job通过yarn的客户端进行提交
        wcjob.waitForCompletion(true);
    }
}
    