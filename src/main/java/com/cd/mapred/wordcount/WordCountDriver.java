package com.cd.mapred.wordcount;

import lombok.extern.log4j.Log4j2;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

import java.net.URI;

@Log4j2
public class WordCountDriver {
    private static String dfs = "hdfs://ns1";

    public static void main(String[] args) throws Exception {
        System.setProperty("HADOOP_USER_NAME", "hd ");

        Configuration conf = new Configuration();
        //1:指定为hdfs文件系统
        conf.set("fs.defaultFS", "hdfs://ns1");

        //2:指定jar包位置, 仅linux有效
        conf.set("mapreduce.job.jar", "D:\\Idea_Workspace\\sboot-hd\\sboot-hd.jar");
        conf.set("mapreduce.app-submission.cross-platform", "true");

        // 构造一个job对象来封装本mapreduce业务到所有信息
        Job wcjob = Job.getInstance(conf, "my Word Count");

        // 指定本job工作用到的jar包位置
        wcjob.setJarByClass(WordCountDriver.class);
        // 指定本job用到的mapper类
        wcjob.setMapperClass(WordCountMapper.class);
        // 指定本job用到的reducer类
        wcjob.setReducerClass(WordCountReducer.class);

        // 指定本job用到的Combiner类
        wcjob.setCombinerClass(WordCountReducer.class);

        // 指定mapper输出的kv类型
        wcjob.setMapOutputKeyClass(Text.class);
        wcjob.setMapOutputValueClass(LongWritable.class);

        // 指定reducer输出到kv数据类型，（setOutputKeyClass
        // 会对mapper和reducer都起作用,如果上面mapper不设置的话）
        wcjob.setOutputKeyClass(Text.class);
        wcjob.setOutputValueClass(LongWritable.class);

        // 指定程序处理到输入数据所在的路径
        FileInputFormat.setInputPaths(wcjob, new Path("/data/wordcount"));

        FileSystem fileSystem = FileSystem.get(new URI(dfs), conf, "hd");;
        Path path = new Path("/result/wordcount1");
        if (fileSystem.exists(path)) {
            fileSystem.delete(path, true);
        }

        // 指定程序处理到输出结果所在路径
        FileOutputFormat.setOutputPath(wcjob, path);
        // 将该job通过yarn的客户端进行提交
        wcjob.waitForCompletion(true);
    }
}
    