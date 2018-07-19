package com.cd.mapred.flowcount;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

import java.io.IOException;

public class FlowCountPart {

    public static class FlowCountPartMapper extends Mapper<LongWritable, Text, Text, FlowBean> {
        private FlowBean flowBean = new FlowBean();
        @Override
        public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
//            context.write((KEYOUT) key, (VALUEOUT) value);
            String line = String.valueOf(value);
            String[] values = line.split(" ");
//            FlowBean flowBean = new FlowBean();
            flowBean.set(values[0], Long.parseLong(values[1]));
            context.write(new Text(values[0]), flowBean);
        }
    }

    public static class FlowCountPartReducer extends Reducer<Text, FlowBean, NullWritable, FlowBean>{
        private FlowBean flowBean = new FlowBean();
        @Override
        public void reduce(Text key, Iterable<FlowBean> values, Context context) throws IOException, InterruptedException {
//            context.write((KEYOUT) key, (VALUEOUT) value);
            Long countUp = 0L;
            for(FlowBean value: values) {
                countUp += value.getUpFlow();
            }
            flowBean.set(key.toString(), countUp);
            context.write(NullWritable.get(), flowBean);
        }
    }

    public static void main(String[] args) throws Exception {

        Configuration conf = new Configuration();
        conf.set("fs.defaultFS", "hdfs://hdsm1:9000");
        conf.set("mapreduce.app-submission.cross-platform", "true");

        Job job = Job.getInstance(conf, "flow count");
        job.setMapperClass(FlowCountPartMapper.class);
        job.setReducerClass(FlowCountPartReducer.class);

        job.setMapOutputKeyClass(Text.class);
        job.setMapOutputValueClass(FlowBean.class);

        job.setOutputKeyClass(NullWritable.class);
        job.setOutputValueClass(FlowBean.class);

        /**
         * 设置reduce task的数量，要跟AreaPartitioner返回的partition个数匹配
         * 如果reduce task的数量比partitioner中分组数多，就会产生多余的几个空文件
         * 如果reduce task的数量比partitioner中分组数少，就会发生异常，因为有一些key没有对应reducetask接收
         * (如果reduce task的数量为1，也能正常运行，所有的key都会分给这一个reduce task)
         * reduce task 或 map task 指的是，reuder和mapper在集群中运行的实例
         */
        job.setNumReduceTasks(4);
        job.setPartitionerClass(AreaPartitioner.class);

        Path outPath = new Path("/wordcount/output");
        FileSystem fileSystem = FileSystem.get(conf);
        if (fileSystem.exists(outPath)) {
            fileSystem.delete(outPath, true);
        }
        FileInputFormat.setInputPaths(job, new Path("/wordcount/data"));
        FileOutputFormat.setOutputPath(job, outPath);

        job.waitForCompletion(true);

    }


}
    