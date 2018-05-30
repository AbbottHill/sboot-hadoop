package com.cd.sboothd.mapred.flowcount;

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

/**
 * sort
 * 1. job.setMapOutputKeyClass(FlowBean.class);
 *
 */
public class FlowCountSort {

    public static class FlowCountSortMapper extends Mapper<LongWritable, Text, FlowBean, NullWritable> {
        private FlowBean flowBean = new FlowBean();
        @Override
        public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
//            context.write((KEYOUT) key, (VALUEOUT) value);
            String line = String.valueOf(value);
            String[] values = line.split(" ");
//            FlowBean flowBean = new FlowBean();
            flowBean.set(values[0], Long.parseLong(values[1]));
            context.write(flowBean, NullWritable.get());
        }
    }

    public static class FlowCountSortReducer extends Reducer<FlowBean, NullWritable, FlowBean, NullWritable>{
        private FlowBean flowBean = new FlowBean();
        @Override
        public void reduce(FlowBean bean, Iterable<NullWritable> values, Context context) throws IOException, InterruptedException {
//            context.write((KEYOUT) key, (VALUEOUT) value);
            context.write(bean, NullWritable.get());
        }
    }

    public static void main(String[] args) throws Exception {

        Configuration conf = new Configuration();
        conf.set("fs.defaultFS", "hdfs://hdsm1:9000");
        conf.set("mapreduce.app-submission.cross-platform", "true");

        Job job = Job.getInstance(conf, "flow sort");
        job.setMapperClass(FlowCountSortMapper.class);
        job.setReducerClass(FlowCountSortReducer.class);

        job.setMapOutputKeyClass(FlowBean.class);
        job.setMapOutputValueClass(NullWritable.class);

        job.setOutputKeyClass(FlowBean.class);
        job.setOutputValueClass(NullWritable.class);

//        job.setInputFormatClass(TextInputFormat.class);
//        job.setOutputFormatClass(TextOutputFormat.class);

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
    