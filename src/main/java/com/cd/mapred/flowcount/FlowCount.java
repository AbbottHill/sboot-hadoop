package com.cd.mapred.flowcount;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

import java.io.IOException;

public class FlowCount {

    public static class FlowCountMapper extends Mapper<LongWritable, Text, Text, FlowBean> {
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

    public static class FlowCountReducer extends Reducer<Text, FlowBean, Text, FlowBean>{
        private FlowBean flowBean = new FlowBean();
        @Override
        public void reduce(Text key, Iterable<FlowBean> values, Context context) throws IOException, InterruptedException {
//            context.write((KEYOUT) key, (VALUEOUT) value);
            Long countUp = 0L;
            for(FlowBean value: values) {
                countUp += value.getUpFlow();
            }
            flowBean.setUpFlow(countUp);
            context.write(key, flowBean);
        }
    }

    public static void main(String[] args) throws Exception {

        Configuration conf = new Configuration();
        conf.set("fs.defaultFS", "hdfs://ns1");
        conf.set("mapreduce.app-submission.cross-platform", "true");

        Job job = Job.getInstance(conf, "flow count");
        job.setMapperClass(FlowCountMapper.class);
        job.setReducerClass(FlowCountReducer.class);

        job.setMapOutputKeyClass(Text.class);
        job.setMapOutputValueClass(FlowBean.class);

        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(FlowBean.class);

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
    