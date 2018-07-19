package com.cd.client;

import org.apache.commons.io.IOUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.junit.Before;
import org.junit.Test;

import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.io.IOException;
import java.net.URI;
import java.net.URISyntaxException;

/**
 * Created by DONG on 2018/5/13.
 */
public class HdfsClient {

    // HdfsClient

    static FileSystem fs = null;

    @Before
    public void getFS() throws URISyntaxException, IOException, InterruptedException {

        // get a configuration object
        Configuration conf = new Configuration();
        // to set a paramter, figure out the filesystem is hdfs
        conf.set("fs.defaultFS", "hdfds://hdsm-00:9000/");
        conf.set("dfs.replication", "1");

        // get a instance of HDFS filesystem
        // fs = FileSystem.get(conf);

        // 指定人员
        fs = FileSystem.get(new URI("hdfs://"), conf, "hadoop");
    }

    // download
    @Test
    public void testDownload() throws IOException {
        FSDataInputStream is = fs.open(new Path("hdfs://hdsm-00:9000/jdk.tgz"));
        FileOutputStream os = new FileOutputStream("/home/hadoop/jdk.download");
        IOUtils.copy(is, os);
    }

    // upload a local file to hdfs
    public static void main(String args[]) throws IOException {
        // open ouputStream of hdfs
        Path desfile = new Path("hdfs://hdsm-00:999/test.tgz");
        FSDataOutputStream os = fs.create(desfile);

        FileInputStream is = new FileInputStream("/home/haddoop/jdk...tar.gz");

        // write
        IOUtils.copy(is, os);

    }
}
