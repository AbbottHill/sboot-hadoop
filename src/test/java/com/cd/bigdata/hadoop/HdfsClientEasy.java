package com.cd.bigdata.hadoop;

//import lombok.extern.log4j.Log4j2;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.*;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import java.io.IOException;
import java.net.URI;
import java.net.URISyntaxException;

/**
 * Created by cd on 2018/5/13.
 */
//@Log4j2
public class HdfsClientEasy {
//    private static String dfs = "hdfs://hdsm1:9000/";
    private static String dfs = "hdfs://ns1";

    FileSystem fs = null;

    @Before
    public void initFs() throws URISyntaxException, IOException, InterruptedException {
//        log.info("-------- before ------------");
//         get a configuration object
        Configuration conf = new Configuration();
        // to set a paramter, figure out the filesystem is hdfs
        conf.set("fs.defaultFS", dfs);
//        conf.set("dfs.replication", "1");

        // get a instance of HDFS filesystem
//         fs = FileSystem.get(conf);

        // 指定人员
        fs = FileSystem.get(new URI(dfs), conf, "hd");
    }

    @After
    public void afterTest() {
//        log.debug("------------- after test ----------");
    }

    @Test
    public void testUpload() throws IOException {
        fs.copyFromLocalFile(new Path("C:\\hd\\data\\phone-info.txt"), new Path("/wordcount/data/dataphone-info.txt"));
    }

    @Test
    public void testRMfile() throws IOException {
        fs.delete(new Path("/wordcount/data/dataphone-info.txt"), true);// recursive
    }

    @Test
    public void testRename() throws IOException {
        boolean rename = fs.rename(new Path("/wordcount/hello.txt"), new Path("/wordcount/hello.rename.txt"));
    }

    @Test
    public void testDown() throws IOException {
        fs.copyToLocalFile(false, new Path("/wordcount/qingshu.txt"), new Path("C:\\Users\\Public\\Desktop\\qingshu.down.txt"), true);
    }


    @Test
    public void TestListFiles() throws IOException {
        RemoteIterator<LocatedFileStatus> listFiles = fs.listFiles(new Path("/"), true);
        while (listFiles.hasNext()) {
            LocatedFileStatus file = listFiles.next();
            System.out.println(file.getPath().getName());
        }

        System.out.println("--------------  -----");
        FileStatus[] status = fs.listStatus(new Path("/"));
        for (FileStatus file : status) {
            System.out.println((file.isDirectory() ? "d" : "f" + "  " + file.getPath().getName()));
        }
    }

}
