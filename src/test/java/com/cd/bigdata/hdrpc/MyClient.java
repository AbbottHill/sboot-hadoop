package com.cd.bigdata.hdrpc;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.ipc.RPC;

import java.io.IOException;
import java.net.InetSocketAddress;

public class MyClient {

    public static void main(String[] args) throws IOException {
        MyProtocol localhost = RPC.getProxy(MyProtocol.class, 1L,
                new InetSocketAddress("cdpc", 8888), new Configuration());
        System.out.println(localhost.receiceMsg("hello"));
    }
}
