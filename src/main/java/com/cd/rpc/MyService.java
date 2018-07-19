package com.cd.rpc;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.ipc.RPC;
import org.apache.hadoop.ipc.RPC.Builder;

import java.io.IOException;

/**
 * RPC service
 */
public class MyService {

    public static void main(String[] args) throws IOException {
        Builder builder = new Builder(new Configuration()).setBindAddress("cdpc").setPort(8888).
                setProtocol(MyProtocol.class).setInstance(new MyProtocolImpl());
        RPC.Server build = builder.build();
        build.start();
    }
}
    