package com.cd.bigdata.hdrpc;

/**
 * RPC protocal
 */
public interface MyProtocol {
    long versionID = 1L;

    String receiceMsg(String msg);
}