package com.cd.rpc;

/**
 * RPC protocal
 * interface client service 端都存在
 */
public interface MyProtocol {
    long versionID = 1L;

    String receiceMsg(String msg);
}