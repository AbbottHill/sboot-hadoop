package com.cd.rpc;

import lombok.extern.log4j.Log4j2;

/**
 * 存在于service 端
 */
@Log4j2
public class MyProtocolImpl implements MyProtocol{

    @Override
    public String receiceMsg(String msg) {
        System.out.println("service receive :" + msg);
        return "receive: " + msg;
    }

}
    