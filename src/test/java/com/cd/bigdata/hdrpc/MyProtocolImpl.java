package com.cd.bigdata.hdrpc;

//import lombok.extern.log4j.Log4j2;

//@Log4j2
public class MyProtocolImpl implements MyProtocol{

    @Override
    public String receiceMsg(String msg) {
        System.out.println("service receive :" + msg);
        return "receive: " + msg;
    }

}
    