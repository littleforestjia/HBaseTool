package com.jd.hbase.logtest;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class LogPlay {
    protected final static Logger logger = LoggerFactory.getLogger(LogPlay.class);

    public void printLog() {
        logger.info("我爱你！中国。");
    }

    public static void main(String[] args) {
        logger.info("我爱你！中国。");
    }
}
