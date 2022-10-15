package com.jd.hbase.logtest;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

public class CommonsLoggingTest {
    private static final Log log = LogFactory.getLog(CommonsLoggingTest.class);

    public static void main(String[] args) {
        log.info("start...");
        log.warn("end...");
    }
}
