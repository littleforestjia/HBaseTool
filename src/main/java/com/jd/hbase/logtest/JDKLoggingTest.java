package com.jd.hbase.logtest;

import java.util.logging.Logger;

public class JDKLoggingTest {
    private static final Logger logger = Logger.getGlobal();

    public static void main(String[] args) {
        logger.info("start process...");
        logger.warning("memory is running out...");
        logger.fine("ignored.");
        logger.severe("process will be terminated...");
    }
}
