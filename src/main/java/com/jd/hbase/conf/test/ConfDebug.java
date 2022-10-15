package com.jd.hbase.conf.test;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseConfiguration;

import java.net.URL;

public class ConfDebug {
    public static void main(String[] args) {
        Configuration hbconf = HBaseConfiguration.create();
        Configuration conf = new Configuration();
        //conf.set("jia", "zheng");
        boolean testConf = conf.getBoolean("hbase.regionserver.codecs.bzip2.compatible", false);
        System.out.println("压缩格式：" + testConf);

        System.out.println("!");

        URL classpath1 = ConfDebug.class.getResource("");
        System.out.println(classpath1);

        URL classpath2 = ConfDebug.class.getClassLoader().getResource("");
        System.out.println(classpath2);
        URL is = ConfDebug.class.getClassLoader().getResource("core-site.xml");
        System.out.println(is);
        URL is1 = ConfDebug.class.getClassLoader().getResource("core-default.xml");
        System.out.println(is1);
    }
}
