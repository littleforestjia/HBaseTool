package com.jd.hbase.perCellTTLTest;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseConfiguration;

public class SeeConfParameterTest {
    public static void main(String[] args) {
        Configuration conf = HBaseConfiguration.create();
        String string1 = conf.get("hbase.client.rpc.codec");
        System.out.println(string1);

        String hfileVesion = conf.get("hfile.format.version");
        System.out.println(hfileVesion);
    }
}
