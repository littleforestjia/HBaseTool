package com.jd.hbase.client;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.Connection;
import org.apache.hadoop.hbase.client.ConnectionFactory;
import org.apache.hadoop.hbase.client.Table;

import java.io.IOException;

public class GetData {
    public static void main(String[] args) throws IOException {
        Configuration hbconf = HBaseConfiguration.create();
        hbconf.set("hbase.zookeeper.property.clientPort", "2182");
        Connection connection = ConnectionFactory.createConnection(hbconf);
        Table table = connection.getTable(TableName.valueOf("fellowjava"));
    }
}
