package com.jd.hbase.perCellTTLTest;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.*;
import org.apache.hadoop.hbase.client.*;
import org.apache.hadoop.hbase.util.Bytes;

import java.io.IOException;

public class PutTTLTest {
    public static void main(String[] args) throws IOException {
        //这里创建HBase连接的目的是将数据放到目标表当中。
        Configuration conf = HBaseConfiguration.create();
        conf.set("hbase.zookeeper.property.clientPort", "2182");
        conf.set("hbase.client.rpc.codec", "org.apache.hadoop.hbase.codec.CellCodecWithTags");
        Connection conn = ConnectionFactory.createConnection(conf);
        Table table = conn.getTable(TableName.valueOf("fellowjava"));

        Put put1 = new Put(Bytes.toBytes("haha")).addColumn(Bytes.toBytes("d"), Bytes.toBytes("feild"), Bytes.toBytes("haha"));
        Put put2 = new Put(Bytes.toBytes("xixi")).addColumn(Bytes.toBytes("d"), Bytes.toBytes("feild"), Bytes.toBytes("xixi"));

        put1.setTTL(600000L);
        put2.setTTL(6000000L);

        byte[] bytes = Bytes.toBytes("d");
        Put put3 = new Put(Bytes.toBytes("hehe")).add(new KeyValue(Bytes.toBytes("hehe"), bytes, Bytes.toBytes("feild"), HConstants.LATEST_TIMESTAMP, bytes,new Tag[]{new Tag(TagType.TTL_TAG_TYPE, Bytes.toBytes(6000000L))}));

        table.put(put3);
        table.put(put1);
        table.put(put2);
    }
}
