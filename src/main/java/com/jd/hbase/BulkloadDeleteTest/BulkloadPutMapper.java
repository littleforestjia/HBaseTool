package com.jd.hbase.BulkloadDeleteTest;

import org.apache.hadoop.hbase.*;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.io.ImmutableBytesWritable;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;

public class BulkloadPutMapper extends Mapper<LongWritable, Text, ImmutableBytesWritable, Put> {
    public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
        String values = value.toString();
        String[] lines = values.split(",");

        byte[] rowkey = generateValue(lines[0]);
        byte[] columnFamily = "d".getBytes();
        byte[] qualifierName = "b".getBytes();
        byte[] qualifierSex = "c".getBytes();
        byte[] qualifierWork = "d".getBytes();

        ImmutableBytesWritable putRowKey = new ImmutableBytesWritable(rowkey);
        Put put = new Put(rowkey);

        Cell cell1 = new KeyValue(rowkey, columnFamily, qualifierName, HConstants.LATEST_TIMESTAMP, generateValue(lines[2]));
        put.add(cell1);
        Cell cell2 = new KeyValue(rowkey, columnFamily, qualifierSex, HConstants.LATEST_TIMESTAMP, generateValue(lines[3]));
        put.add(cell2);
        Cell cell3 = new KeyValue(rowkey, columnFamily, qualifierWork, HConstants.LATEST_TIMESTAMP, generateValue(lines[4]));
        put.add(cell3);

        context.write(putRowKey, put);
    }

    //将value字符串转化为byte[]。
    private byte[] generateValue(String value) {
        return Bytes.toBytes(value);
    }
}
