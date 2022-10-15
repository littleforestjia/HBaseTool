package com.jd.hbase.perCellTTLTest;

import org.apache.hadoop.hbase.*;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.io.ImmutableBytesWritable;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;

public class BulkloadTTLTestCellMapper extends Mapper<LongWritable, Text, ImmutableBytesWritable, Cell> {
    public void map(LongWritable key, Text value, Mapper.Context context) throws IOException, InterruptedException {
        String values = value.toString();
        String[] lines = values.split(",");

        byte[] rowkey = generateValue(lines[0]);
        byte[] columnFamily = "d".getBytes();
        byte[] qualifierName = "1".getBytes();
        byte[] qualifierSex = "2".getBytes();
        byte[] qualifierWork = "3".getBytes();

        Cell cell1 = new KeyValue(rowkey, columnFamily, qualifierName, HConstants.LATEST_TIMESTAMP, generateValue(lines[2]),new Tag[]{new Tag(TagType.TTL_TAG_TYPE, Bytes.toBytes(6000L))});
        Cell cell2 = new KeyValue(rowkey, columnFamily, qualifierSex, HConstants.LATEST_TIMESTAMP, generateValue(lines[3]),new Tag[]{new Tag(TagType.TTL_TAG_TYPE, Bytes.toBytes(16000L))});
        Cell cell3 = new KeyValue(rowkey, columnFamily, qualifierWork, HConstants.LATEST_TIMESTAMP, generateValue(lines[4]),new Tag[]{new Tag(TagType.TTL_TAG_TYPE, Bytes.toBytes(26000L))});

        context.write(new ImmutableBytesWritable(), cell1);
        context.write(new ImmutableBytesWritable(), cell2);
        context.write(new ImmutableBytesWritable(), cell3);
    }

    //将value字符串转化为byte[]。
    private byte[] generateValue(String value) {
        return Bytes.toBytes(value);
    }
}
