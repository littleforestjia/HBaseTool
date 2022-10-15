package com.jd.hbase.conf.test;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.io.ImmutableBytesWritable;
import org.apache.hadoop.hbase.mapreduce.TableMapper;

public class GetConfMapper extends TableMapper<ImmutableBytesWritable, Put> {
    private String firstFieldLength;

    public void map(ImmutableBytesWritable row, Result value, Context context) {
        Configuration conf = context.getConfiguration();
        firstFieldLength = conf.get("first.field.length");
    }
}
