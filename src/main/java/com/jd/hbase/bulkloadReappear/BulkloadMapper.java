package com.jd.hbase.bulkloadReappear;

import org.apache.commons.lang.StringUtils;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.io.ImmutableBytesWritable;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;

public class BulkloadMapper extends Mapper<LongWritable, Text, ImmutableBytesWritable, Put> {

    //重要：Mapper当中map()方法的Text value输入参数默认是Hfile源文件当中的一行数据，也就是Hbase表中的一行。
    //value的结构为id+birthday+name+sex+work，设计id+birthday组成rowkey，name、sex、work作为列族D的列。
    public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
        String values = value.toString();
        String[] lines = values.split(",");

        byte[] rowkey = generateRowkey(lines[0], lines[1]);
        byte[] columnFamily = "d".getBytes();
        byte[] qualifierName = "name".getBytes();
        byte[] qualifierSex = "sex".getBytes();
        byte[] qualifierWork = "Work".getBytes();

        ImmutableBytesWritable putRowKey = new ImmutableBytesWritable(rowkey);
        Put put = new Put(rowkey);
        put.add(columnFamily, qualifierName, generateValue(lines[2]));
        put.add(columnFamily, qualifierSex, generateValue(lines[3]));
        put.add(columnFamily, qualifierWork, generateValue(lines[4]));
        context.write(putRowKey, put);
    }

    //将id和birthday转化为byte[]，并将id进行reverse，然后拼接在一起组成rowkey，达到散列的目的，避免数据倾斜。
    private byte[] generateRowkey(String id, String birthday) {

        byte[] newBytes = null;

        try {
            //先将id转化成Interger类型，然后进行反转。
            int d = Integer.parseInt(id);
            //使用反射调用本类当中的reverse()方法，这样的话就可以输入不同的数据类型。
            Object objectId = this.getClass().getMethod("reverse", Object.class).invoke(this, d);
            int reverseId = (int) objectId;

            byte[] idByte = Bytes.toBytes(reverseId);
            byte[] birthdayByte = Bytes.toBytes(birthday);

            //将两个byte[]拼接在一起。
            int length = idByte.length + birthdayByte.length;
            newBytes = new byte[length];
            System.arraycopy(idByte, 0, newBytes, 0, 4);
            System.arraycopy(birthdayByte, 0, newBytes, 4, birthdayByte.length);
        }
        catch (Exception e) {
            e.printStackTrace();
        }

        return newBytes;
    }

    //将value字符串转化为byte[]。
    private byte[] generateValue(String value) {
        return Bytes.toBytes(value);
    }

    //重要：要通过反射来调用的方法，访问级别必须是public。
    //对Long、Interger、String三种类型的数据进行reverse。
    public Object reverse(Object data) {
        if (Long.class.equals(data.getClass())) {
            return Long.reverse((long) data);
        }
        if (Integer.class.equals(data.getClass())) {
            return Integer.reverse((int) data);
        }
        else {
            return StringUtils.reverse((String) data);
        }
    }
}
