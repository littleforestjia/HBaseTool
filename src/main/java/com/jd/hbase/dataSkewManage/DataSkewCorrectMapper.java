package com.jd.hbase.dataSkewManage;

import org.apache.hadoop.hbase.Cell;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.io.ImmutableBytesWritable;
import org.apache.hadoop.hbase.mapreduce.TableMapper;
import org.apache.hadoop.hbase.util.Bytes;

import java.io.IOException;

public class DataSkewCorrectMapper extends TableMapper<ImmutableBytesWritable, Put> {

    //该Mapper所对应的每次map任务从hbase表中取出一行数据，并重新将改行数据封装成一个Put对象。
    public void map(ImmutableBytesWritable row, Result value, Context context) throws InterruptedException, IOException {

        byte[] rowkey = reverseFirstField(row.get(), 4);
        ImmutableBytesWritable putRowkey = new ImmutableBytesWritable(rowkey);
        Put put = new Put(rowkey);
        for (Cell cell : value.listCells()) {
            byte[] colFamily = new byte[cell.getFamilyLength()];
            byte[] colQualifier = new byte[cell.getQualifierLength()];
            byte[] colValue = new byte[cell.getValueLength()];
            System.arraycopy(cell.getFamilyArray(), cell.getFamilyOffset(), colFamily, 0, cell.getFamilyLength());
            System.arraycopy(cell.getQualifierArray(), cell.getQualifierOffset(), colQualifier, 0, cell.getQualifierLength());
            System.arraycopy(cell.getValueArray(), cell.getValueOffset(), colValue, 0, cell.getValueLength());

            put.add(colFamily, colQualifier, colValue);
        }
        context.write(putRowkey, put);
    }

    //对组成rowkey的首个字段进行reverse。
    public byte[] reverseFirstField(byte[] rowkey, int firstFieldLenth) {
        byte[] reverseField = new byte[firstFieldLenth];
        System.arraycopy(rowkey, 0, reverseField, 0, firstFieldLenth);

        if (firstFieldLenth == 4) {
            int reverseInt = Integer.reverse(Bytes.toInt(reverseField));
            reverseField = Bytes.toBytes(reverseInt);
        }else {
            long reverseLong = Long.reverse(Bytes.toLong(reverseField));
            reverseField = Bytes.toBytes(reverseLong);
        }

        System.arraycopy(reverseField, 0, rowkey, 0, firstFieldLenth);
        return rowkey;
    }
}
