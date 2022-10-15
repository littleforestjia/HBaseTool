package com.jd.hbase.exportSnapshotBulkload;

import org.apache.hadoop.hbase.Cell;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.io.ImmutableBytesWritable;
import org.apache.hadoop.hbase.mapreduce.TableMapper;
import org.apache.hadoop.hbase.util.Bytes;

import java.io.IOException;

public class ReverseMapper extends TableMapper<ImmutableBytesWritable, Put> {

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
