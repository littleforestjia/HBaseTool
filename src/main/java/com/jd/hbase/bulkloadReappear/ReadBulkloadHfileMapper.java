package com.jd.hbase.bulkloadReappear;

import org.apache.hadoop.hbase.Cell;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.io.ImmutableBytesWritable;
import org.apache.hadoop.hbase.mapreduce.TableMapper;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;

//读取用于测试而bulkload创建的fellowjava表的hfile文件。
public class ReadBulkloadHfileMapper extends TableMapper<Text, Text> {
    private Text writeKey = new Text();

    @Override
    public void map(ImmutableBytesWritable row, Result value, Mapper.Context context)
            throws InterruptedException, IOException {

        for (Cell cell : value.listCells()) {
            String colName = Bytes.toString(cell.getQualifierArray(), cell.getQualifierOffset(), cell.getQualifierLength());
            String colValue = Bytes.toString(cell.getValueArray(), cell.getValueOffset(), cell.getValueLength()).replaceAll("\\n|\\r|\\t|\1|\\\\N", " ");
            String res = colName + "," + colValue;

            byte[] rowkey = row.get();
            byte[] bytesId = new byte[4];
            byte[] bytesBd = new byte[rowkey.length - 4];
            System.arraycopy(rowkey, 0, bytesId, 0, 4);
            System.arraycopy(rowkey, 4, bytesBd, 0, bytesBd.length);

            int reverseId = Bytes.toInt(bytesId);
            int id = Integer.reverse(reverseId);


            writeKey.set(new Text(id + Bytes.toString(bytesBd)));
            context.write(writeKey, new Text(res));
            System.out.println(id + " " + Bytes.toString(bytesBd) + " " + res);
        }
    }
}
