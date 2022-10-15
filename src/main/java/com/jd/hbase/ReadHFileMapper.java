package com.jd.hbase;
import org.apache.hadoop.hbase.Cell;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.io.ImmutableBytesWritable;
import org.apache.hadoop.hbase.mapreduce.TableMapper;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.io.Text;

import java.io.IOException;

public class ReadHFileMapper extends TableMapper<Text, Text> {
        private Text writeKey = new Text();

        @Override
        public void map(ImmutableBytesWritable row, Result value, Context context)
                throws InterruptedException, IOException {

                        //Cell是Hbase当中的最小存储单元，也就是Hbase表中的一个数据单元格，其中主要以key-value的形式保存数据。
                        for (Cell cell : value.listCells()) {
                                String colName = Bytes.toString(cell.getQualifierArray(), cell.getQualifierOffset(), cell.getQualifierLength());
                                String colValue = Bytes.toString(cell.getValueArray(), cell.getValueOffset(), cell.getValueLength()).replaceAll("\\n|\\r|\\t|\1|\\\\N", " ");
                                String res = colName + "," + colValue;
                                writeKey.set(new Text(Bytes.toString(row.get())));
                                context.write(writeKey, new Text(res));
                                System.out.println(Bytes.toString(row.get()) + " " + res);
                        }
        }
}
