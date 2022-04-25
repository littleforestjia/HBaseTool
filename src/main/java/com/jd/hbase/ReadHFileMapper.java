package com.jd.hbase;
import org.apache.hadoop.hbase.Cell;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.io.ImmutableBytesWritable;
import org.apache.hadoop.hbase.mapreduce.TableMapper;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.io.Text;
import sun.awt.windows.WPrinterJob;

import java.io.IOException;

public class ReadHFileMapper extends TableMapper<Text, Text> {
        private Text writeKey = new Text();

        @Override
        public void map(ImmutableBytesWritable row, Result value, Context context)
                throws InterruptedException, IOException {

                //重要：在该scanMR执行过程中，会自动扫描hbase文件夹，根据表名找到表的元数据文件，不需要设置文件输入路径。
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
