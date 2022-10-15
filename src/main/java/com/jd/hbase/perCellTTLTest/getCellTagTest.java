package com.jd.hbase.perCellTTLTest;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.LocatedFileStatus;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.RemoteIterator;
import org.apache.hadoop.hbase.*;
import org.apache.hadoop.hbase.client.*;
import org.apache.hadoop.hbase.io.hfile.CacheConfig;
import org.apache.hadoop.hbase.io.hfile.HFile;
import org.apache.hadoop.hbase.io.hfile.HFileScanner;
import org.apache.hadoop.hbase.util.Bytes;

import java.io.IOException;
import java.util.Iterator;
import java.util.List;

public class getCellTagTest {
    public static void main(String[] args) throws IOException {
        //这里创建HBase连接的目的是将数据放到目标表当中。
        Configuration conf = HBaseConfiguration.create();
        conf.set("hbase.zookeeper.property.clientPort", "2182");
        //重要，要从scan中获取到tag，必须要配置该参数。
        conf.set("hbase.client.rpc.codec", "org.apache.hadoop.hbase.codec.KeyValueCodecWithTags");
        Connection conn = ConnectionFactory.createConnection(conf);
        Table table = conn.getTable(TableName.valueOf("fellowjava"));

        //1.get获取数据
        Get get = new Get(Bytes.toBytes("hehe"));
        Result rs = table.get(get);
        if (rs != null) {
            List<Cell> cellList = rs.getColumnCells(Bytes.toBytes("d"), Bytes.toBytes("feild"));
            System.out.println("结果列表的长度为：" + cellList.size());
            if (cellList.size() == 0) {
                System.out.println("无数据！");
            } else {
                Cell cell = cellList.get(0);
                System.out.println(Bytes.toString(cell.getValue()));
                Iterator<Tag> i = CellUtil.tagsIterator(cell.getTagsArray(), cell.getTagsOffset(), cell.getTagsLength());
                while (i.hasNext()) {
                    System.out.println("该cell存在tag！");
                    Tag tag = i.next();
                    if (TagType.TTL_TAG_TYPE == tag.getType()) {
                        long ttl = Bytes.toLong(tag.getBuffer(), tag.getTagOffset(), tag.getTagLength());
                        System.out.println("TTL为：" + ttl);
                    }
                }
                System.out.println("查询结果无TTL的TAG");
            }
        } else {
            System.out.println("查询结果为空");
        }
        System.out.println("------------------------------------------");

        //2.scan获取数据
        Scan scan = new Scan();
        scan.addFamily(Bytes.toBytes("d"));
        scan.setStartRow(Bytes.toBytes("2384783"));
        scan.setStopRow(Bytes.toBytes("2384783"));

        ResultScanner scanner = table.getScanner(scan);
        Result result = scanner.next();
        if (result != null) {
            List<Cell> cellList = result.getColumnCells(Bytes.toBytes("d"), Bytes.toBytes("1"));
            System.out.println("结果列表的长度为：" + cellList.size());
            if (cellList.size() == 0) {
                System.out.println("无数据！");
                return;
            } else {
                Cell cell = cellList.get(0);
                System.out.println(Bytes.toString(cell.getValue()));
                Iterator<Tag> i = CellUtil.tagsIterator(cell.getTagsArray(), cell.getTagsOffset(), cell.getTagsLength());
                while (i.hasNext()) {
                    System.out.println("该cell存在tag！");
                    Tag tag = i.next();
                    if (TagType.TTL_TAG_TYPE == tag.getType()) {
                        long ttl = Bytes.toLong(tag.getBuffer(), tag.getTagOffset(), tag.getTagLength());
                        System.out.println("TTL为：" + ttl);
                    }
                }
            }
        } else {
            System.out.println("查询结果为空");
        }
        System.out.println("------------------------------------------");

        //3.直接读取hfile获取数据
        Path outPutDir = new Path("hdfs://localhost:9005/hbase/data/default/fellowjava/f3fa507630e704a9d735a73f281dee58/d");
        //Path outPutDir = new Path("hdfs://localhost:9005/fellowjavahfile/d");
        FileSystem fs = outPutDir.getFileSystem(conf);
        RemoteIterator<LocatedFileStatus> iterator = fs.listFiles(outPutDir, true);
        LocatedFileStatus keyFileStatus = iterator.next();
        HFile.Reader reader = HFile.createReader(fs, keyFileStatus.getPath(), new CacheConfig(conf), conf);
        HFileScanner hFileScanner = reader.getScanner(false, false, false);
        hFileScanner.seekTo();
        Cell cell = hFileScanner.getKeyValue();
        System.out.println(Bytes.toString(cell.getValue()));
        Iterator<Tag> i = CellUtil.tagsIterator(cell.getTagsArray(), cell.getTagsOffset(), cell.getTagsLength());
        while (i.hasNext()) {
            System.out.println("该cell存在tag！");
            Tag tag = i.next();
            if (TagType.TTL_TAG_TYPE == tag.getType()) {
                long ttl = Bytes.toLong(tag.getBuffer(), tag.getTagOffset(), tag.getTagLength());
                System.out.println("TTL为：" + ttl);
            }
        }
    }
}
