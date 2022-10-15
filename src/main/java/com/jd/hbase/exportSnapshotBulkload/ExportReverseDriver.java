package com.jd.hbase.exportSnapshotBulkload;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.*;
import org.apache.hadoop.hbase.io.ImmutableBytesWritable;
import org.apache.hadoop.hbase.mapreduce.HFileOutputFormat2;
import org.apache.hadoop.hbase.mapreduce.LoadIncrementalHFiles;
import org.apache.hadoop.hbase.mapreduce.TableMapReduceUtil;
import org.apache.hadoop.mapreduce.Job;

public class ExportReverseDriver {
    public static void main(String[] args) throws Exception {

        String hdfsRoot = "hdfs://localhost:9005/"; //hdfs 的根目录
        String snapshotName = "fellowshell_snapshot";
        String destTableName = "exportSnapshotReverse";
        String tmpDir = "testpath";

        Configuration conf = HBaseConfiguration.create();

        /**重要：因为我自己电脑的缘故，导致zookeeper的默认端口2181被占用，启动hbase时会自动将zookeeper启动到2182端口上，
         那么就需要我在hbase的hbase-site.xml文件中配置hbase去连接zookeeper的2182端口，
         同时也需要我在java程序中配置hbase.zookeeper.property.clientPort为2182。*/
        conf.set("hbase.zookeeper.property.clientPort", "2182");

        //重要：将hbase.rootdir设置为存储export快照文件的文件夹。
        //此程序中除了获取快照文件，后续不再使用到hbase.rootdir参数，所以该设置并不会影响hbase系统的文件目录结构，如果后续还需要使用则需要注意。
        conf.set("hbase.rootdir", "hdfs://localhost:9005/exportSnapshot");

        //重要：initTableSnapshotMapperJob方法会在添加的tmp目录下创建临时文件夹，用来存放在hbase客户端本地临时创建的region，
        //该方法先根据快照文件和数据文件在该文件夹中重建快照表的region，然后scan再扫描这些region获取数据。
        //创建该文件夹时如果输入参数没有指定文件系统，则文件夹目录为fs.defaultFS+tmp，如果指定了文件系统则就是tmp，
        //扫描数据时则是从fs.defaultFS目录往下去寻找region。所以在设置该tmp参数时，要么不要指定文件系统，要么就指定文件系统为fs.defaultFS。
        conf.set("fs.defaultFS", hdfsRoot);

        Connection conn = ConnectionFactory.createConnection(conf);
        Table destTable = conn.getTable(TableName.valueOf(destTableName));
        Admin admin = conn.getAdmin();

        Job job = Job.getInstance(conf, "reverse table : " + snapshotName);
        Scan scan = new Scan();
        scan.setCaching(500);
        scan.setCacheBlocks(false);
        TableMapReduceUtil.initTableSnapshotMapperJob(snapshotName, scan, ReverseMapper.class, ImmutableBytesWritable.class, Put.class, job, false, new Path(tmpDir));

        Path outputPath = new Path(hdfsRoot + destTableName);
        HFileOutputFormat2.setOutputPath(job, outputPath);
        HFileOutputFormat2.configureIncrementalLoad(job, destTable, conn.getRegionLocator(TableName.valueOf(destTableName)));

        job.waitForCompletion(true);

        if (job.isSuccessful()) {
            System.out.println("reverse成功！");
            LoadIncrementalHFiles loader = new LoadIncrementalHFiles(conf);
            //loader.doBulkLoad(outputPath, admin, destTable, conn.getRegionLocator(TableName.valueOf(destTableName)));
        }else {
            System.out.println("reverse失败！");
        }
    }
}
