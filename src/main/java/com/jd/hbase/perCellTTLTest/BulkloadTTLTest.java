package com.jd.hbase.perCellTTLTest;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.*;
import org.apache.hadoop.hbase.io.ImmutableBytesWritable;
import org.apache.hadoop.hbase.mapreduce.HFileOutputFormat2;
import org.apache.hadoop.hbase.mapreduce.LoadIncrementalHFiles;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;

import java.io.IOException;

public class BulkloadTTLTest {
    public static void main(String[] args) throws Exception {
        //这里创建HBase连接的目的是将数据放到目标表当中。
        Configuration conf = HBaseConfiguration.create();
        conf.set("hbase.zookeeper.property.clientPort", "2182");
        conf.set("hbase.replication.rpc.codec", "org.apache.hadoop.hbase.codec.CellCodecWithTags");
        String hdfsRoot = "hdfs://localhost:9005/";
        conf.set("fs.defaultFS", hdfsRoot);

        Connection conn = ConnectionFactory.createConnection(conf);
        Table table = conn.getTable(TableName.valueOf("fellowjava"));
        Admin admin = conn.getAdmin();

        //设置相关类
        Job job = Job.getInstance(conf, "jiazhengyang bulkload: fellowjava");
        job.setJarByClass(BulkloadTTLTest.class);
        job.setMapperClass(BulkloadTTLTestMapper.class);
        job.setMapOutputKeyClass(ImmutableBytesWritable.class);
        job.setMapOutputValueClass(Put.class);
        //job.setCombinerClass(PutCombiner.class);
        //job.setReducerClass(PutSortReducer.class);

        //设置源Hfile路径和目标Hfile路径
        String inputFile = hdfsRoot + "fellowtest";
        String outputFile = hdfsRoot + "fellowjavahfile";
        Path outputPath = new Path(outputFile);

        try {
            FileSystem fileSystem = FileSystem.get(conf);
            if (fileSystem.exists(outputPath)) {
                fileSystem.delete(outputPath, true);
            }
        } catch (IOException e1) {
            e1.printStackTrace();
        }

        job.setInputFormatClass(TextInputFormat.class);
        TextInputFormat.setInputPaths(job, inputFile);
        job.setOutputFormatClass(HFileOutputFormat2.class);
        HFileOutputFormat2.setOutputPath(job, outputPath);

        //配置MR作业任务，将源Hfile数据以增量的形式加载到目标Hfile当中，并生成Hbase表元数据文件。
        HFileOutputFormat2.configureIncrementalLoad(job, table, conn.getRegionLocator(TableName.valueOf("fellowjava")));

        job.waitForCompletion(true);

        //在MR作业完成后，将Hbase表元数据文件推送给Hbase的RegionServers，这样就可以通过Hbase操作表数据。
        if (job.isSuccessful()) {
            System.out.println("Hfile创建成功！");
            LoadIncrementalHFiles loader = new LoadIncrementalHFiles(conf);
            loader.doBulkLoad(outputPath, admin, table, conn.getRegionLocator(TableName.valueOf("fellowjava")));
            //loader.doBulkLoad(outputPath, (HTable) conn.getRegionLocator(TableName.valueOf("fellowjava")));
        }else {
            System.out.println("Hfile创建失败！");
        }
    }
}
