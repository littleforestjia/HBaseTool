package com.jd.hbase;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.mapreduce.TableMapReduceUtil;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

import java.io.IOException;

public class SnapshotDemo {

    public static void main(String[] args) {

        Configuration conf = HBaseConfiguration.create();
        String hdfsRoot = "hdfs://localhost:9005/"; //hdfs 的根目录
        conf.set("hbase.zookeeper.property.clientPort", "2182");
        conf.set("fs.defaultFS", hdfsRoot);
        conf.set("hbase.rootdir","hdfs://localhost:9005/hbase");
        String snapshot = "fellowshell_snapshot"; // snapshot name
        Scan scan = new Scan();

        //使用scanMR时这两个参数一定要设置
        scan.setCaching(500);
        scan.setCacheBlocks(false);

        Job job = null;
        try {

            //重要：snapshot快照文件的本质是hbase表的元数据；
            // snapshotscanMR在执行时，先把snapshot快照文件复制到这个tmp path临时目录，然后根据该元数据文件绕过hbase直接从hdfs中读取hfile数据文件。
            // snapshotscanMR完成作业后，可以删除这个临时目录和其中文件。
            job = Job.getInstance(conf, "Analyze data in snapshot " + snapshot);
            TableMapReduceUtil.initTableSnapshotMapperJob(snapshot, scan, ReadHFileMapper.class, Text.class, Text.class, job, false, new Path(hdfsRoot+"testpath"));
            FileOutputFormat.setOutputPath(job, new Path(hdfsRoot+"snapshotread"));
            System.exit(job.waitForCompletion(true) ? 0 : 1);
        } catch (IOException e) {
            e.printStackTrace();
        } catch (InterruptedException e) {
            e.printStackTrace();
        } catch (ClassNotFoundException e) {
            e.printStackTrace();
        }
    }
}
