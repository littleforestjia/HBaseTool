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

public class ExportSnapshotDemo {
    public static void main(String[] args) {

        Configuration conf = HBaseConfiguration.create();
        String hdfsRoot = "hdfs://localhost:9005/"; //hdfs 的根目录
        conf.set("hbase.zookeeper.property.clientPort", "2182");
        conf.set("fs.defaultFS", hdfsRoot);
        conf.set("hbase.rootdir","hdfs://localhost:9005/exportSnapshot");
        String exprotSnapshot = "fellowshell_snapshot"; // snapshot name
        Scan scan = new Scan();

        scan.setCaching(500);
        scan.setCacheBlocks(false);

        Job job = null;
        try {
            job = Job.getInstance(conf, "Analyze data in exportsnapshot " + exprotSnapshot);
            TableMapReduceUtil.initTableSnapshotMapperJob(exprotSnapshot, scan, ReadHFileMapper.class, Text.class, Text.class, job, false, new Path(hdfsRoot+"testpath"));
            FileOutputFormat.setOutputPath(job, new Path(hdfsRoot+"exportSnapshotread"));
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
