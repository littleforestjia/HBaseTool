package com.jd.hbase;

import com.jd.hbase.bulkloadReappear.ReadBulkloadHfileMapper;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.mapreduce.TableMapReduceUtil;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

import java.io.IOException;

public class Demo {
    public static void main(String[] args) {

        Configuration conf = HBaseConfiguration.create();
        String hdfsRoot = "hdfs://localhost:9005/"; //hdfs 的根目录
        String tablename = "fellowjava";
        Scan scan = new Scan();
        scan.setCaching(500);
        scan.setCacheBlocks(false);
        Job job = null;
        try {
            job = Job.getInstance(conf, "read table : " + tablename);

            //tablename为源表名，scan为扫描类对象
            TableMapReduceUtil.initTableMapperJob(tablename, scan, ReadBulkloadHfileMapper.class, Text.class, Text.class, job, false);

            FileOutputFormat.setOutputPath(job, new Path( hdfsRoot +"fellowjavaread1"));
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
