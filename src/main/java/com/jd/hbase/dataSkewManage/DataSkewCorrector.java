package com.jd.hbase.dataSkewManage;

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

import java.io.IOException;

public class DataSkewCorrector {
    public static void main(String[] args) throws Exception {

        try {

            //这里创建HBase连接的目的是将数据放到目标表当中。
            Configuration conf = HBaseConfiguration.create();
            Connection conn = ConnectionFactory.createConnection(conf);
            Table destTable = conn.getTable(TableName.valueOf("fellowreverse"));
            Admin admin = conn.getAdmin();

            //设置被读取的Hbase表名
            String hdfsRoot = "hdfs://localhost:9005/"; //hdfs 的根目录
            String srcTablename = "fellowjava";

            //设置相关读取配置
            Job job = Job.getInstance(conf, "reverse table : " + srcTablename);
            Scan scan = new Scan();
            scan.setCaching(500);
            scan.setCacheBlocks(false);
            TableMapReduceUtil.initTableMapperJob(srcTablename, scan, DataSkewCorrectMapper.class, ImmutableBytesWritable.class, Put.class, job, false);

            //设置目标Hfile路径，并配置生成Hbase表元数据文件。
            Path outputPath = new Path(hdfsRoot +"fellowreverse");
            HFileOutputFormat2.setOutputPath(job, outputPath);
            HFileOutputFormat2.configureIncrementalLoad(job, destTable, conn.getRegionLocator(TableName.valueOf("fellowreverse")));

            //执行MR任务
            job.waitForCompletion(true);

            //在MR作业完成后，将Hbase表元数据文件推送给Hbase的RegionServers，这样就可以通过Hbase操作表数据。
            if (job.isSuccessful()) {
                System.out.println("reverse成功！");
                LoadIncrementalHFiles loader = new LoadIncrementalHFiles(conf);
                //loader.doBulkLoad(outputPath, admin, destTable, conn.getRegionLocator(TableName.valueOf("fellowreverse")));
            }else {
                System.out.println("reverse失败！");
            }

        } catch (IOException e) {
            e.printStackTrace();
        } catch (InterruptedException e) {
            e.printStackTrace();
        } catch (ClassNotFoundException e) {
            e.printStackTrace();
        }

    }
}
