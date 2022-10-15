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

public class Demo {
    public static void main(String[] args) {

        Configuration conf = HBaseConfiguration.create();
        //非常重要：因为我自己电脑的缘故，导致zookeeper的默认端口2181被占用，启动hbase时会自动将zookeeper启动到2182端口上，
        //那么就需要我在hbase的hbase-site.xml文件中配置hbase去连接zookeeper的2182端口，
        //同时也需要我在java程序中配置hbase.zookeeper.property.clientPort为2182。
        conf.set("hbase.zookeeper.property.clientPort", "2182");

        String hdfsRoot = "hdfs://localhost:9005/"; //hdfs 的根目录
        String tablename = "fellowjava";
        Scan scan = new Scan();
        scan.setCaching(500);
        scan.setCacheBlocks(false);
        Job job = null;

        try {
            job = Job.getInstance(conf, "read table : " + tablename);

            //重要：在该scanMR执行过程中，会自动扫描hbase文件夹，根据表名找到表的元数据文件，不需要设置文件输入路径。
            //tablename为源表名，scan为扫描类对象
            TableMapReduceUtil.initTableMapperJob(tablename, scan, ReadHFileMapper.class, Text.class, Text.class, job, false);

            //重要：扫描得到的数据以HFile的形式放在HDFS目录下。
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
