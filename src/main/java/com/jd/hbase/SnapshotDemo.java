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
        String hdfsRoot = "hdfs://pandora/"; //
        conf.set("fs.defaultFS", hdfsRoot);
        conf.set("hbase.rootdir", hdfsRoot+"hbase");//hfile
        conf.set("dfs.nameservices", "xxx");
        conf.set("dfs.ha.namenodes.xxx", "nn1,nn2");
        conf.set("dfs.namenode.rpc-address.xxx.nn1", "xxx");
        conf.set("dfs.namenode.rpc-address.xxx.nn2", "xxx");
        conf.set("dfs.client.failover.proxy.provider.xxx", "org.apache.hadoop.hdfs.server.namenode.ha.ConfiguredFailoverProxyProvider");
        String snapshot = ""; // snapshot name
        Scan scan = new Scan();
        scan.setCaching(500);
        scan.setCacheBlocks(false);
        Job job = null;
        try {
            job = Job.getInstance(conf, "Analyze data in snapshot " + snapshot);
            TableMapReduceUtil.initTableSnapshotMapperJob(snapshot, // The name of the snapshot (of a table) to read from
                    scan, // Scan instance to control CF and attribute selection
                    ReadHFileMapper.class, // mapper
                    Text.class,             // mapper output key
                    Text.class,             // mapper output value.
                    job, // The current job to adjust
                    false, // upload HBase jars and jars for any of the configured job classes via the distributed cache (tmpjars)
                    new Path(hdfsRoot+"tmp/hbase-snapshot") // how many input splits to generate per one region
            );
            FileOutputFormat.setOutputPath(job, new Path(hdfsRoot+"tmp/hbase-output/"+snapshot));
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
