package com.jd.hbase.perCellTTLTest;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.LocatedFileStatus;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.RemoteIterator;
import org.apache.hadoop.hbase.*;
import org.apache.hadoop.hbase.io.ImmutableBytesWritable;
import org.apache.hadoop.hbase.io.hfile.CacheConfig;
import org.apache.hadoop.hbase.io.hfile.HFile;
import org.apache.hadoop.hbase.io.hfile.HFileScanner;
import org.apache.hadoop.hbase.mapreduce.HFileOutputFormat2;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.RecordWriter;
import org.apache.hadoop.mapreduce.TaskAttemptContext;
import org.apache.hadoop.mapreduce.TaskAttemptID;
import org.apache.hadoop.mapreduce.task.TaskAttemptContextImpl;

import java.io.FileNotFoundException;
import java.io.IOException;
import java.util.Iterator;

import static org.junit.Assert.assertTrue;

/**
 * 本测试案例使用hbase1.3可以通过，使用hbase1.1.6和hbase1.6.0却无法通过
 */
public class WriteTagTest {
    public static void main(String[] args) throws IOException, InterruptedException {
        Configuration conf = HBaseConfiguration.create();
        conf.set("hbase.zookeeper.property.clientPort", "2182");
        String hdfsRoot = "hdfs://localhost:9005/";
        conf.set("fs.defaultFS", hdfsRoot);
        //conf.set("hbase.client.rpc.codec", "org.apache.hadoop.hbase.codec.KeyValueCodecWithTags");
        final String HFILE_FORMAT_VERSION_CONF_KEY = "hfile.format.version";
        conf.setInt(HFILE_FORMAT_VERSION_CONF_KEY, HFile.MIN_FORMAT_VERSION_WITH_TAGS);
        RecordWriter<ImmutableBytesWritable, Cell> writer = null;
        TaskAttemptContext context = null;
        Path outPutDir = new Path("hdfs://localhost:9005/WriteTagTest");
        try {
            FileSystem fileSystem = FileSystem.get(conf);
            if (fileSystem.exists(outPutDir)) {
                fileSystem.delete(outPutDir, true);
            }
        } catch (IOException e1) {
            e1.printStackTrace();
        }
        try {
            Job job = new Job(conf);
            HFileOutputFormat2.setOutputPath(job, outPutDir);
            context = new TaskAttemptContextImpl(job.getConfiguration(), TaskAttemptID.forName("attempt_202210021446_0001_m_000000_0"));
            HFileOutputFormat2 hof = new HFileOutputFormat2();
            writer = hof.getRecordWriter(context);
            final byte [] b = Bytes.toBytes("b");

            KeyValue kv = new KeyValue(b, b, b, HConstants.LATEST_TIMESTAMP, b, new Tag[] {
                    new Tag(TagType.TTL_TAG_TYPE, Bytes.toBytes(978670L)) });
            writer.write(new ImmutableBytesWritable(), kv);
            writer.close(context);
            writer = null;
            FileSystem fs = outPutDir.getFileSystem(conf);
            RemoteIterator<LocatedFileStatus> iterator = fs.listFiles(outPutDir, true);
            while(iterator.hasNext()) {
                LocatedFileStatus keyFileStatus = iterator.next();
                HFile.Reader reader = HFile.createReader(fs, keyFileStatus.getPath(), new CacheConfig(conf), conf);
                HFileScanner scanner = reader.getScanner(false, false, false);
                scanner.seekTo();
                Cell cell = scanner.getKeyValue();
                System.out.println("------------------------------------------");
                System.out.println("该cell中的值为：" + Bytes.toString(cell.getValue()));
                System.out.println("------------------------------------------");
                Iterator<Tag> tagsIterator = CellUtil.tagsIterator(cell.getTagsArray(),
                        cell.getTagsOffset(), cell.getTagsLength());
                assertTrue(tagsIterator.hasNext());
                System.out.println("该cell存在tag！");
                Tag tag = tagsIterator.next();
                if (TagType.TTL_TAG_TYPE == tag.getType()) {
                    long ttl = Bytes.toLong(tag.getBuffer(), tag.getTagOffset(), tag.getTagLength());
                    System.out.println("TTL为：" + ttl);
                }
            }
        } catch (InterruptedException e) {
            e.printStackTrace();
        } catch (FileNotFoundException e) {
            e.printStackTrace();
        } catch (IOException e) {
            e.printStackTrace();
        } finally {
            if (writer != null && context != null) writer.close(context);
            outPutDir.getFileSystem(conf).delete(outPutDir, true);
        }
    }
}
