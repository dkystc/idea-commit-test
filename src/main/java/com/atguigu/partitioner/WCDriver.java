package com.atguigu.partitioner;

import com.atguigu.writablecomparable.FlowBean;
import com.atguigu.writablecomparable.WCMapper;
import com.atguigu.writablecomparable.WCReducer;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

import java.io.IOException;

/**
 * @Author : dky
 * @Version : 1.0
 * @CreateDate : 2020/7/18 11:52
 * @Description :
 */
public class WCDriver {
    public static void main(String[] args) throws IOException, ClassNotFoundException, InterruptedException {
        Job job = Job.getInstance(new Configuration());

        job.setJarByClass(FlowBean.class);

        job.setMapperClass(WCMapper.class);
        job.setReducerClass(WCReducer.class);

        job.setMapOutputKeyClass(FlowBean.class);
        job.setMapOutputValueClass(Text.class);
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(FlowBean.class);


        job.setPartitionerClass(WCPartitioner.class);
        job.setNumReduceTasks(5);


        FileInputFormat.setInputPaths(job, new Path("d:/input "));
        FileOutputFormat.setOutputPath(job, new Path("d:/output2"));
        boolean b = job.waitForCompletion(true);

        System.exit(b ? 0 : 1);
    }
}
