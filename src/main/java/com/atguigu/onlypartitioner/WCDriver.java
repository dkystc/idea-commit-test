package com.atguigu.onlypartitioner;

import com.atguigu.flow.FlowBean;
import com.atguigu.flow.FlowMapper;
import com.atguigu.flow.FlowReducer;
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
 * @CreateDate : 2020/7/18 13:48
 * @Description :
 */
public class WCDriver {
    public static void main(String[] args) throws IOException, ClassNotFoundException, InterruptedException {
        Job job = Job.getInstance(new Configuration());
        job.setJarByClass(WCDriver.class);


        job.setMapperClass(FlowMapper.class);
        job.setReducerClass(FlowReducer.class);

        job.setMapOutputKeyClass(Text.class);

        job.setMapOutputValueClass(FlowBean.class);
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(FlowBean.class);

        job.setNumReduceTasks(5);
        job.setPartitionerClass(MyPartitioner.class);
        FileInputFormat.setInputPaths(job, new Path("d:/input"));
        FileOutputFormat.setOutputPath(job, new Path("d:/outputonlypart"));
        boolean b = job.waitForCompletion(true);
    }
}
