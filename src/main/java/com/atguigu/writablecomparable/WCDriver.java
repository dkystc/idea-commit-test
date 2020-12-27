package com.atguigu.writablecomparable;

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
 * @CreateDate : 2020/7/18 11:27
 * @Description :
 */
public class WCDriver {
    public static void main(String[] args) throws IOException, ClassNotFoundException, InterruptedException {
        Job job = Job.getInstance(new Configuration());

        //关联jar包
        job.setJarByClass(FlowBean.class);
        //关联Mapper和Reducer
        job.setMapperClass(WCMapper.class);
        job.setReducerClass(WCReducer.class);
        //设置map输出类型
        job.setMapOutputKeyClass(FlowBean.class);
        job.setMapOutputValueClass(Text.class);
        //最终类型
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(FlowBean.class);
        //输入输出路径
        FileInputFormat.setInputPaths(job, new Path("d:/output"));
        FileOutputFormat.setOutputPath(job, new Path("d:/output1"));

        job.waitForCompletion(true);
    }
}
