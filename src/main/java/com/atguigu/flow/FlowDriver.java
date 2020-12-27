package com.atguigu.flow;

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
 * @CreateDate : 2020/7/17 20:33
 * @Description :
 */
public class FlowDriver {
    // 切片是在客户端完成的 准确说是在Driver完成的
    public static void main(String[] args) throws IOException, ClassNotFoundException, InterruptedException {
        //创建job对象
        Job job = Job.getInstance(new Configuration());
        //关联jar
        job.setJarByClass(FlowBean.class);
        //关联mapper和reducer
        job.setMapperClass(FlowMapper.class);
        job.setReducerClass(FlowReducer.class);
        //设置map输出的kv类型
        job.setMapOutputKeyClass(Text.class);
        job.setMapOutputValueClass(FlowBean.class);
        //设置最终输出的kv类型 因为可能没有reduce 所以map输出类型是有可能是最终的类型的
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(FlowBean.class);

        //设置输入输出的路径
        FileInputFormat.setInputPaths(job, new Path("D:\\phone_data .txt"));//写两个反斜杠或者 一个/都可以
        FileOutputFormat.setOutputPath(job, new Path("D:/output"));
        //提交job
        job.waitForCompletion(true);
    }
}
