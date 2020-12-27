package com.atguigu.wordcount;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

import java.io.IOException;

/**
 * @Author : dky
 * @Version : 1.0
 * @CreateDate : 2020/7/17 11:14
 * @Description :
 */
public class McDriver {
    public static void main(String[] args) throws IOException, ClassNotFoundException, InterruptedException {

        // 1 获取配置信息以及封装任务
        Configuration configuration = new Configuration();
        configuration.set("fs.defaultFS","hdfs://hadoop102:8020");
        //设置向hive队列提交
        //如果是在本地idea上运行 就不用再次打包
        configuration.set("mapred.job.queue.name", "hive");
        //表示任务往yarn提交
        configuration.set("mapreduce.framework.name", "yarn");
        //指定Yarn resourcemanager的位置
        configuration.set("yarn.resourcemanager.hostname", "hadoop103");
        //  指定mapreduce可以在远程集群运行
        configuration.set("mapreduce.app-submission.cross-platform", "true");
        Job job = Job.getInstance(configuration);

        // 2 设置jar加载路径
       // job.setJarByClass(McDriver.class);//这里写我们自己写的三个类其一都可以
      //  job.setJar("E:\\000000作业\\mapreduce0523\\target\\mapreduce0523-1.0-SNAPSHOT.jar");
        //表示 要找到包含 有我们这三个类 的jar包
        // 表示我们写的程序 是在包含这个字节码信息的jar包里面 所以是提供了一个路径

        // 3 设置map和reduce类
        job.setMapperClass(McMapper.class);
        job.setReducerClass(McReducer.class);

        // 4 设置map输出
        job.setMapOutputKeyClass(Text.class);
        job.setMapOutputValueClass(IntWritable.class);

        // 5 设置最终输出kv类型
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(IntWritable.class);

        // 6 设置输入和输出路径
        FileInputFormat.setInputPaths(job, new Path(args[0]));
        FileOutputFormat.setOutputPath(job, new Path(args[1]));

        // 7 提交
        boolean result = job.waitForCompletion(true);

        System.exit(result ? 0 : 1);
    }
}

