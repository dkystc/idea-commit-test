package com.atguigu.newwordcount;

import com.atguigu.wordcount.McMapper;
import com.atguigu.wordcount.McReducer;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.Tool;

import java.io.IOException;

/**
 * @Author : dky
 * @Version : 1.0
 * @CreateDate : 2020/7/17 11:14
 * @Description :
 */
public class McDriver implements Tool {
    private Configuration conf  ;
    /**
     * 替代了run方法
     * @param args
     * @return
     * @throws Exception
     */
    @Override
    public int run(String[] args) throws Exception {
        // 1 获取配置信息以及封装任务


        Job job = Job.getInstance(conf);

        // 2 设置jar加载路径
        // job.setJarByClass(McDriver.class);//这里写我们自己写的三个类其一都可以
        job.setJar("E:\\000000作业\\mapreduce0523\\target\\mapreduce0523-1.0-SNAPSHOT.jar");
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


        return result ? 0 : 1;
    }

    @Override
    public void setConf(Configuration conf) {
        this.conf = conf;
    }

    @Override
    public Configuration getConf() {
        return conf;
    }
}

