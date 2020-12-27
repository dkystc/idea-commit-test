package com.atguigu.wordcount;

/**
 * @Author : dky
 * @Version : 1.0
 * @CreateDate : 2020/11/11 15:16
 * @Description :
 */

import java.io.IOException;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.compress.BZip2Codec;
import org.apache.hadoop.io.compress.DefaultCodec;
import org.apache.hadoop.io.compress.GzipCodec;
import org.apache.hadoop.io.compress.Lz4Codec;
import org.apache.hadoop.io.compress.SnappyCodec;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

public class WordCountDriver {

    public static void main(String[] args) throws IOException, ClassNotFoundException, InterruptedException {

        Configuration configuration = new Configuration();

        Job job = Job.getInstance(configuration);

        job.setJarByClass(WordCountDriver.class);

        job.setMapperClass(McMapper.class);
        job.setReducerClass(McReducer.class);

        job.setMapOutputKeyClass(Text.class);
        job.setMapOutputValueClass(IntWritable.class);

        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(IntWritable.class);

        FileInputFormat.setInputPaths(job, new Path(args[0]));
        FileOutputFormat.setOutputPath(job, new Path(args[1]));

        // 设置reduce端输出压缩开启
        FileOutputFormat.setCompressOutput(job, true);

        // 设置压缩的方式
        FileOutputFormat.setOutputCompressorClass(job, Lz4Codec.class);
//	    FileOutputFormat.setOutputCompressorClass(job, GzipCodec.class);
//	    FileOutputFormat.setOutputCompressorClass(job, DefaultCodec.class);

        boolean result = job.waitForCompletion(true);

        System.exit(result?1:0);
    }
}