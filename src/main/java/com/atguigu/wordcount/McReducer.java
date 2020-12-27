package com.atguigu.wordcount;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;

/**
 * @Author : dky
 * @Version : 1.0
 * @CreateDate : 2020/7/17 11:14
 * @Description :
 */
public class McReducer extends Reducer<Text, IntWritable,Text,IntWritable> {
    private IntWritable result = new IntWritable();


    /**
     * 合并方法 将map输出的数据按一定的规则合并
     * @param key 要输出到集群或者本地文件的单词
     * @param values 对应这个单词所有的1
     * @param context 任务环境 调度Reducer
     * @throws IOException
     * @throws InterruptedException
     */
    @Override
    protected void reduce(Text key, Iterable<IntWritable> values, Context context) throws IOException, InterruptedException {
        //累加
        int sum = 0;
        for (IntWritable value : values) {
            sum += value.get();
        }
        result.set(sum);
        context.write(key, result);
    }
}
