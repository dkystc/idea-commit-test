package com.atguigu.wordcount;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;

/**
 * @Author : dky
 * @Version : 1.0
 * @CreateDate : 2020/7/17 10:26
 * @Description :
 */
public class McMapper extends Mapper<LongWritable, Text,Text,IntWritable> {

    //定义要输出的key
    private Text word = new Text();

    //定义要输出的v 因为一直都是1 所以不用变
    private  IntWritable one = new IntWritable(1);
    /**
     * 将输入的KV对变成 (word,1)  (word,1) 是个临时的变量 后面会进行合并处理
     * @param key  表示从文件中读取数据的偏移量(也就是这一行数据在文件中的位置) 用来告诉程序下一次在哪里继续读取下一行的内容
     * @param value  行内容
     * @param context 负责调度Mapper运行  MapReduce框架的运行环境 有关MR框架的所有东西 都能从context获取
     * @throws IOException
     * @throws InterruptedException
     */
    @Override
    protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
        //拆分这一行的数据 按空格
        String[] words = value.toString().split(" ");

        //数据一开始是从框架来  然后我们在这一步输出到框架
        for (String word : words) {
            this.word.set(word);
            //这里可以把context理解为任务本身
            //把切出来的每个单词 写成kv 键值对
        context.write(this.word,this.one);

        }
    }
}
