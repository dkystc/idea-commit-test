package com.atguigu.writablecomparable;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;

/**
 * @Author : dky
 * @Version : 1.0
 * @CreateDate : 2020/7/18 11:17
 * @Description :
 */
public class WCMapper extends Mapper<LongWritable, Text, FlowBean, Text> {
    private FlowBean flowBean = new FlowBean();
    private Text phoneNum = new Text();

    @Override
    protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
        //value就是一行数据
        String line = value.toString();
        String[] split = line.split("\t");
        phoneNum.set(split[0]);

        flowBean.set(Long.parseLong(split[1]), Long.parseLong(split[2]));
        context.write(flowBean, phoneNum);
    }
}
