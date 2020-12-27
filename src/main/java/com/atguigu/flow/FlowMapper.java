package com.atguigu.flow;
import java.io.IOException;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;

/**
 * @Author : dky
 * @Version : 1.0
 * @CreateDate : 2020/7/17 15:09
 * @Description :
 */
public class FlowMapper extends Mapper<LongWritable,Text,Text,FlowBean> {
    //在这里声明出 要给到reduce的kv对  这里一般都这么写
    //reduce里面只在这里声明代表要输出的v对象 因为key 是由reduce方法中的参数给出 对应的手机号就是key
    private Text k = new Text();//手机号
    private FlowBean v = new FlowBean();//手机号对应的 等等属性封装好的对象

    @Override
    protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
        // 1 获取一行数据
        String line = value.toString();

        // 2 切割字段
        String[] fields = line.split("\t");

        // 3 封装对象
        // 取出手机号码
        String phoneNum = fields[1];

        // 取出上行流量和下行流量
        long upFlow = Long.parseLong(fields[fields.length - 3]);
        long downFlow = Long.parseLong(fields[fields.length - 2]);

        k.set(phoneNum);
        v.SetFlowBean(downFlow, upFlow);

        // 4 写出
        context.write(k, v);
    }
}
