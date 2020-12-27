package com.atguigu.writablecomparable;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;

/**
 * @Author : dky
 * @Version : 1.0
 * @CreateDate : 2020/7/18 11:22
 * @Description :
 */
//因为我们这里想让 手机号显示在前面 所以这里要让Text作为key
public class WCReducer extends Reducer<FlowBean, Text,Text,FlowBean> {
    /**
     * 已经排完序了，将KV调换顺序即可
     * @param key
     * @param values
     * @param context
     * @throws IOException
     * @throws InterruptedException
     */
    @Override
    protected void reduce(FlowBean key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
        //每一个流量对象 对应一个手机号
        for (Text value : values) {
            context.write(value, key);
        }
    }
}
