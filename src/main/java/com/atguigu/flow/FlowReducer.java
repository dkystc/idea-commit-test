package com.atguigu.flow;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;

/**
 * @Author : dky
 * @Version : 1.0
 * @CreateDate : 2020/7/17 16:53
 * @Description :
 */
public class FlowReducer extends Reducer<Text,FlowBean,Text,FlowBean> {
    private FlowBean resultBean = new FlowBean();
    @Override
    protected void reduce(Text key, Iterable<FlowBean> values, Context context) throws IOException, InterruptedException {
        long sum_upFlow = 0;
        long sum_downFlow = 0;

        // 1 这里是对同一个手机号(key)进行遍历叠加他的各个属性值，将其中的上行流量，下行流量分别累加
        for (FlowBean flowBean : values) {
            sum_upFlow += flowBean.getUpFlow();
            sum_downFlow += flowBean.getDownFlow();
        }

        // 2 封装对象

        resultBean.SetFlowBean(sum_upFlow, sum_downFlow);

        // 3 写出
        //这里的key是手机号  value是这个手机号对应的FlowBean对象
        context.write(key, resultBean);
    }
}
