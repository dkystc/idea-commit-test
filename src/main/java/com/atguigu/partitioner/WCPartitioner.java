package com.atguigu.partitioner;

import com.atguigu.writablecomparable.FlowBean;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Partitioner;

/**
 * @Author : dky
 * @Version : 1.0
 * @CreateDate : 2020/7/18 11:46
 * @Description :
 */
//这里的泛型 由map方法输出的kv类型决定  因为map方法结束后会输出kv对 进入shuffle阶段 会进入环形缓冲区
    //然后进行 二次排序     1.分区 2.对key快排
    //这里是管分区的 所以 是接在map方法后面
public class WCPartitioner extends Partitioner<FlowBean, Text> {

    /**
     * 根据手机号分区
     * @param flowBean 流量
     * @param text 手机号
     * @param i 分区数量
     * @return
     */
    @Override
    public int getPartition(FlowBean flowBean, Text text, int i) {
        String head = text.toString().substring(0, 3);
        String[] split = head.split("\t");
        String phoneNum = split[0];
        int partition = 0;//代表分区号
        switch (phoneNum){
            case "136":
                partition = 1;
                break;
            case "137":
                partition = 2;
                break;
            case "138":
                partition = 3;
                break;
            case "139":
                partition = 4;
                break;
        }
        return partition;
    }
}
