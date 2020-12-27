package com.atguigu.onlypartitioner;


import com.atguigu.flow.FlowBean;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Partitioner;

/**
 * @Author : dky
 * @Version : 1.0
 * @CreateDate : 2020/7/18 13:50
 * @Description :
 */
//Text,FlowBean 这里是Map的输出  分区在Map后面
public class MyPartitioner extends Partitioner<Text, FlowBean> {

    /**
     * 返回分区号的方法
     * @param text
     * @param flowBean
     * @param i
     * @return
     */
    @Override
    public int getPartition(Text text, FlowBean flowBean, int i) {

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
