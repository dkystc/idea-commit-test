package com.atguigu.newwordcount;

import org.apache.hadoop.conf.Configurable;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;
import org.junit.Test;

import java.util.Arrays;

/**
 * @Author : dky
 * @Version : 1.0
 * @CreateDate : 2020/7/21 20:02
 * @Description :
 */
public class TotalDriver {
    public static void main(String[] args) throws Exception {
        Tool tool = ToolFactory.newTool(args[0]);//多态
        //tool 经过第一个参数wordcount 得到为一个McDriver对象
        //好处是用一个driver 调用多个wordcount程序

        //这些配置放这里 代表所有任务的conf配置都会被改
        Configuration conf = new Configuration();
        conf.set("fs.defaultFS", "hdfs://hadoop102:8020");


        //表示任务往yarn提交
        conf.set("mapreduce.framework.name", "yarn");
        //指定Yarn resourcemanager的位置
        conf.set("yarn.resourcemanager.hostname", "hadoop103");
        //  指定mapreduce可以在远程集群运行
        conf.set("mapreduce.app-submission.cross-platform", "true");
        if (tool != null){

            //第三个参数就是个数组对象 除了wordcount 这个参数后面的所有参数 比如 声明提交到hive队列  输入路径 输出路径
            //我们这里就是动态的指定了(用命令行参数) 任务提交到哪种队列 默认队列或hive队列
            //以后 很少在代码中指定 不够灵活
            ToolRunner.run(conf,tool, Arrays.copyOfRange(args,1,args.length));
        }
    }

}
