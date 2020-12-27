package com.atguigu.newwordcount;

import org.apache.hadoop.util.Tool;

import java.io.Serializable;

/**
 * @Author : dky
 * @Version : 1.0
 * @CreateDate : 2020/7/21 20:04
 * @Description :
 */
public class ToolFactory {
    public static Tool newTool(String name) {
        Tool tool = null;
        switch (name){
            case "wordcount":
                tool = new McDriver();
                break;
                //这里可以用不同Driver 所以扩展性比较好
        }
        return tool;

    }
}
