package com.atguigu;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IOUtils;
import org.apache.hadoop.io.compress.CompressionCodec;
import org.apache.hadoop.io.compress.CompressionCodecFactory;
import org.apache.hadoop.io.compress.CompressionInputStream;
import org.apache.hadoop.io.compress.CompressionOutputStream;
import org.apache.hadoop.util.ReflectionUtils;

import java.io.*;

/**
 * @Author : dky
 * @Version : 1.0
 * @CreateDate : 2020/11/10 19:08
 * @Description :
 */
public class TestCompress {
    public static void main(String[] args) throws Exception {

        compress("d:/QzMemberPaperQuestion.log","org.apache.hadoop.io.compress.Lz4Codec");
       // decompress("d:/QzMemberPaperQuestion.log.gz");
    }

    private static void decompress(String file) throws Exception {
        long start = System.currentTimeMillis();
        //校验是否能解压缩  压缩方式检查
        CompressionCodecFactory factory = new CompressionCodecFactory(new Configuration());

        //这里是根据后缀名 压缩方式
        CompressionCodec codec = factory.getCodec(new Path(file));

        if (codec == null) {
            System.out.println("cannot find codec for file " + file);
            return;
        }

        //获取输入流
        //包装成 可以解压缩的输入流
        CompressionInputStream cis = codec.createInputStream(new FileInputStream(new File(file)));

        //获取输出流
        FileOutputStream fos = new FileOutputStream(file + ".decoded");

        //流对拷  是否拷贝完成之后关闭
        IOUtils.copyBytes(cis,fos,1024*1024*5,false);
        long end = System.currentTimeMillis();
        //关闭资源
        fos.close();
        cis.close();
        System.out.println("解压时间"+String.valueOf(end-start));

    }

    private static void compress(String file, String method) throws Exception {
        //获取输入流
        FileInputStream fis = new FileInputStream(new File(file));
        //获取 压缩方式的主类信息
        Class codecClass = Class.forName(method);
        //用反射 获取codec 是一个具体的压缩方式 是个接口
        CompressionCodec codec = (CompressionCodec) ReflectionUtils.newInstance(codecClass, new Configuration());

        //获取普通输出流 同时给要输出的文件加上 压缩之后的对应的扩展名
        FileOutputStream fos = new FileOutputStream(new File(file + codec.getDefaultExtension()));
        //因为现在是要 把数据压缩之后 写出去 所以要用压缩输出流进行包装
        //把普通的输出流 变成具有压缩格式的输出流
        CompressionOutputStream cos = codec.createOutputStream(fos);

        //流对拷
        IOUtils.copyBytes(fis,cos,1024*1024*5, false);

        //先开的后关
        cos.close();
        fos.close();
        fis.close();



    }
}
