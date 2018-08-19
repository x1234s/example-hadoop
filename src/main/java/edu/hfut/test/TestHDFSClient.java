package edu.hfut.test;

import io.vavr.control.Try;
import org.apache.commons.io.IOUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;

import java.io.FileInputStream;
import java.net.URI;

public class TestHDFSClient {

    public static void main(String[] args) {

        Try<Void> of = Try.of(() -> {

            // 配置文件
            Configuration conf = new Configuration();
            // 这里指定使用hdfs文件系统
            conf.set("fs.defaultFS", "hdfs://192.168.255.129:9000");

            // 通过这种方式指定java客户端访问hdfs的身份
            System.setProperty("HADOOP_USER_NAME", "root");

            FileSystem fs = FileSystem.get(conf);
            // FileSystem.get(new URI("hdfs://192.168.255.129:9000"), conf,"root");

            // 创建
//            fs.create(new Path("/createByJava"));

            // 文件下载到本地(需要本地平台hadoop的支持)
//            fs.copyToLocalFile(new Path("/createByJava"), new Path("D:/filepath"));

            // 使用stream形式，操作HDFS更底层的方式
            FSDataOutputStream outputStream = fs.create(new Path("/1.txt"), true);

            FileInputStream inputStream = new FileInputStream("D:\\1.txt");

            IOUtils.copy(inputStream, outputStream);

            return null;
        });


    }

}
