package edu.hfut.hadoop;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

import java.io.IOException;

public class WordCountDriver {
    public static void main(String[] args) throws IOException, ClassNotFoundException, InterruptedException {
        // 通过job分装本地mr信息
        Configuration conf = new Configuration();
        // 设置本地运行模式，可不放入hadoop集群中运行
//         conf.set("mapreduce.framework.name", "local");

        Job job = Job.getInstance(conf);
        // 指定本次mr job jar运行主类
        job.setJarByClass(WordCountDriver.class);

        // 设置本次mr  所用的mapper  reducer类分别是什么
        job.setMapperClass(WordCountMapper.class);
        job.setReducerClass(WordCountReducer.class);

        // 设置本次mr mapper阶段的输出 k  v 类型
        job.setMapOutputKeyClass(Text.class);
        job.setMapOutputValueClass(IntWritable.class);

        // 设置本次mr 最后输出的k  v类型
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(IntWritable.class);

        // 本地模式需要配置
//         FileInputFormat.setInputPaths(job, "D:\\wordcount\\input");
//         FileOutputFormat.setOutputPath(job, new Path("D:\\wordcount\\output"));
        FileInputFormat.setInputPaths(job, "/wordcount/input");
        FileOutputFormat.setOutputPath(job, new Path("/wordcount/output"));

        boolean b = job.waitForCompletion(true);
        System.exit(b ? 0 : 1);
    }
}
