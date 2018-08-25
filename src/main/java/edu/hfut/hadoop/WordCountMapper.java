package edu.hfut.hadoop;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;

public class WordCountMapper extends Mapper<LongWritable, Text, Text, IntWritable> {
    /**
     *
     * @param key
     * @param value
     * @param context
     * @throws IOException
     * @throws InterruptedException
     */
    @Override
    protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
        // super.map(key, value, context);
        // 拿到传入的一行内容，把数据类型转换成String
        String line = value.toString();

        // 姜哲一行内容切分成各个单词
        String[] words = line.split(" ");

        // 遍历数组，每出现一个单词就标记一个数字1
        for (String word : words) {
            // 使用mr的上下文的context把mapper阶段的数据发送出去，作为reduce节点的输入数据
            context.write(new Text(word), new IntWritable(1));
        }
    }
}
