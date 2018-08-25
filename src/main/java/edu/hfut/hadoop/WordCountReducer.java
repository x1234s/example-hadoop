package edu.hfut.hadoop;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;

public class WordCountReducer extends Reducer<Text, IntWritable, Text, IntWritable> {
    @Override
    protected void reduce(Text key, Iterable<IntWritable> values, Context context) throws IOException, InterruptedException {
        // 定义一个计数器
        int count = 0;
        // 遍历一组迭代器，把每一组1累加起来构成了单词的总次数
        for (IntWritable value :
                values) {
            count += value.get();
        }
        // 把最终的结果输出
        context.write(key, new IntWritable(count));
    }


}
