package edu.hfut.flowsum.sort;

import edu.hfut.flowsum.FlowBean;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;

/**
 * 在mr程序中也可以用自定义的类型作为mr的数据类型，前提是需要实现hadoop的序列化机制，writeable
 */
public class FlowSumSortMapper extends Mapper<LongWritable, Text, FlowBean, Text> {

    Text v = new Text();
    FlowBean k = new FlowBean();

    @Override
    protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
        String line = value.toString();
        String[] fields = line.split("\t");
        String phoneNum = fields[0];
        long upflow = Long.parseLong(fields[1]);
        long downflow = Long.parseLong(fields[2]);

        k.set(upflow, downflow);
        v.set(phoneNum);

        context.write(k, v);

    }
}
