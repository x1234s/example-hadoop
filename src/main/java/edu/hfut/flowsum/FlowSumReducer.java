package edu.hfut.flowsum;

import org.apache.hadoop.mapreduce.Reducer;

import javax.xml.soap.Text;
import java.io.IOException;

public class FlowSumReducer extends Reducer<Text, FlowBean, Text, FlowBean> {
    FlowBean v = new FlowBean();
    @Override
    protected void reduce(Text key, Iterable<FlowBean> values, Context context) throws IOException, InterruptedException {
        long upFlowCount = 0;
        long downFlowCount = 0;

        for (FlowBean bean :
                values) {
            upFlowCount += bean.getUpflow();
            downFlowCount += bean.getDownflow();
        }

        v.set(upFlowCount, downFlowCount);
        context.write(key, v);
    }
}
