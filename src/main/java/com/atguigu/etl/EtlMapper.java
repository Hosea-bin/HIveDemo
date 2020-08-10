package com.atguigu.etl;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;

public class EtlMapper extends Mapper<LongWritable, Text,Text, NullWritable> {
    Text outK = new Text();
    @Override
    protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
        //获取一条数据
        String srcData = value.toString();
        //清洗数据
        String data = EtlUtils.etlData(srcData);
        if(data==null){
            return ;
        }
        //写出
        outK.set(data);
        context.write(outK,NullWritable.get());
    }
}
