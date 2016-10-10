package org.hy.mhadoop.mission12.mysqlcontrol;

import java.io.IOException;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

public class ConnMysqlMapper1 extends Mapper<LongWritable, Text, Text, Text> {

	public void map(LongWritable key, Text value, Context context)
			throws IOException, InterruptedException {
		// 读取 hdfs 中的数据
		String[] splits = value.toString().split("\t");
		context.write(new Text(splits[0]), new Text(splits[1] + "\t" + splits[2]));
	}
}
