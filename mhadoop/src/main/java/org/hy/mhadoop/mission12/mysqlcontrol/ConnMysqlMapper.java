package org.hy.mhadoop.mission12.mysqlcontrol;

import java.io.IOException;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

public class ConnMysqlMapper extends Mapper<LongWritable, UserRecord, Text, Text>{
	
	@Override
	protected void map(LongWritable key, UserRecord values, Context context)
			throws IOException, InterruptedException {
		//from mysql reading need to field
		context.write(new Text(values.uid+""), new Text(values.name+" "+values.email));
	}

}
