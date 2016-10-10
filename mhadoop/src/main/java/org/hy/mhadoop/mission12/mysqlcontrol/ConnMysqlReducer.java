package org.hy.mhadoop.mission12.mysqlcontrol;

import java.io.IOException;
import java.util.Iterator;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

public class ConnMysqlReducer extends Reducer<Text, Text, Text, Text>{
	@Override
	protected void reduce(Text key, Iterable<Text> values, Context context)
			throws IOException, InterruptedException {
		// The output of data to a
		for(Iterator<Text> itr = values.iterator(); itr.hasNext();){
			context.write(key, itr.next());
		}
	}

}
