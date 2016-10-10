package org.hy.mhadoop.mission12.mysqlcontrol;

import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.db.DBConfiguration;
import org.apache.hadoop.mapreduce.lib.db.DBOutputFormat;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;

public class WriteDataToMysql {
	public static void main(String args[]) throws IOException,
			InterruptedException, ClassNotFoundException {
		Configuration conf = new Configuration();
		// 配置 JDBC 驱动、数据源和数据库访问的用户名和密码
		DBConfiguration.configureDB(conf, "com.mysql.jdbc.Driver",
				"jdbc:mysql://192.168.80.128:3306/myhadoop",
				"root", "root");
		Job job = Job.getInstance(conf, "test mysql connection");// 新建一个任务
		job.setJarByClass(WriteDataToMysql.class);// 主类

		job.setMapperClass(ConnMysqlMapper1.class); // Mapper
		job.setReducerClass(ConnMysqlReducer1.class); // Reducer

		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(Text.class);

		job.setInputFormatClass(TextInputFormat.class);
		job.setOutputFormatClass(DBOutputFormat.class);// 向数据库写数据
		// 输入路径
		FileInputFormat.addInputPath(job, new Path("hdfs://hy:9000/mission/12-mysql/data.txt"));
		// 设置输出到数据库 表名：user 字段：uid、email、name
		DBOutputFormat.setOutput(job, "user", "uid", "email", "name");
		System.exit(job.waitForCompletion(true) ? 0 : 1);
	}

}
