package org.hy.mhadoop.mission12.mysqlcontrol;

import java.net.URI;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.lib.db.DBConfiguration;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.filecache.DistributedCache;
import org.apache.hadoop.mapreduce.lib.db.DBInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

public class ConnMysql {
	public static void main(String[] args) throws Exception {
		Configuration conf = new Configuration();
		Path output = new Path("hdfs://hy:9000/mission/12-mysql/out");
		
		FileSystem fs = FileSystem.get(URI.create(output.toString()), conf);
		if (fs.exists(output)){
			fs.delete(output, true);
		}
		
		//mysql的jdbc驱动
		DistributedCache.addFileToClassPath(new Path("hdfs://hy:9000/advance/jar/mysql-connector-java-5.1.14.jar"), conf);
		
		//设置mysql配置信息   4个参数分别为： Configuration对象、mysql数据库地址、用户名、密码
		DBConfiguration.configureDB(conf, "com.mysql.jdbc.Driver", "jdbc:mysql://192.168.80.128:3306/myhadoop", "root", "root");
		
		Job job = Job.getInstance(conf, "test mysql connection");
		job.setJarByClass(ConnMysql.class);
		
		job.setMapperClass(ConnMysqlMapper.class);
		job.setReducerClass(ConnMysqlReducer.class);
		
		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(Text.class);
		
		job.setInputFormatClass(DBInputFormat.class);
		FileOutputFormat.setOutputPath(job, output);
		
		//column name
		String[] fields = {"uid", "email", "name"};
		//六个参数分别为：
        //1.Job;2.Class< extends DBWritable> 3.表名;4.where条件 5.order by语句;6.列名
		DBInputFormat.setInput(job, UserRecord.class, "user", null, null, fields);
		System.exit(job.waitForCompletion(true)?0:1);
		
	}

}
