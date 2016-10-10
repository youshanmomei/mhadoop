package org.hy.mhadoop.mission12.mysqlcontrol;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;

import org.apache.hadoop.io.Writable;
import org.apache.hadoop.mapred.lib.db.DBWritable;

public class UserRecord implements Writable, DBWritable{
	int uid;
	String email;
	String name;

	/**
	 * 
	 * @param email
	 * @param name
	 */
	public UserRecord(int uid, String email, String name) {
		this.uid = uid;
		this.email = email;
		this.name = name;
	}

	/**
	 * 读取数据库所需要的字段
	 */
	public void readFields(ResultSet rs) throws SQLException {
		this.uid = rs.getInt(1);
		this.email = rs.getString(2);
		this.name = rs.getString(3);
	}

	/**
	 * 向数据库写入数据
	 */
	public void write(PreparedStatement statement) throws SQLException {
		statement.setInt(1, this.uid);
		statement.setString(2, this.email);
		statement.setString(3, this.name);
	}

	/**
	 * 读取序列化数据
	 */
	public void readFields(DataInput in) throws IOException {
		this.uid = in.readInt();
		this.email = in.readUTF();//读取一个Unicode编码
		this.name = in.readUTF();
	}

	/**
	 * 将数据序列化
	 */
	public void write(DataOutput out) throws IOException {
		out.writeInt(uid);	
		out.writeUTF(email);
		out.writeUTF(name);
	}
	
	@Override
	public String toString() {
		return new String(this.uid+" "+this.email+" "+this.name);
	}

}
