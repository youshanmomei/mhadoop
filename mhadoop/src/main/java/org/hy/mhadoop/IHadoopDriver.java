package org.hy.mhadoop;

import org.apache.hadoop.util.ProgramDriver;
import org.hy.mhadoop.mission11.tvratings.RatingCount;
import org.hy.mhadoop.mission12.mysqlcontrol.ConnMysql;

public class IHadoopDriver {

	public static void main(String[] args) {
		int exitCode = -1;
	    ProgramDriver pgd = new ProgramDriver();
	    try {
	      pgd.addClass("wordcount", WordCount.class, 
	                   "A map/reduce program that counts the words in the input files.");
	      pgd.addClass("wordcount2", WordCount2.class, 
	    		  "A map/reduce program that counts the words in the input files use the type2.");
	      pgd.addClass("tvratings", RatingCount.class, "a tv ratings count");
	      pgd.addClass("test_mysql", ConnMysql.class, "test conn to mysql");
	      exitCode = pgd.run(args);
	    }
	    catch(Throwable e){
	      e.printStackTrace();
	    }
	    
	    System.exit(exitCode);
	}
}
