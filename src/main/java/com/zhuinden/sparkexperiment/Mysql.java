package com.zhuinden.sparkexperiment;

import java.util.List;

import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Encoders;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Component;

@Component
public class Mysql {

	 @Autowired
	 private SparkSession sparkSession;
	 
	 @Autowired
	 private JavaSparkContext jsc;
	 
	 public List<String> getTableNames(String JDBC_URI,String JDBC_username,String JDBC_password) {
		 List<String> TablesList =  mysql_getTablesName(JDBC_URI, JDBC_username, JDBC_password);
	       return TablesList;
	    }
	 
	 public List<String> mysql_getTablesName(String uri,String username,String password) {
	    	String DB = uri.substring(uri.lastIndexOf("/") + 1);
	    	 
	    	String dbQuery = "(SELECT table_name FROM information_schema.tables WHERE table_schema = '"+DB+"') tmp";
	    		Dataset<Row> mysql =
	    				 sparkSession.read().format("jdbc")
						.option("url", uri)
						.option("user", username)
						.option("password", password)
						.option("dbtable",dbQuery )
						.load();
		    			 
		    	 System.out.println("Total Tables "+mysql.count());
		    	 List<String> list_tables  = mysql.select("table_name").map(row -> row.mkString(), Encoders.STRING()).collectAsList();
		    	  return list_tables; 
	    }
}
