package com.zhuinden.sparkexperiment;

import static org.apache.spark.sql.functions.col;

import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.function.Function;
import java.util.stream.Collectors;

import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.RelationalGroupedDataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Component;

import com.mongodb.spark.MongoSpark;
import com.mongodb.spark.config.ReadConfig;

@Component
public class MongoDB {
	 @Autowired
	 private SparkSession sparkSession;
	 
	 @Autowired
	 private JavaSparkContext jsc;
	 
	 public long count(String DB,String Collection) {
	       Dataset<Row> mongoDataset =  getDB(jsc, DB, Collection);
	       return mongoDataset.count();
	    }
	 private static Dataset<Row> getDB(JavaSparkContext jsc_, String DB, String Coll) {

			// Create a custom ReadConfig
			System.out.println("/****************** Reading Databas"+ DB + " and "+Coll+"******************/");
			System.out.println("Reading Database "+ DB + " and "+Coll);
			Map<String, String> readOverrides = new HashMap<String, String>();
			readOverrides.put("database",DB );
			readOverrides.put("collection", Coll);
			readOverrides.put("readPreference.name", "secondaryPreferred");
			
			System.out.println(readOverrides);
			ReadConfig readConfig = ReadConfig.create(jsc_).withOptions(readOverrides);
			
			Dataset<Row> DS = MongoSpark.load(jsc_,readConfig).toDF().drop("_id").dropDuplicates();
			DS.show(1,false);
			System.out.println();
			 
			System.out.println("/************************************/");
			 
			return DS;
			   
		}
	 
}
