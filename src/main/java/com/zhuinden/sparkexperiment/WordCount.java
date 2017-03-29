package com.zhuinden.sparkexperiment;


import org.apache.spark.SparkContext;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.sql.*;
import org.apache.spark.sql.types.DataType;
import org.apache.spark.sql.types.StructField;
import org.apache.spark.sql.types.StructType;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.stereotype.Component;
import scala.Tuple2;
import scala.collection.JavaConversions;
import scala.collection.Seq;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.List;
import java.util.function.Function;
import java.util.stream.Collectors;
import java.util.stream.Stream;
import static org.apache.spark.sql.functions.col;
/**
 * Created by achat1 on 9/23/15.
 * Just an example to see if it works.
 */
@Component
public class WordCount {
    @Autowired
    private SparkSession sparkSession;

    public List<Count> count() {
        String input = "hello world hello hello hello";
        String[] _words = input.split(" ");
        List<Word> words = Arrays.stream(_words).map(Word::new).collect(Collectors.toList());
        Dataset<Row> dataFrame = sparkSession.createDataFrame(words, Word.class);
        dataFrame.show();
        //StructType structType = dataFrame.schema();

        RelationalGroupedDataset groupedDataset = dataFrame.groupBy(col("word"));
        groupedDataset.count().show();
        List<Row> rows = groupedDataset.count().collectAsList();//JavaConversions.asScalaBuffer(words)).count();
        return rows.stream().map(new Function<Row, Count>() {
            @Override
            public Count apply(Row row) {
                return new Count(row.getString(0), row.getLong(1));
            }
        }).collect(Collectors.toList());
    }
}