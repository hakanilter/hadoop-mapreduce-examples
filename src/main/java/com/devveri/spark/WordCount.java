package com.devveri.spark;

import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.FlatMapFunction;
import org.apache.spark.api.java.function.Function2;
import org.apache.spark.api.java.function.PairFunction;
import scala.Tuple2;

import java.util.Arrays;
import java.util.regex.Pattern;

/**
 * Created by hilter on 11/05/14.
 */
public class WordCount {

    private static final String MASTER = "spark://localhost";

    private static final Pattern SPACE = Pattern.compile(" ");

    public static void main(String[] args) {
        // create context
        JavaSparkContext jsc = new JavaSparkContext(
                MASTER,
                "WordCount",
                System.getenv("SPARK_HOME"),
                JavaSparkContext.jarOfClass(WordCount.class)
        );

        // load file
        JavaRDD<String> file = jsc.textFile("/Users/devveri/test.txt");

        // split words
        JavaRDD<String> words = file.flatMap(new FlatMapFunction<String, String>() {
            public Iterable<String> call(String s) { return Arrays.asList(s.split(" ")); }
        });

        // create pairs
        JavaPairRDD<String, Integer> pairs = words.map(new PairFunction<String, String, Integer>() {
            public Tuple2<String, Integer> call(String s) { return new Tuple2<String, Integer>(s, 1); }
        });

        // count
        JavaPairRDD<String, Integer> counts = pairs.reduceByKey(new Function2<Integer, Integer, Integer>() {
            public Integer call(Integer a, Integer b) throws Exception { return a + b; }
        });

        // save result
        counts.saveAsTextFile("/tmp/result");
    }

}
