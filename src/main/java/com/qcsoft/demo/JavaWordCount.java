package com.qcsoft.demo;

import java.util.Arrays;
import java.util.List;
import java.util.regex.Pattern;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.sql.SparkSession;

import scala.Tuple2;

public final class JavaWordCount {
  private static final Pattern SPACE = Pattern.compile(" ");

  public static void main(String[] args) throws Exception {

	String textFile = "/tmp/abc.log";

	SparkConf conf = new SparkConf()
	.setAppName("JavaWordCount")
	.setMaster("spark://cloud.qcsoft.com.cn:7077")
	.set("spark.executor.memory", "450m")
	.set("spark.driver.memory", "200m");
	
	
    SparkSession spark = SparkSession
      .builder()
      .config(conf)
      .getOrCreate();

    JavaRDD<String> lines = spark.read().textFile(textFile).javaRDD();

    JavaRDD<String> words = lines.flatMap(s -> Arrays.asList(SPACE.split(s)).iterator());

    JavaPairRDD<String, Integer> ones = words.mapToPair(s -> new Tuple2<>(s, 1));

    JavaPairRDD<String, Integer> counts = ones.reduceByKey((i1, i2) -> i1 + i2);

    List<Tuple2<String, Integer>> output = counts.collect();
    for (Tuple2<?,?> tuple : output) {
      System.out.println("\r\n###"+tuple._1() + ": " + tuple._2()+"\r\n");
    }
    spark.stop();
    
  }
}
