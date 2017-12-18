package com.qcsoft.spark;

import java.util.Properties;

import org.apache.log4j.Logger;
import org.apache.spark.sql.DataFrameReader;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SQLContext;
import org.apache.spark.sql.SparkSession;

public class App {

	private static Logger logger = Logger.getLogger(App.class);

	public static void main(String[] args) {

		String master = "spark://at2100.com:7077";
		

		SparkSession sparkSession = SparkSession
				  .builder()
			      .master(master)
			      .appName("spark session example")
			      //.enableHiveSupport()
			      .getOrCreate();

		//sparkSession.conf().set("spark.sql.shuffle.partitions", 6);
		//sparkSession.conf().set("spark.executor.memory", "1g");
		
		try {
			SQLContext sqlContext = new SQLContext(sparkSession);

			// 一个条件表示一个分区
			String[] predicates = new String[] { 
					"1=1 order by id limit 1,10",
					"1=1 order by id limit 11,20",
					"1=1 order by id limit 21,30",
					"1=1 order by id limit 31,40"};

			String url = "jdbc:mysql://192.168.1.157:3306/zhmall?characterEncoding=utf8&useUnicode=true";
			String table = "bm";
			Properties connectionProperties = new Properties();
			connectionProperties.setProperty("dbtable", table);// 设置表
			connectionProperties.setProperty("user", "root");// 设置用户名
			connectionProperties.setProperty("password", "1234");// 设置密码
			connectionProperties.setProperty("driver", "com.mysql.jdbc.Driver");

			// 读取数据
			DataFrameReader jread = sqlContext.read();
			Dataset<Row> jdbcDs = jread.jdbc(url, table, predicates, connectionProperties);


			jdbcDs.printSchema();
			long c = jdbcDs.count();
			System.out.println(c);

			// 写入数据
			//String url2 = "jdbc:mysql://192.168.1.157:3306/test?characterEncoding=utf8&useUnicode=true";
			//Properties connectionProperties2 = new Properties();
			//connectionProperties2.setProperty("user", "root");// 设置用户名
			//connectionProperties2.setProperty("password", "1234");// 设置密码
			//connectionProperties2.setProperty("driver", "com.mysql.jdbc.Driver");
			//String table2 = "bm";

			// SaveMode.Append表示添加的模式
			//jdbcDs.write().mode(SaveMode.Append).jdbc(url2, table2, connectionProperties2);
			
			
		} catch (Exception e) {
			logger.error("exception error", e);
		} finally {
			if (sparkSession != null) {
				sparkSession.cloneSession();
			}

		}

	}
}