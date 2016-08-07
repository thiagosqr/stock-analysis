package com.github.thiagosqr;

import org.apache.spark.SparkConf;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SaveMode;
import org.apache.spark.sql.SparkSession;

import java.util.Properties;

import static org.apache.spark.sql.functions.explode;

/**
 * Created by thiago on 8/6/16.
 */
public class SyncStocks {

    private static final String CONNECTION_URL = "jdbc:postgresql://localhost:5432/postgres";
    private static final Properties p = new Properties();

    private static final SparkConf sc = new SparkConf().setAppName("SparkSaveToDb").setMaster("local[*]");

    private static final SparkSession spark = SparkSession
            .builder()
            .appName("Java Spark SQL Example")
            .config(sc)
            .getOrCreate();

    public static void main(String args[]){

        p.setProperty("username","thiago");
        p.setProperty("password","");

        final Dataset<Row> df = spark.read()
                .json("src/main/resources/stocks.json")
                .select("query.results.quote");

        final Dataset<Row> q = df.select(
                df.col("quote"),
                explode(df.col("quote")).as("quotes")
        ).select("quotes");

        q.show();
        q.printSchema();

        final Dataset<Row> quotes = q.select(q.col("quotes.Bid"), q.col("quotes.Symbol"));

        quotes.registerTempTable("quotes");
        quotes.write().mode(SaveMode.Overwrite).jdbc(CONNECTION_URL, "quotes", p);

    }
}
