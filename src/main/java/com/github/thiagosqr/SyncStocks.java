package com.github.thiagosqr;

import com.google.common.base.Preconditions;
import javaslang.control.Try;
import org.apache.spark.SparkConf;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SaveMode;
import org.apache.spark.sql.SparkSession;
import org.glassfish.jersey.client.rx.RxInvocationBuilder;
import org.glassfish.jersey.client.rx.rxjava.RxObservable;
import org.glassfish.jersey.client.rx.rxjava.RxObservableInvoker;
import rx.Observable;

import javax.ws.rs.client.ClientBuilder;
import javax.ws.rs.client.WebTarget;
import javax.ws.rs.core.Response;
import java.io.*;
import java.util.Properties;
import java.util.concurrent.TimeUnit;

import static org.apache.spark.sql.functions.explode;

/**
 * Created by thiago on 8/6/16.
 */
public class SyncStocks {

    private static final Properties p = new Properties();

    private static final InputStream is = SyncStocks.class.getClassLoader().getResourceAsStream("jdbc.properties");

    private static final SparkConf sc = new SparkConf().setAppName("StockAnalysis").setMaster("local[*]");
    private static final SparkSession spark = SparkSession
            .builder()
            .config(sc)
            .getOrCreate();

    public static void main(String args[]){

        try {
            p.load(is);

            final File temp = File.createTempFile("stocks", ".json");
            final BufferedWriter bw = new BufferedWriter(new FileWriter(temp));

            final WebTarget stocksAPI = ClientBuilder.newClient().target("http://query.yahooapis.com")
                    .path("v1/public/yql")
                    .queryParam("q","select * from yahoo.finance.quotes where symbol in ('YHOO','AAPL','GOOG','MSFT')")
                    .queryParam("env","http://datatables.org/alltables.env")
                    .queryParam("format","json");

            final RxInvocationBuilder<RxObservableInvoker> rxb = RxObservable.from(stocksAPI).request();

            final Observable<Response> stocks = rxb
                    // Reactive invoker.
                    .rx()
                    // Return a list of destinations.
                    .get(Response.class)
                    .timeout(30, TimeUnit.SECONDS);

            stocks.subscribe(r -> {

                Preconditions.checkArgument(r.getStatus() < 300);
                final String json = r.readEntity(String.class).replaceAll("\"symbol\"","\"symbol_\"");
                try{ bw.write(json); }catch (Exception e){e.printStackTrace();}

            },
            e -> {

                e.printStackTrace();
                System.exit(1);
            },
            () -> {

                try { bw.close(); } catch (IOException e) { e.printStackTrace(); }
                process(temp);

            });

        } catch (Exception e) {
            e.printStackTrace();
            System.exit(1);
        }

    }

    private static void process(final File temp) {

        final Dataset<Row> df = spark.read()
//                    .json("src/main/resources/stocks.json")
                .json(temp.getAbsolutePath())
                .select("query.results.quote");

        final Dataset<Row> q = df.select(
                df.col("quote"),
                explode(df.col("quote")).as("quotes")
        ).select("quotes");

        q.show();
        q.printSchema();

        final Dataset<Row> quotes = q.select(q.col("quotes.Bid"), q.col("quotes.Symbol"));

        quotes.registerTempTable("quotes");
        quotes.write().mode(SaveMode.Overwrite).jdbc(p.getProperty("url"), "quotes", p);

    }
}
