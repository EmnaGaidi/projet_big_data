package hadoop.streaming;

import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Encoders;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.streaming.StreamingQuery;
import org.apache.spark.sql.streaming.StreamingQueryException;
import org.apache.spark.sql.streaming.Trigger;

import static org.apache.spark.sql.functions.*;

import java.util.concurrent.TimeoutException;

public class Stream {
    public static void main(String[] args) throws StreamingQueryException, TimeoutException {
        SparkSession spark = SparkSession
            .builder()
            .appName("StreamExample")
            .master("local[*]")
            .getOrCreate();

        // Create DataFrame representing the stream of input lines from connection to localhost:9999
        Dataset<String> lines = spark
            .readStream()
            .format("socket")
            .option("host", "localhost")
            .option("port", 9999)
            .load()
            .as(Encoders.STRING());

        // Parse the CSV-like format directly to extract the release year and streams columns
        /* 
        Dataset<Row> df = lines.selectExpr("split(value, ',') as data")
                                .selectExpr("cast(data[3] as int) as released_year",
                                            "cast(data[8] as int) as streams");*/
        Dataset<Row> df = lines.selectExpr("split(value, ',') as data")
                        .filter(col("data").getItem(3).rlike("\\b\\d{4}\\b")) // Filtre pour un entier de 4 chiffres
                        .selectExpr("cast(data[3] as int) as released_year", "data[8] as streams")
                        .filter(col("streams").rlike("\\d+"))
                        .selectExpr("released_year", "cast(streams as int) as streams");


        // Group by 'released_year' and sum 'streams'
        Dataset<Row> streamsPerYear = df.groupBy("released_year").agg(sum("streams").alias("total_streams"));

        // Start running the query that prints the total streams per year to the console
        StreamingQuery query = streamsPerYear.writeStream()
            .outputMode("complete")
            .format("console")
            .trigger(Trigger.ProcessingTime("1 second"))
            .start();

        query.awaitTermination();
    }
}
