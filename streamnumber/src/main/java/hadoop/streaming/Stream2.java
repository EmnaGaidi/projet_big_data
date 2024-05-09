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

public class Stream2 {
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

        // Parse the CSV-like format directly to extract relevant columns (artist, track_name, streams)
        Dataset<Row> df = lines.selectExpr("split(value, ',') as data")
                        .selectExpr("explode(split(data[1], ';')) as artist", 
                                    "cast(data[0] as string) as track_name",
                                    "cast(data[8] as int) as streams")
                        .filter(col("streams").rlike("\\d+"));

        // Group by 'artist' and find max 'streams', also get the corresponding 'track_name'
        Dataset<Row> maxStreamsPerArtist = df.groupBy("artist")
            .agg(max(struct(col("streams"), col("track_name"))).alias("max_streams"))
            .select(col("artist"), col("max_streams.streams").alias("max_streams"), col("max_streams.track_name").alias("track_name"));

        // Start running the query that prints the artist with max streams and the corresponding track name
        StreamingQuery query = maxStreamsPerArtist.writeStream()
            .outputMode("complete")
            .format("console")
            .trigger(Trigger.ProcessingTime("1 second"))
            .start();

        query.awaitTermination();
    }
}