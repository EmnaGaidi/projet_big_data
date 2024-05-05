package kafkastream;

import org.apache.spark.sql.*;
import org.apache.spark.sql.streaming.StreamingQuery;
import org.apache.spark.sql.streaming.Trigger;

import static org.apache.spark.sql.functions.*;


public class ConsumerSpark {

    public static void main(String[] args) throws Exception {

        if (args.length < 3) {
            System.err.println("Usage: SparkKafkaMaxStreamsPerArtist <bootstrap-servers> <subscribe-topics> <group-id>");
            System.exit(1);
        }

        String bootstrapServers = args[0];
        String topics = args[1];
        String groupId = args[2];

        SparkSession spark = SparkSession
                .builder()
                .appName("SparkKafkaMaxStreamsPerArtist")
                .master("local[*]")  // Set the master URL for local mode
                .getOrCreate();

        // Create DataFrame representing the stream of input lines from Kafka
        Dataset<Row> df = spark
                .readStream()
                .format("kafka")
                .option("kafka.bootstrap.servers", bootstrapServers)
                .option("subscribe", topics)
                .option("kafka.group.id", groupId)
                .load()
                .selectExpr("CAST(value AS STRING)")
                .as(Encoders.STRING())
                .selectExpr("split(value, ',') as data")
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
