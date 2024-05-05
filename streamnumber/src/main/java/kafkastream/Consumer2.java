package kafkastream;

import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.streaming.StreamingQuery;
import org.apache.spark.sql.streaming.Trigger;

import static org.apache.spark.sql.functions.*;

public class Consumer2 {

    public static void main(String[] args) throws Exception {

        if (args.length < 3) {
            System.err.println("Usage: ConsumerSpark <bootstrap-servers> <subscribe-topics> <group-id>");
            System.exit(1);
        }

        String bootstrapServers = args[0];
        String topics = args[1];
        String groupId = args[2];

        SparkSession spark = SparkSession
                .builder()
                .appName("ConsumerSpark")
                .master("local[*]") // Set the master URL for local mode
                .config("spark.mongodb.input.uri", "mongodb://localhost:27017/")
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
                .as("value")
                .selectExpr("split(value, ',') as data")
                .selectExpr("explode(split(data[1], ';')) as artist",
                        "cast(data[0] as string) as track_name",
                        "cast(data[8] as int) as streams")
                .filter(col("streams").rlike("\\d+"));

        // Group by 'artist' and find max 'streams', also get the corresponding 'track_name'
        Dataset<Row> maxStreamsPerArtist = df.groupBy("artist")
                .agg(max(struct(col("streams"), col("track_name"))).alias("max_streams"))
                .select(col("artist"), col("max_streams.streams").alias("max_streams"), col("max_streams.track_name").alias("track_name"));

        // Write the streaming data to MongoDB
        StreamingQuery query = maxStreamsPerArtist.writeStream()
                .outputMode("update") // Change output mode to update for MongoDB
                .foreachBatch((batchDF, batchId) -> {
                    // Write each batch to MongoDB
                    batchDF.write()
                            .format("mongodb")
                            //.option("checkpointLocation", "/tmp/")
                            //.option("forceDeleteTempCheckpointLocation", "true")
                            //.option("spark.mongodb.connection.uri", "mongodb://localhost:27017/")
                            .option("database", "BigData_Project")
                            .option("collection", "stream")
                            .mode("append")
                            .save();
                })
                .trigger(Trigger.ProcessingTime("1 second"))
                .start();

        query.awaitTermination();
    }
}