package kafkastream;

import java.util.concurrent.TimeoutException;

import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.streaming.StreamingQuery;
import org.apache.spark.sql.streaming.StreamingQueryException;
import org.apache.spark.sql.streaming.Trigger;
import org.apache.spark.sql.types.DataTypes;
import org.apache.spark.sql.types.StructType;

public class Consumer {

    public static void main(String[] args) throws StreamingQueryException, TimeoutException {

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

        StructType schema = new StructType()
                .add("artist", DataTypes.StringType)
                .add("max_streams", DataTypes.IntegerType)
                .add("track_name", DataTypes.StringType);

        Dataset<Row> df = spark
                .readStream()
                .format("kafka")
                .option("kafka.bootstrap.servers", bootstrapServers)
                .option("subscribe", topics)
                .option("kafka.group.id", groupId)
                .load()
                .selectExpr("CAST(value AS STRING)");

        MongoDBForeachWriter mongoWriter = new MongoDBForeachWriter();

        StreamingQuery query = df.selectExpr("split(value, ',') as data")
        .selectExpr("explode(split(data[1], ';')) as artist",
                "cast(data[0] as string) as track_name",
                "cast(data[8] as int) as streams")
        .filter("streams is not null")
        .groupBy("artist")
        .agg(org.apache.spark.sql.functions.max("streams").alias("max_streams"),
                org.apache.spark.sql.functions.first("track_name").alias("track_name"))
        .writeStream()
        .outputMode("complete")
        .format("console")
        //.foreach(new MongoDBForeachWriter())
        //.start();
        .foreachBatch((batchDF, batchId) -> {
    // Set the batch ID in the MongoDB writer
                mongoWriter.setBatchId(batchId);
    // Process each batch using the MongoDB writer
                //mongoWriter.process((Row) batchDF); // Assuming you have a method like processBatch in your MongoDBForeachWriter
    // Print when the batch changes
                System.out.println("Batch ID: " + batchId);
        })
        .foreach(mongoWriter)
        .trigger(Trigger.ProcessingTime("1 second"))
        .start();

        query.awaitTermination();
    }
}
