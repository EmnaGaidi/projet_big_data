package kafkastream;

import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Encoders;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.streaming.StreamingQuery;

import org.bson.Document;
import com.mongodb.ConnectionString;
import com.mongodb.client.MongoClients;
import com.mongodb.client.MongoClient;
import com.mongodb.client.MongoDatabase;

import java.util.List;

public class ConsumerMongo {

    public static void main(String[] args) throws Exception {

        String bootstrapServers = "localhost:9092";
        String topics = "topic1";
        String groupId = "mySparkConsumerGroup";

        SparkSession spark = SparkSession
                .builder()
                .appName("SparkKafka")
                .master("local[*]")
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
                .filter("streams IS NOT NULL");

        // Group by 'artist' and find max 'streams'
        Dataset<Row> maxStreamsPerArtist = df.groupBy("artist")
                .agg(org.apache.spark.sql.functions.max("streams").alias("max_streams"));

        // MongoDB configuration
        MongoClient mongoClient = MongoClients.create(new ConnectionString("mongodb+srv://emnagaidii:r1ytLz9IbySSK9zh@cluster0.huypuow.mongodb.net/"));
        MongoDatabase database = mongoClient.getDatabase("BigData_Project");

        StreamingQuery mongoDBQuery = maxStreamsPerArtist.writeStream()
        .outputMode("complete")
        .foreachBatch((batchDF, batchId) -> {
            List<Row> rows = batchDF.collectAsList();
            for (Row row : rows) {
                String artist = row.getString(row.fieldIndex("artist"));
                Integer maxStreams = row.getInt(row.fieldIndex("max_streams"));
                System.out.println("artist is " + artist + " and maxStreams " + maxStreams);

                // Check if a document with the same artist name already exists
                Document existingDocument = database.getCollection("streams")
                        .find(new Document("artist", artist))
                        .first();

                // If exists, delete it
                if (existingDocument != null) {
                    database.getCollection("streams")
                            .deleteOne(new Document("artist", artist));
                    System.out.println("Existing document with artist " + artist + " deleted.");
                }

                // Insert new document
                try {
                    database.getCollection("streams")
                            .insertOne(new Document("artist", artist)
                                    .append("max_streams", maxStreams));
                    System.out.println("New document with artist " + artist + " inserted.");
                } catch (Exception e) {
                    e.printStackTrace();
                    System.err.println("An error occurred while inserting into MongoDB: " + e.getMessage());
                }
            }
        })
        .start();

        // Await termination
        mongoDBQuery.awaitTermination();

    }
}  
    
