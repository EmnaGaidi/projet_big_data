package hadoop.streaming;

import org.apache.spark.sql.streaming.StreamingQuery;
import org.apache.spark.sql.streaming.StreamingQueryException;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.functions;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.Encoders;
import org.apache.spark.sql.streaming.Trigger;

import com.mongodb.ConnectionString;
import com.mongodb.client.MongoClients;
import com.mongodb.client.MongoClient;
import com.mongodb.client.MongoDatabase;
import com.mongodb.client.MongoCollection;
import org.bson.Document;

import java.util.List;
import java.util.concurrent.TimeoutException;

public class StreamMongoDB {
    public static void main(String[] args) throws StreamingQueryException, TimeoutException {
        SparkSession spark = SparkSession
            .builder()
            .appName("StreamExample")
            .master("local[*]")
            .getOrCreate();

        // Read data from Kafka
        Dataset<Row> kafkaStream = spark
            .readStream()
            .format("kafka")
            .option("kafka.bootstrap.servers", "localhost:9092")
            .option("subscribe", "topic1")
            .option("kafka.group.id", "mySparkConsumerGroup")
            .load();

        // Extract value from Kafka message
        Dataset<String> lines = kafkaStream.selectExpr("CAST(value AS STRING)").as(Encoders.STRING());

        // Parse the CSV-like format directly to extract the release year and streams columns
        Dataset<Row> df = lines.selectExpr("split(value, ',') as data")
                        .filter(functions.col("data").getItem(3).rlike("\\b\\d{4}\\b")) // Filter for a 4-digit integer
                        .selectExpr("cast(data[3] as int) as released_year", "data[8] as streams")
                        .filter(functions.col("streams").rlike("\\d+"))
                        .selectExpr("released_year", "cast(streams as long) as streams");

        // Group by 'released_year' and sum 'streams'
        Dataset<Row> streamsPerYear = df.groupBy("released_year").agg(functions.sum("streams").alias("total_streams"));

        // MongoDB connection URI
        String connectionString = "mongodb+srv://emnagaidii:r1ytLz9IbySSK9zh@cluster0.huypuow.mongodb.net/";
        
        // MongoDB configuration
        MongoClient mongoClient = MongoClients.create(new ConnectionString(connectionString));
        MongoDatabase database = mongoClient.getDatabase("BigData_Project");
        MongoCollection<Document> collection = database.getCollection("streamByYear");

        // Define the StreamingQuery with foreachBatch to write total streams per year to MongoDB
        StreamingQuery query = streamsPerYear.writeStream()
            .outputMode("complete")
            .foreachBatch((batchDF, batchId) -> {
                List<Row> rows = batchDF.collectAsList();
                for (Row row : rows) {
                    int releasedYear = row.getInt(row.fieldIndex("released_year"));
                    long totalStreams = row.getLong(row.fieldIndex("total_streams"));

                    // Check if a document with the same released year already exists
                    Document existingDocument = collection.find(new Document("released_year", releasedYear)).first();

                    // If exists, delete it
                    if (existingDocument != null) {
                        collection.deleteOne(new Document("released_year", releasedYear));
                        System.out.println("Existing document with released year " + releasedYear + " deleted.");
                    }

                    // Insert new document
                    try {
                        collection.insertOne(new Document("released_year", releasedYear)
                                .append("total_streams", totalStreams));
                        System.out.println("New document with released year " + releasedYear + " inserted.");
                    } catch (Exception e) {
                        e.printStackTrace();
                        System.err.println("An error occurred while inserting into MongoDB: " + e.getMessage());
                    }
                }
            })
            .trigger(Trigger.ProcessingTime("1 second"))
            .start();

        // Await termination
        query.awaitTermination();
    }
}