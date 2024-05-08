package hadoop.streaming;

import org.apache.spark.sql.streaming.StreamingQuery;
import org.apache.spark.sql.streaming.StreamingQueryException;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.functions;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.Encoders;
import org.apache.spark.sql.streaming.Trigger;

import org.apache.hadoop.hbase.client.Table;

import java.util.List;
import java.util.concurrent.TimeoutException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.client.Connection;
import org.apache.hadoop.hbase.client.ConnectionFactory;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.util.Bytes;

public class StreamHbase {
    public static void main(String[] args) throws StreamingQueryException, TimeoutException {
        SparkSession spark = SparkSession
            .builder()
            .appName("StreamExample")
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
                        .filter(functions.col("data").getItem(3).rlike("\\b\\d{4}\\b")) // Filtre pour un entier de 4 chiffres
                        .selectExpr("cast(data[3] as int) as released_year", "data[8] as streams")
                        .filter(functions.col("streams").rlike("\\d+"))
                        .selectExpr("released_year", "cast(streams as int) as streams");

        // Group by 'released_year' and sum 'streams'
        Dataset<Row> streamsPerYear = df.groupBy("released_year").agg(functions.sum("streams").alias("total_streams"));

        // Define HBase configuration
        Configuration hbaseConfig = HBaseConfiguration.create();

        // Define the StreamingQuery with foreachBatch to write total streams per year to HBase
        StreamingQuery query = streamsPerYear.writeStream()
    .outputMode("complete")
    .foreachBatch((batchDF, batchId) -> {
        try (Connection connection = ConnectionFactory.createConnection(hbaseConfig);
             Table table = connection.getTable(TableName.valueOf("streamByYear"))) {
            System.out.println("entred try block" + batchDF.count());
            System.out.println("BatchId" + batchId);

            List<Row> rows = batchDF.collectAsList();
            System.out.println("rows rows are : " + rows);
            for (Row row : rows) {
                int releasedYear = row.getInt(row.fieldIndex("released_year"));
                long totalStreams = row.getLong(row.fieldIndex("total_streams"));

                Put put = new Put(Bytes.toBytes(String.valueOf(releasedYear)));
                put.addColumn(Bytes.toBytes("cf"), Bytes.toBytes("total_streams"), Bytes.toBytes(Long.toString(totalStreams)));
                table.put(put);
            }
        } catch (Exception e) {
            e.printStackTrace();
            System.err.println("An error occurred while accessing HBase table: " + e.getMessage());
        }
    })
    .trigger(Trigger.ProcessingTime("1 second"))
    .start();

// Await termination
query.awaitTermination();


        query.awaitTermination();
    }
}

