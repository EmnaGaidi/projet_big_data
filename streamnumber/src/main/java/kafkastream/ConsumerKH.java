package kafkastream;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.*;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Encoders;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.streaming.StreamingQuery;

import static org.apache.spark.sql.functions.sort_array;

import java.io.IOException;
import java.util.*;

public class ConsumerKH {

    public static void main(String[] args) throws Exception {

        String bootstrapServers = "localhost:9092";
        String topics = "topic1";
        String groupId = "mySparkConsumerGroup";
        String hbaseTable = "MaxStreamsPerArtist";
        String columnFamily = "ArtistData";

        SparkSession spark = SparkSession
                .builder()
                .appName("SparkKafka")
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
        
        // Define HBase configuration
        Configuration hbaseConfig = HBaseConfiguration.create();
        // Create HBase table if it does not exist
        try (Connection connection = ConnectionFactory.createConnection(hbaseConfig);
             Admin admin = connection.getAdmin()) {
            TableName tableName = TableName.valueOf(hbaseTable);
            if (!admin.tableExists(tableName)) {
                TableDescriptor tableDescriptor = TableDescriptorBuilder.newBuilder(tableName)
                        .setColumnFamily(ColumnFamilyDescriptorBuilder.of(columnFamily))
                        .build();
                admin.createTable(tableDescriptor);
            }
        } catch (IOException e) {
            e.printStackTrace();
            System.err.println("An error occurred while creating HBase table !!!!!!!!!!!!!!!!!!!: " + e.getMessage());
        }

        // Define the StreamingQuery with foreachBatch to write max streams per artist to HBase
        StreamingQuery hbaseQuery = maxStreamsPerArtist.writeStream()
                .outputMode("complete")
                .foreachBatch((batchDF, batchId) -> {
                    try (Connection connection = ConnectionFactory.createConnection(hbaseConfig);
                         Table table = connection.getTable(TableName.valueOf(hbaseTable))) {
                            System.out.println("entred try block"+batchDF.count());
                            System.out.println("BatchId"+batchId);
                            //int index = 0;
                        //List<Row> rows = batchDF.takeAsList(10);
                        List<Row> rows = batchDF.collectAsList();
                        System.out.println("rows rows are : "+rows);
                        for (Row row : rows) {
    List<String> rowList = new ArrayList<>();
    for (int i = 0; i < row.length(); i++) {
        rowList.add(String.valueOf(row.get(i)));
    }
    String artist = rowList.get(0);
    Integer maxStreams = row.getInt(row.fieldIndex("max_streams"));
    System.out.println("artist is " + artist + " and maxStreams " + maxStreams);
    //Put put = new Put(Bytes.toBytes(index));
    Put put = new Put(Bytes.toBytes(artist));
    //put.addColumn(Bytes.toBytes(columnFamily), Bytes.toBytes("artist"), Bytes.toBytes(artist));
    put.addColumn(Bytes.toBytes(columnFamily), Bytes.toBytes("max_streams"), Bytes.toBytes(Integer.toString(maxStreams)));
    table.put(put);
}


                    } catch (Exception e) {
                        e.printStackTrace();
                        System.err.println("An error occurred while accessing HBase table: " + e.getMessage());
                    }
                })
                .start();

        // Await termination
        hbaseQuery.awaitTermination();

    }
}
