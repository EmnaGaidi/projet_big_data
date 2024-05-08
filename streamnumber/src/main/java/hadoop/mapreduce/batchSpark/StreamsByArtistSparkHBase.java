package hadoop.mapreduce.batchSpark;

import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.sql.SparkSession;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.Admin;
import org.apache.hadoop.hbase.client.ColumnFamilyDescriptorBuilder;
import org.apache.hadoop.hbase.client.Connection;
import org.apache.hadoop.hbase.client.ConnectionFactory;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.client.TableDescriptorBuilder;
import org.apache.hadoop.hbase.util.Bytes;
import scala.Tuple2;

import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.List;

public class StreamsByArtistSparkHBase {

    public static void main(String[] args) {
        String csvFileName = "Popular_Spotify_Songs.csv"; // Change this to your CSV file name

        // Create a SparkSession
        SparkSession spark = SparkSession.builder()
                .appName("StreamsByArtistSparkHBase")
                .getOrCreate();

        // Create a JavaSparkContext from the SparkSession
        JavaSparkContext sc = JavaSparkContext.fromSparkContext(spark.sparkContext());

        // Load input data from CSV
        JavaRDD<String> input = sc.textFile(csvFileName);

        // Parse input and map it to (artist, numStreams) pairs
        JavaPairRDD<String, Integer> artistStreams = input.flatMapToPair(line -> {
            String[] parts = line.split(",(?=(?:[^\"]*\"[^\"]*\")*[^\"]*$)");
            String artistField = parts[1];

            if (artistField.startsWith("\"") && artistField.endsWith("\"")) {
                artistField = artistField.substring(1, artistField.length() - 1);
            }

            String[] artists = artistField.split(",\\s*");
            int numStreams;
            try {
                numStreams = Integer.parseInt(parts[8]);
            } catch (NumberFormatException e) {
                numStreams = 0;
            }

            List<Tuple2<String, Integer>> tuples = new ArrayList<>();
            for (String art : artists) {
                tuples.add(new Tuple2<>(art.trim(), numStreams));
            }
            return tuples.iterator();
        });

        // Reduce by key to get total streams per artist
        JavaPairRDD<String, Integer> totalStreamsPerArtist = artistStreams.reduceByKey(Integer::sum);

        // Store the result in HBase
        totalStreamsPerArtist.foreachPartition(partition -> {
            org.apache.hadoop.conf.Configuration hbaseConfig = HBaseConfiguration.create();
            Connection connection = ConnectionFactory.createConnection(hbaseConfig);

            // Create HBase admin
            Admin admin = connection.getAdmin();

            // Specify the HBase table name
            TableName tableName = TableName.valueOf("StreamsArtist"); 

            // Check if the table exists, if not, create it
            if (!admin.tableExists(tableName)) {
                admin.createTable(new TableDescriptorBuilder
                        .ModifyableTableDescriptor(tableName)
                        .setColumnFamily(new ColumnFamilyDescriptorBuilder.ModifyableColumnFamilyDescriptor(Bytes.toBytes("cf"))));
            }

            // Close HBase admin
            admin.close();

            // Create or get the table
            org.apache.hadoop.hbase.client.Table table = connection.getTable(tableName);

            partition.forEachRemaining(tuple -> {
                int totalStreams = tuple._2();

                byte[] artistBytes = tuple._1().getBytes(StandardCharsets.UTF_8);

                // Créer un objet Put avec le nom de l'artiste comme clé de ligne
                Put put = new Put(artistBytes);

                // Add column family, qualifier, and value
                put.addColumn(Bytes.toBytes("cf"), Bytes.toBytes("totalStreams"), Bytes.toBytes(Integer.toString(totalStreams)));

                try {
                    // Insert data into HBase table
                    table.put(put);
                } catch (IOException e) {
                    e.printStackTrace();
                }
            });

            // Close HBase table and connection
            table.close();
            connection.close();
        });

        // Stop the SparkSession
        spark.stop();
    }
}

