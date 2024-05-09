package hadoop.mapreduce.batchSpark;

import com.mongodb.ConnectionString;
import com.mongodb.MongoClientSettings;
import com.mongodb.client.MongoClient;
import com.mongodb.client.MongoClients;
import com.mongodb.client.MongoCollection;
import com.mongodb.client.MongoDatabase;

import hadoop.mapreduce.ArtistMapper;
import hadoop.mapreduce.TotalStreamsReducer;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.bson.Document;

import java.io.BufferedReader;
import java.io.InputStreamReader;
import java.util.ArrayList;
import java.util.List;

public class StreamsByArtistMongoDB {

    public static void main(String[] args) throws Exception {
        if (args.length != 2) {
            System.err.println("Usage: StreamsByArtistMain <input_path> <output_path>");
            System.exit(-1);
        }

        Configuration conf = new Configuration();
        Job job = Job.getInstance(conf, "StreamsByArtist");
        job.setJarByClass(StreamsByArtistMongoDB.class);

        // Mapper and Reducer classes
        job.setMapperClass(ArtistMapper.class);
        job.setCombinerClass(TotalStreamsReducer.class);
        job.setReducerClass(TotalStreamsReducer.class);

        // Output key-value types
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(IntWritable.class);

        // Input and Output paths
        FileInputFormat.addInputPath(job, new Path(args[0]));
        FileOutputFormat.setOutputPath(job, new Path(args[1]));

        // Submit the job and wait for completion
        if (job.waitForCompletion(true)) {
            // Once the job completes, store data in MongoDB
            FileSystem fs = FileSystem.get(new Configuration());
            FileStatus[] status = fs.listStatus(new Path(args[1]));
            for (FileStatus fileStatus : status) {
                BufferedReader br = new BufferedReader(new InputStreamReader(fs.open(fileStatus.getPath())));
                String line;
                List<Document> docs = new ArrayList<>();
                while ((line = br.readLine()) != null) {
                    String[] fields = line.split("\t");
                    Document doc = new Document("artist", fields[0])
                            .append("streams", Integer.parseInt(fields[1]));
                    docs.add(doc);
                }
                if (!docs.isEmpty()) {
                    String uri = "mongodb+srv://emnagaidii:r1ytLz9IbySSK9zh@cluster0.huypuow.mongodb.net/";
                    ConnectionString mongoURI = new ConnectionString(uri);
                    MongoClientSettings settings = MongoClientSettings.builder()
                            .applyConnectionString(mongoURI)
                            .build();
                    MongoClient mongoClient = MongoClients.create(settings);
                    MongoDatabase database = mongoClient.getDatabase("BigData_Project");
                    MongoCollection<Document> collection = database.getCollection("streamsPerArtist");

                    collection.insertMany(docs);
                    mongoClient.close();
                }
            }
            System.exit(0);
        } else {
            System.exit(1);
        }
    }
}