package hadoop.mapreduce;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

public class StreamsByArtistMain {

    public static void main(String[] args) throws Exception {
        if (args.length != 2) {
            System.err.println("Usage: StreamsByArtistMain <input_path> <output_path>");
            System.exit(-1);
        }

        Configuration conf = new Configuration();
        Job job = Job.getInstance(conf, "StreamsByArtist");
        job.setJarByClass(StreamsByArtistMain.class);

        // Mapper and Reducer classes
        job.setMapperClass(ArtistMapper.class);
        job.setReducerClass(TotalStreamsReducer.class);

        // Output key-value types
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(IntWritable.class);

        // Input and Output paths
        FileInputFormat.addInputPath(job, new Path(args[0]));
        FileOutputFormat.setOutputPath(job, new Path(args[1]));

        // Submit the job and wait for completion
        System.exit(job.waitForCompletion(true) ? 0 : 1);
    }
}
