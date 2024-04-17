package hadoop.mapreduce;

import java.io.IOException;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

public class ArtistMapper extends Mapper<LongWritable, Text, Text, IntWritable> {
    private final static IntWritable streams = new IntWritable(1);
    private Text artist = new Text();

    public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
        String[] parts = value.toString().split(",");
        String[] artists = parts[1].split(",");
        int numStreams = Integer.parseInt(parts[8]);
        for (String art : artists) {
            artist.set(art.trim());
            streams.set(numStreams);
            context.write(artist, streams);
        }
    }
}