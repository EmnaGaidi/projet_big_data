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
        String[] parts = value.toString().split(",(?=(?:[^\"]*\"[^\"]*\")*[^\"]*$)");
       String artistField = parts[1];

        // Remove surrounding double quotes if present
        if (artistField.startsWith("\"") && artistField.endsWith("\"")) {
            artistField = artistField.substring(1, artistField.length() - 1);
        }

        // Split the artist names using ", " as delimiter
        String[] artists = artistField.split(",\\s*");
        /*
        int numStreams = Integer.parseInt(parts[8]);
        */
        int numStreams;
try {
    numStreams = Integer.parseInt(parts[8]);
} catch (NumberFormatException e) {
    // Si la conversion échoue, parts[8] n'est pas un nombre valide
    // Dans ce cas, considérez-le comme 0
    numStreams = 0;
}

        for (String art : artists) {
            artist.set(art.trim());
            streams.set(numStreams);
            context.write(artist, streams);
        }
    }
}