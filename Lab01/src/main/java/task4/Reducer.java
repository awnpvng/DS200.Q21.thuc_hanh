package task4;

import java.io.IOException;
import java.net.URI;
import java.util.Map;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;

import utils.Parse;
import utils.SideTables;

public class Reducer extends org.apache.hadoop.mapreduce.Reducer<Text, Text, NullWritable, Text> {

    private static final String[] INTERVALS = {"0-18", "18-35", "35-50", "50+"};
    private Map<String, String> movieDict;
    private final Text outputText = new Text();

    @Override
    protected void setup(Context ctx) throws IOException {
        URI[] uris = ctx.getCacheFiles();
        movieDict = SideTables.loadMovieTitles(ctx.getConfiguration(), uris, "movies.txt");
    }

    @Override
    protected void reduce(Text k, Iterable<Text> vals, Context ctx) throws IOException, InterruptedException {
        double[] intervalSums = new double[4];
        long[] intervalCounts = new long[4];
        
        for (Text val : vals) {
            String[] tokens = val.toString().split("\t", 2);
            if (tokens.length >= 2) {
                String currentBucket = tokens[0];
                try {
                    double rating = Double.parseDouble(tokens[1]);
                    for (int i = 0; i < 4; i++) {
                        if (INTERVALS[i].equals(currentBucket)) {
                            intervalSums[i] += rating;
                            intervalCounts[i] += 1;
                            break;
                        }
                    }
                } catch (NumberFormatException ignored) {}
            }
        }
        
        String mId = k.toString();
        String resultTitle = movieDict.getOrDefault(mId, mId);
        
        StringBuilder sb = new StringBuilder();
        sb.append(resultTitle).append(": [");
        
        for (int i = 0; i < 4; i++) {
            if (i > 0) {
                sb.append(", ");
            }
            if (intervalCounts[i] == 0) {
                sb.append(INTERVALS[i]).append(": N/A");
            } else {
                sb.append(INTERVALS[i]).append(": ").append(Parse.fmtRating(intervalSums[i] / intervalCounts[i]));
            }
        }
        sb.append("]");
        
        outputText.set(sb.toString());
        ctx.write(NullWritable.get(), outputText);
    }
}
