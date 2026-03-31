package task3;

import java.io.IOException;
import java.net.URI;
import java.util.Map;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;

import utils.Parse;
import utils.SideTables;

public class Reducer extends org.apache.hadoop.mapreduce.Reducer<Text, Text, NullWritable, Text> {

    private Map<String, String> movieDict;
    private final Text outputText = new Text();

    @Override
    protected void setup(Context ctx) throws IOException {
        URI[] uris = ctx.getCacheFiles();
        movieDict = SideTables.loadMovieTitles(ctx.getConfiguration(), uris, "movies.txt");
    }

    @Override
    protected void reduce(Text k, Iterable<Text> vals, Context ctx) throws IOException, InterruptedException {
        double maleSum = 0.0, femaleSum = 0.0;
        long maleCount = 0, femaleCount = 0;
        
        for (Text val : vals) {
            String[] tokens = val.toString().split("\t", 2);
            if (tokens.length >= 2) {
                try {
                    double rating = Double.parseDouble(tokens[1]);
                    if (tokens[0].equals("M")) {
                        maleSum += rating;
                        maleCount++;
                    } else if (tokens[0].equals("F")) {
                        femaleSum += rating;
                        femaleCount++;
                    }
                } catch (NumberFormatException ignored) {}
            }
        }
        
        String mId = k.toString();
        String resultTitle = movieDict.getOrDefault(mId, mId);
        
        String mAvg = (maleCount > 0) ? Parse.fmtRating(maleSum / maleCount) : "N/A";
        String fAvg = (femaleCount > 0) ? Parse.fmtRating(femaleSum / femaleCount) : "N/A";
        
        StringBuilder sb = new StringBuilder();
        sb.append(resultTitle).append(": ").append(mAvg).append(", ").append(fAvg);
        outputText.set(sb.toString());
        ctx.write(NullWritable.get(), outputText);
    }
}
