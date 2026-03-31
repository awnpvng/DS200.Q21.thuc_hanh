package task1;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Comparator;
import java.util.List;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import utils.Parse;

public class ReportReducer extends Reducer<Text, Text, NullWritable, Text> {

    private static final int THRESHOLD = 5;

    private static final class MovieData {
        final String name;
        final double score;
        final long votes;

        MovieData(String name, double score, long votes) {
            this.name = name;
            this.score = score;
            this.votes = votes;
        }
    }

    private final List<MovieData> records = new ArrayList<>();
    private final Text outputText = new Text();

    @Override
    protected void reduce(Text k, Iterable<Text> vals, Context ctx) {
        records.clear();
        for (Text txt : vals) {
            String[] tokens = txt.toString().split("\t", 3);
            if (tokens.length >= 3) {
                try {
                    records.add(new MovieData(tokens[0], Double.parseDouble(tokens[1]), Long.parseLong(tokens[2])));
                } catch (NumberFormatException ignored) {}
            }
        }
    }

    @Override
    protected void cleanup(Context ctx) throws IOException, InterruptedException {
        Collections.sort(records, Comparator.comparing(md -> md.name));
        
        for (MovieData md : records) {
            StringBuilder sb = new StringBuilder();
            sb.append(md.name).append(" AverageRating: ").append(Parse.fmtRating(md.score))
              .append(" (TotalRatings: ").append(md.votes).append(")");
            outputText.set(sb.toString());
            ctx.write(NullWritable.get(), outputText);
        }
        
        outputText.set("");
        ctx.write(NullWritable.get(), outputText);

        MovieData topMovie = null;
        for (MovieData md : records) {
            if (md.votes >= THRESHOLD) {
                if (topMovie == null || md.score > topMovie.score) {
                    topMovie = md;
                }
            }
        }

        if (topMovie != null) {
            StringBuilder sb = new StringBuilder();
            sb.append(topMovie.name).append(" is the highest rated movie with an average rating of ")
              .append(Parse.fmtRating(topMovie.score)).append(" among movies with at least 5 ratings.");
            outputText.set(sb.toString());
        } else {
            outputText.set("No movie has at least 5 ratings.");
        }
        ctx.write(NullWritable.get(), outputText);
    }
}
