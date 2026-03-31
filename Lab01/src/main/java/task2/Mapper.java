package task2;

import java.io.IOException;
import java.net.URI;
import java.util.List;
import java.util.Map;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;

import utils.Parse;
import utils.SideTables;

public class Mapper extends org.apache.hadoop.mapreduce.Mapper<LongWritable, Text, Text, Text> {

    private Map<String, List<String>> lookupGenres;
    private final Text keyOut = new Text();
    private final Text valOut = new Text();

    @Override
    protected void setup(Context ctx) throws IOException {
        URI[] uris = ctx.getCacheFiles();
        lookupGenres = SideTables.loadGenresByMovie(ctx.getConfiguration(), uris, "movies.txt");
    }

    @Override
    protected void map(LongWritable k, Text v, Context ctx) throws IOException, InterruptedException {
        String[] parsed = Parse.parseRatingLine(v.toString());
        if (parsed != null) {
            String mId = parsed[1];
            List<String> found = lookupGenres.get(mId);
            if (found != null && !found.isEmpty()) {
                for (int i = 0; i < found.size(); i++) {
                    keyOut.set(found.get(i));
                    valOut.set(parsed[2]);
                    ctx.write(keyOut, valOut);
                }
            }
        }
    }
}
