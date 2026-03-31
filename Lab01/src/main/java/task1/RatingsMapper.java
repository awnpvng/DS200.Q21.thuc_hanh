package task1;

import java.io.IOException;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import utils.Parse;

public class RatingsMapper extends Mapper<LongWritable, Text, Text, Text> {

    private final Text keyOut = new Text();
    private final Text valOut = new Text();

    @Override
    protected void map(LongWritable k, Text v, Context ctx) throws IOException, InterruptedException {
        String[] parsed = Parse.parseRatingLine(v.toString());
        if (parsed != null) {
            keyOut.set(parsed[1]);
            valOut.set(parsed[2]);
            ctx.write(keyOut, valOut);
        }
    }
}
