package task2;

import java.io.IOException;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;

import utils.Parse;

public class Reducer extends org.apache.hadoop.mapreduce.Reducer<Text, Text, NullWritable, Text> {

    private final Text outText = new Text();

    @Override
    protected void reduce(Text k, Iterable<Text> vals, Context ctx) throws IOException, InterruptedException {
        double acc = 0.0;
        long n = 0;
        
        for (Text val : vals) {
            try {
                acc += Double.parseDouble(val.toString());
                n += 1;
            } catch (NumberFormatException ignored) {}
        }
        
        if (n > 0) {
            double average = acc / n;
            StringBuilder sb = new StringBuilder();
            sb.append(k.toString()).append(": ").append(Parse.fmtRating(average))
              .append(" (TotalRatings: ").append(n).append(")");
            outText.set(sb.toString());
            ctx.write(NullWritable.get(), outText);
        }
    }
}
