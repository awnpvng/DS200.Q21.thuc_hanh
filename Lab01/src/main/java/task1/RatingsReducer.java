package task1;

import java.io.IOException;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

public class RatingsReducer extends Reducer<Text, Text, Text, Text> {

    private final Text valOut = new Text();

    @Override
    protected void reduce(Text k, Iterable<Text> vals, Context ctx) throws IOException, InterruptedException {
        double totalSum = 0.0;
        long totalCount = 0;
        
        for (Text txt : vals) {
            try {
                totalSum += Double.parseDouble(txt.toString());
                totalCount += 1;
            } catch (NumberFormatException ignored) {
            }
        }
        
        if (totalCount > 0) {
            StringBuilder sb = new StringBuilder();
            sb.append(totalSum).append("\t").append(totalCount);
            valOut.set(sb.toString());
            ctx.write(k, valOut);
        }
    }
}
