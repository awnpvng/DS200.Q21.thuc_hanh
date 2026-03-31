package task1;

import java.io.IOException;
import java.net.URI;
import java.util.Map;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import utils.SideTables;

public class ReportMapper extends Mapper<LongWritable, Text, Text, Text> {

    private static final String KEY_CONSTANT = "TASK1_REPORT";
    private Map<String, String> titlesDict;
    private final Text keyOut = new Text();
    private final Text valOut = new Text();

    @Override
    protected void setup(Context ctx) throws IOException {
        URI[] uris = ctx.getCacheFiles();
        titlesDict = SideTables.loadMovieTitles(ctx.getConfiguration(), uris, "movies.txt");
    }

    @Override
    protected void map(LongWritable k, Text v, Context ctx) throws IOException, InterruptedException {
        String row = v.toString().trim();
        if (row.isEmpty()) return;
        
        String[] tokens = row.split("\t");
        if (tokens.length >= 3) {
            try {
                double totalSum = Double.parseDouble(tokens[1]);
                long totalCount = Long.parseLong(tokens[2]);
                if (totalCount > 0) {
                    double average = totalSum / totalCount;
                    String movieName = titlesDict.getOrDefault(tokens[0], tokens[0]);
                    
                    keyOut.set(KEY_CONSTANT);
                    
                    StringBuilder sb = new StringBuilder();
                    sb.append(movieName).append("\t").append(average).append("\t").append(totalCount);
                    valOut.set(sb.toString());
                    
                    ctx.write(keyOut, valOut);
                }
            } catch (NumberFormatException ignored) {}
        }
    }
}
