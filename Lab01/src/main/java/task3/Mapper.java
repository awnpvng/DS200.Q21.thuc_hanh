package task3;

import java.io.IOException;
import java.net.URI;
import java.util.Map;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;

import utils.Parse;
import utils.SideTables;
import utils.SideTables.UserRow;

public class Mapper extends org.apache.hadoop.mapreduce.Mapper<LongWritable, Text, Text, Text> {

    private Map<String, UserRow> userDict;
    private final Text kOut = new Text();
    private final Text vOut = new Text();

    @Override
    protected void setup(Context ctx) throws IOException {
        URI[] uris = ctx.getCacheFiles();
        userDict = SideTables.loadUsers(ctx.getConfiguration(), uris, "users.txt");
    }

    @Override
    protected void map(LongWritable k, Text v, Context ctx) throws IOException, InterruptedException {
        String[] parsed = Parse.parseRatingLine(v.toString());
        if (parsed != null) {
            String uId = parsed[0];
            String mId = parsed[1];
            
            if (userDict.containsKey(uId)) {
                UserRow user = userDict.get(uId);
                String gen = user.gender;
                
                if (gen.equals("M") || gen.equals("F")) {
                    kOut.set(mId);
                    StringBuilder sb = new StringBuilder();
                    sb.append(gen).append("\t").append(parsed[2]);
                    vOut.set(sb.toString());
                    ctx.write(kOut, vOut);
                }
            }
        }
    }
}
