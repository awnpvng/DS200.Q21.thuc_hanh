package task4;

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
    private final Text outTokenKey = new Text();
    private final Text outTokenVal = new Text();

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
                String ageGroup = Parse.ageBucket(user.age);
                
                outTokenKey.set(mId);
                StringBuilder sb = new StringBuilder();
                sb.append(ageGroup).append("\t").append(parsed[2]);
                outTokenVal.set(sb.toString());
                
                ctx.write(outTokenKey, outTokenVal);
            }
        }
    }
}
