package task4;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileUtil;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

public final class Driver {

    private Driver() {}

    public static void main(String[] args) throws Exception {
        if (args.length != 5) {
            System.err.println("Usage: Driver <merged_ratings> <users.txt> <movies.txt> <work_dir> <final_report.txt>");
            System.exit(1);
        }

        Path ratingsPath = new Path(args[0]);
        Path usersPath = new Path(args[1]);
        Path moviesPath = new Path(args[2]);
        Path tempPath = new Path(args[3]);
        Path outPath = new Path(args[4]);

        Configuration cfg = new Configuration();
        cfg.set("mapreduce.framework.name", "local");
        cfg.set("fs.defaultFS", "file:///");

        Path outStage = new Path(tempPath, "t4_worker");
        FileSystem sys = FileSystem.get(cfg);
        sys.delete(outStage, true);
        if (sys.exists(outPath)) {
            sys.delete(outPath, false);
        }

        Job ageJob = Job.getInstance(cfg, "Task 4 - Age Groups by Movie");
        ageJob.getConfiguration().set("mapreduce.output.textoutputformat.separator", "");
        ageJob.addCacheFile(usersPath.toUri());
        ageJob.addCacheFile(moviesPath.toUri());
        ageJob.setJarByClass(Driver.class);
        ageJob.setMapperClass(Mapper.class);
        ageJob.setReducerClass(Reducer.class);
        ageJob.setMapOutputKeyClass(Text.class);
        ageJob.setMapOutputValueClass(Text.class);
        ageJob.setOutputKeyClass(NullWritable.class);
        ageJob.setOutputValueClass(Text.class);
        ageJob.setNumReduceTasks(1);
        FileInputFormat.addInputPath(ageJob, ratingsPath);
        FileOutputFormat.setOutputPath(ageJob, outStage);
        if (!ageJob.waitForCompletion(true)) {
            System.exit(1);
        }

        Path partFile = new Path(outStage, "part-r-00000");
        if (!FileUtil.copy(sys, partFile, sys, outPath, false, true, cfg)) {
            System.exit(1);
        }
    }
}
