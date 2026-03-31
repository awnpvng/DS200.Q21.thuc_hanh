package task3;

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

        Path outStage = new Path(tempPath, "t3_worker");
        FileSystem sys = FileSystem.get(cfg);
        sys.delete(outStage, true);
        if (sys.exists(outPath)) {
            sys.delete(outPath, false);
        }

        Job genderJob = Job.getInstance(cfg, "Task 3 - Gender by Movie");
        genderJob.getConfiguration().set("mapreduce.output.textoutputformat.separator", "");
        genderJob.addCacheFile(usersPath.toUri());
        genderJob.addCacheFile(moviesPath.toUri());
        genderJob.setJarByClass(Driver.class);
        genderJob.setMapperClass(Mapper.class);
        genderJob.setReducerClass(Reducer.class);
        genderJob.setMapOutputKeyClass(Text.class);
        genderJob.setMapOutputValueClass(Text.class);
        genderJob.setOutputKeyClass(NullWritable.class);
        genderJob.setOutputValueClass(Text.class);
        genderJob.setNumReduceTasks(1);
        FileInputFormat.addInputPath(genderJob, ratingsPath);
        FileOutputFormat.setOutputPath(genderJob, outStage);
        if (!genderJob.waitForCompletion(true)) {
            System.exit(1);
        }

        Path partFile = new Path(outStage, "part-r-00000");
        if (!FileUtil.copy(sys, partFile, sys, outPath, false, true, cfg)) {
            System.exit(1);
        }
    }
}
