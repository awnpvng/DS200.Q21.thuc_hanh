package task2;

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
        if (args.length != 4) {
            System.err.println("Usage: Driver <merged_ratings> <movies.txt> <work_dir> <final_report.txt>");
            System.exit(1);
        }

        Path ratingsPath = new Path(args[0]);
        Path cachePath = new Path(args[1]);
        Path tempPath = new Path(args[2]);
        Path finalPath = new Path(args[3]);

        Configuration cfg = new Configuration();
        cfg.set("mapreduce.framework.name", "local");
        cfg.set("fs.defaultFS", "file:///");

        Path outStage = new Path(tempPath, "t2_worker");
        FileSystem sys = FileSystem.get(cfg);
        sys.delete(outStage, true);
        if (sys.exists(finalPath)) {
            sys.delete(finalPath, false);
        }

        Job explodeJob = Job.getInstance(cfg, "Task 2 - Genre Averages");
        explodeJob.getConfiguration().set("mapreduce.output.textoutputformat.separator", "");
        explodeJob.addCacheFile(cachePath.toUri());
        explodeJob.setJarByClass(Driver.class);
        explodeJob.setMapperClass(Mapper.class);
        explodeJob.setReducerClass(Reducer.class);
        explodeJob.setMapOutputKeyClass(Text.class);
        explodeJob.setMapOutputValueClass(Text.class);
        explodeJob.setOutputKeyClass(NullWritable.class);
        explodeJob.setOutputValueClass(Text.class);
        explodeJob.setNumReduceTasks(1);
        FileInputFormat.addInputPath(explodeJob, ratingsPath);
        FileOutputFormat.setOutputPath(explodeJob, outStage);
        if (!explodeJob.waitForCompletion(true)) {
            System.exit(1);
        }

        Path partFile = new Path(outStage, "part-r-00000");
        if (!FileUtil.copy(sys, partFile, sys, finalPath, false, true, cfg)) {
            System.exit(1);
        }
    }
}
