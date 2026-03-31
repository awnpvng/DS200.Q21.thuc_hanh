package task1;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileUtil;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;

public final class Driver {

    private Driver() {}

    public static void main(String[] args) throws Exception {
        if (args.length != 4) {
            System.err.println("Usage: Driver <merged_ratings> <movies.txt> <work_dir> <final_report.txt>");
            System.exit(1);
        }

        Path inPath = new Path(args[0]);
        Path moviesPath = new Path(args[1]);
        Path workPath = new Path(args[2]);
        Path outPath = new Path(args[3]);

        Configuration cfg = new Configuration();
        cfg.set("mapreduce.framework.name", "local");
        cfg.set("fs.defaultFS", "file:///");

        Path temp1 = new Path(workPath, "t1_stage1");
        Path temp2 = new Path(workPath, "t1_stage2");

        org.apache.hadoop.fs.FileSystem fs = org.apache.hadoop.fs.FileSystem.get(cfg);
        fs.delete(temp1, true);
        fs.delete(temp2, true);
        if (fs.exists(outPath)) {
            fs.delete(outPath, false);
        }

        Job aggregateJob = Job.getInstance(cfg, "Task 1 - Aggregation");
        aggregateJob.setJarByClass(Driver.class);
        aggregateJob.setMapperClass(RatingsMapper.class);
        aggregateJob.setReducerClass(RatingsReducer.class);
        aggregateJob.setMapOutputKeyClass(Text.class);
        aggregateJob.setMapOutputValueClass(Text.class);
        aggregateJob.setOutputKeyClass(Text.class);
        aggregateJob.setOutputValueClass(Text.class);
        aggregateJob.setNumReduceTasks(1);
        FileInputFormat.addInputPath(aggregateJob, inPath);
        FileOutputFormat.setOutputPath(aggregateJob, temp1);
        if (!aggregateJob.waitForCompletion(true)) {
            System.exit(1);
        }

        Job formatJob = Job.getInstance(cfg, "Task 1 - Formatting");
        formatJob.getConfiguration().set("mapreduce.output.textoutputformat.separator", "");
        formatJob.addCacheFile(moviesPath.toUri());
        formatJob.setJarByClass(Driver.class);
        formatJob.setMapperClass(ReportMapper.class);
        formatJob.setReducerClass(ReportReducer.class);
        formatJob.setMapOutputKeyClass(Text.class);
        formatJob.setMapOutputValueClass(Text.class);
        formatJob.setOutputKeyClass(NullWritable.class);
        formatJob.setOutputValueClass(Text.class);
        formatJob.setNumReduceTasks(1);
        FileInputFormat.addInputPath(formatJob, temp1);
        FileOutputFormat.setOutputPath(formatJob, temp2);
        formatJob.setOutputFormatClass(TextOutputFormat.class);
        if (!formatJob.waitForCompletion(true)) {
            System.exit(1);
        }

        if (!FileUtil.copy(fs, new Path(temp2, "part-r-00000"), fs, outPath, false, true, cfg)) {
            System.err.println("Failed to copy MR output to " + outPath);
            System.exit(1);
        }
    }
}
