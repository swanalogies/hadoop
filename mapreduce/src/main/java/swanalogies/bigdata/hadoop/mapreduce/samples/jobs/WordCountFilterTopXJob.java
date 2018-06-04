package swanalogies.bigdata.hadoop.mapreduce.samples.jobs;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.KeyValueTextInputFormat;
import org.apache.hadoop.mapreduce.lib.jobcontrol.ControlledJob;
import org.apache.hadoop.mapreduce.lib.jobcontrol.JobControl;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;
import swanalogies.bigdata.hadoop.mapreduce.samples.combiners.TopXCombiner;
import swanalogies.bigdata.hadoop.mapreduce.samples.comparators.IntWritableDecreasingComparator;
import swanalogies.bigdata.hadoop.mapreduce.samples.mappers.InvertWordCountMapper;
import swanalogies.bigdata.hadoop.mapreduce.samples.mappers.WordCountMapper;
import swanalogies.bigdata.hadoop.mapreduce.samples.reducers.TopXWordsReducer;
import swanalogies.bigdata.hadoop.mapreduce.samples.reducers.WordCountReducer;

public class WordCountFilterTopXJob extends Configured implements Tool {
    @Override
    public int run(String[] args) throws Exception {

        JobControl jobControl = new JobControl("wordcountchaining");
        Configuration conf1 = getConf();
        Job job1 = Job.getInstance(conf1, "wordcount");
        job1.setJarByClass(WordCountFilterTopXJob.class);
        job1.setMapperClass(WordCountMapper.class);
        job1.setCombinerClass(WordCountReducer.class);
        job1.setReducerClass(WordCountReducer.class);
        job1.setOutputKeyClass(Text.class);
        job1.setOutputValueClass(IntWritable.class);

        FileInputFormat.addInputPath(job1, new Path(args[0]));
        FileOutputFormat.setOutputPath(job1, new Path(args[1]));

        ControlledJob controlledJob = new ControlledJob(conf1);
        controlledJob.setJob(job1);

        jobControl.addJob(controlledJob);

        Configuration conf2 = getConf();
        Job job2 = Job.getInstance(conf2, "TopXWordCount");
        job2.setNumReduceTasks(1);
        job2.setSortComparatorClass(IntWritableDecreasingComparator.class);

        job2.setJarByClass(WordCountFilterTopXJob.class);
        job2.setMapperClass(InvertWordCountMapper.class);
        job2.setCombinerClass(TopXCombiner.class);
        job2.setReducerClass(TopXWordsReducer.class);

        job2.setMapOutputKeyClass(IntWritable.class);
        job2.setMapOutputValueClass(Text.class);
        job2.setOutputKeyClass(Text.class);
        job2.setOutputValueClass(IntWritable.class);
        job2.setInputFormatClass(KeyValueTextInputFormat.class);

        FileInputFormat.addInputPath(job2, new Path(args[1]));
        FileOutputFormat.setOutputPath(job2, new Path(args[2]));

        ControlledJob controlledJob2 = new ControlledJob(conf2);
        controlledJob2.setJob(job2);

        controlledJob2.addDependingJob(controlledJob);
        jobControl.addJob(controlledJob2);

        jobControl.run();

        while (!jobControl.allFinished()) {
            try {
                Thread.sleep(5000);
            } catch (Exception ex) {
            }

        }


        return (job1.waitForCompletion(true) ? 1 : 0);

    }

    public static void main(String[] args) throws Exception {
        try {
            int exitCode = ToolRunner.run(new WordCountFilterTopXJob(), args);
            System.exit(exitCode);
        } catch (Exception ex) {

        }
    }
}
