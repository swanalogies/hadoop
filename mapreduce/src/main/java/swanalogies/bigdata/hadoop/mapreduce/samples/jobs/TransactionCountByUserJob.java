package swanalogies.bigdata.hadoop.mapreduce.samples.jobs;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import swanalogies.bigdata.hadoop.mapreduce.samples.comparators.TransactionCountGroupingComparator;
import swanalogies.bigdata.hadoop.mapreduce.samples.comparators.TransactionCountSortComparator;
import swanalogies.bigdata.hadoop.mapreduce.samples.customkeyclasses.UserSourceCompositeKey;
import swanalogies.bigdata.hadoop.mapreduce.samples.mappers.TransactionCountMapper;
import swanalogies.bigdata.hadoop.mapreduce.samples.partitioners.TransactionCountPartitioner;
import swanalogies.bigdata.hadoop.mapreduce.samples.reducers.TransactionCountReducer;

public class TransactionCountByUserJob {
    public static void main(String[] args) throws Exception {
        //entry point for the job
        configureJob(args);
    }

    private static void configureJob(final String[] args) throws Exception {
        Configuration conf = new Configuration();
        Job job = Job.getInstance(conf, "transactionCountByUserSample");

        job.setJarByClass(TransactionCountByUserJob.class);

        job.setMapperClass(TransactionCountMapper.class);
        job.setMapOutputKeyClass(UserSourceCompositeKey.class);
        job.setMapOutputValueClass(IntWritable.class);

        //job.setCombinerClass(WordCountReducer.class);
        job.setPartitionerClass(TransactionCountPartitioner.class);
        job.setSortComparatorClass(TransactionCountSortComparator.class);
        job.setGroupingComparatorClass(TransactionCountGroupingComparator.class);

        job.setReducerClass(TransactionCountReducer.class);

        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(IntWritable.class);

        job.setInputFormatClass(TextInputFormat.class);

        FileInputFormat.setInputPaths(job, new Path(args[0]), new Path(args[1]));
        FileOutputFormat.setOutputPath(job, new Path(args[2]));

        System.exit(job.waitForCompletion(true) ? 0 : 1);
    }
}
