package swanalogies.bigdata.hadoop.mapreduce.samples.mappers;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;

public class InvertWordCountMapper extends Mapper<Text, Text, IntWritable, Text> {
    private IntWritable wordCount = new IntWritable();

    @Override
    protected void map(Text key, Text value, Context context) throws IOException, InterruptedException {
        int count = Integer.parseInt(value.toString());
        wordCount.set(count);
        context.write(wordCount, key);
    }
}
