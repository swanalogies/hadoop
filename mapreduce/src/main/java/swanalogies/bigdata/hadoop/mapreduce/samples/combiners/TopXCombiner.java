package swanalogies.bigdata.hadoop.mapreduce.samples.combiners;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;

public class TopXCombiner extends Reducer<IntWritable, Text, IntWritable, Text> {
    private int topX, counter;

    @Override
    protected void setup(Context context) throws IOException, InterruptedException {
        super.setup(context);
        topX = 2;
        counter = 0;
    }

    @Override
    protected void reduce(IntWritable key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
        if (counter < topX) {
            for (Text value : values) {
                context.write(key, value);
                counter++;
            }
        }

    }
}
