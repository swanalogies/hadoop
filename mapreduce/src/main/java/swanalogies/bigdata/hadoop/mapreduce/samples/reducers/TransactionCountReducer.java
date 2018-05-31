package swanalogies.bigdata.hadoop.mapreduce.samples.reducers;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;
import swanalogies.bigdata.hadoop.mapreduce.samples.customkeyclasses.UserSourceCompositeKey;

import java.io.IOException;

public class TransactionCountReducer extends Reducer<UserSourceCompositeKey, IntWritable, Text, IntWritable> {
    @Override
    protected void reduce(UserSourceCompositeKey key, Iterable<IntWritable> values, Context context) throws IOException, InterruptedException {
        int count = -1;
        for (IntWritable value : values) {
            count++;
        }
        context.write(new Text(key.user), new IntWritable(count));
    }
}
