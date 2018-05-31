package swanalogies.bigdata.hadoop.mapreduce.samples.mappers;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.lib.input.FileSplit;
import swanalogies.bigdata.hadoop.mapreduce.samples.customkeyclasses.UserSourceCompositeKey;

import java.io.IOException;

public class TransactionCountMapper extends Mapper<Object, Text, UserSourceCompositeKey, IntWritable> {
    private int source = -1;

    @Override
    protected void map(Object key, Text value, Context context) throws IOException, InterruptedException {
        UserSourceCompositeKey compositeKey = new UserSourceCompositeKey();
        String[] fields = value.toString().split(",");

        if (source == 0) {
            //input file was card members
            compositeKey.user = fields[0];
        } else {
            compositeKey.user = fields[4];
        }
        compositeKey.source = source;
        context.write(compositeKey, new IntWritable(source));

    }

    @Override
    protected void setup(Context context) throws IOException, InterruptedException {
        FileSplit fileSplit = (FileSplit) context.getInputSplit();
        if (fileSplit.getPath().getName().contains("cardMembers")) {
            source = 0;
        } else if (fileSplit.getPath().getName().contains("transactions")) {
            source = 1;
        }
    }
}
