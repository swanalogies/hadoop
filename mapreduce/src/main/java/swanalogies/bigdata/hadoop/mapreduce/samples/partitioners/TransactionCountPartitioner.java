package swanalogies.bigdata.hadoop.mapreduce.samples.partitioners;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.mapreduce.Partitioner;
import swanalogies.bigdata.hadoop.mapreduce.samples.customkeyclasses.UserSourceCompositeKey;

public class TransactionCountPartitioner extends Partitioner<UserSourceCompositeKey, IntWritable> {
    @Override
    public int getPartition(UserSourceCompositeKey key, IntWritable intWritable, int numOfPartitions) {
        //partition only on the basis of user-id field, since we want the data pertaining to the same user-id to reach to the same reducer.
        return key.user.hashCode() % numOfPartitions;
    }
}
