package swanalogies.bigdata.hadoop.mapreduce.samples.comparators;

import org.apache.hadoop.io.WritableComparable;
import org.apache.hadoop.io.WritableComparator;
import swanalogies.bigdata.hadoop.mapreduce.samples.customkeyclasses.UserSourceCompositeKey;

public class TransactionCountGroupingComparator extends WritableComparator {
    protected  TransactionCountGroupingComparator() {
        super(UserSourceCompositeKey.class, true);
    }

    @Override
    public int compare(WritableComparable a, WritableComparable b) {
        UserSourceCompositeKey key1 = (UserSourceCompositeKey) a;
        UserSourceCompositeKey key2 = (UserSourceCompositeKey) b;

        return key1.user.compareTo(key2.user);
    }
}
