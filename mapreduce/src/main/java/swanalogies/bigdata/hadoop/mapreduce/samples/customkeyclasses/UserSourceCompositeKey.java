package swanalogies.bigdata.hadoop.mapreduce.samples.customkeyclasses;

import org.apache.hadoop.io.WritableComparable;
import org.apache.hadoop.io.WritableUtils;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

public class UserSourceCompositeKey implements WritableComparable<UserSourceCompositeKey> {
    public String user;
    public int source;

    public UserSourceCompositeKey() {
        user = null;
        source = -1;
    }

    public UserSourceCompositeKey(String user, int source) {
        this.user = user;
        this.source = source;
    }

    @Override
    public int compareTo(UserSourceCompositeKey input) {
        int result = this.user.compareTo(input.user);
        if (result == 0) {
            result = Integer.compare(this.source, input.source);
        }
        return result;
    }

    @Override
    public void write(DataOutput dataOutput) throws IOException {
        WritableUtils.writeString(dataOutput, this.user);
        WritableUtils.writeVInt(dataOutput, this.source);
    }

    @Override
    public void readFields(DataInput dataInput) throws IOException {
        this.user = WritableUtils.readString(dataInput);
        this.source = WritableUtils.readVInt(dataInput);
    }
}
