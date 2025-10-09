package routines.advanced.clientbehaviorchipuse;

import java.io.IOException;
import org.apache.hadoop.io.*;
import org.apache.hadoop.mapreduce.Reducer;

public class StateAggCombiner extends Reducer<Text, StateClientAggWritable, Text, StateClientAggWritable> {

    @Override
    protected void reduce(Text state, Iterable<StateClientAggWritable> values, Context ctx)
            throws IOException, InterruptedException {
        StateClientAggWritable acc = new StateClientAggWritable();
        for (StateClientAggWritable v : values) {
            acc.add(v);
        }
        ctx.write(state, acc);
    }
}
