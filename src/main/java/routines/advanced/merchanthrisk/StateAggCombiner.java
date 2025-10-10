package routines.advanced.merchanthrisk;

import java.io.IOException;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

/**
 * Combiner (JOB 2): merge local (associativo) de StateMerchantAggWritable
 *
 * Benefício:
 *  - Reduz tráfego de rede somando contadores e juntando mapas/top-K ainda no nó.
 */
public class StateAggCombiner extends Reducer<Text, StateMerchantAggWritable, Text, StateMerchantAggWritable> {

    private int topK;

    @Override
    protected void setup(Context ctx) {
        Configuration conf = ctx.getConfiguration();
        topK = conf.getInt("top.merchants.k", 5);
    }

    @Override
    protected void reduce(Text uf, Iterable<StateMerchantAggWritable> vals, Context ctx)
            throws IOException, InterruptedException {

        StateMerchantAggWritable acc = new StateMerchantAggWritable();
        acc.setK(topK);

        for (StateMerchantAggWritable v : vals) {
            acc.merge(v);
        }
        ctx.write(uf, acc);
    }
}
