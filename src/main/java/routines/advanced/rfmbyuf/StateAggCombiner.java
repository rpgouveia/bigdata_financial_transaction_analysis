package routines.advanced.rfmbyuf;

import java.io.IOException;
import java.util.HashMap;
import java.util.Map;
import java.util.Map.Entry;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

/**
 * Combiner: soma contadores e faz merge do mapa de cidades (HIGH) em uma única passada.
 */
public class StateAggCombiner extends Reducer<Text, StateClientAggWritable, Text, StateClientAggWritable> {

    private final StateClientAggWritable outV = new StateClientAggWritable();

    @Override
    protected void reduce(Text state, Iterable<StateClientAggWritable> values, Context ctx)
            throws IOException, InterruptedException {

        long total = 0L, low = 0L, med = 0L, high = 0L;

        // Acumula cidades HIGH numa estrutura temporária
        Map<String, Long> mergedCities = new HashMap<String, Long>();

        // ÚNICA passada pelo Iterable
        for (StateClientAggWritable v : values) {
            total += v.getTotal();
            low   += v.getLow();
            med   += v.getMed();
            high  += v.getHigh();

            for (Entry<String, Long> e : v.getHighCities().entrySet()) {
                String city = e.getKey();
                Long cnt = e.getValue();
                if (city != null && !city.isEmpty() && cnt != null && cnt > 0) {
                    Long prev = mergedCities.get(city);
                    mergedCities.put(city, (prev == null ? cnt : prev + cnt));
                }
            }
        }

        // Prepara o objeto de saída
        outV.reset(total, low, med, high);
        for (Entry<String, Long> e : mergedCities.entrySet()) {
            outV.addHighCity(e.getKey(), e.getValue());
        }

        ctx.write(state, outV);
    }
}
