package routines.advanced.rfmbyuf;

import java.io.IOException;
import java.util.*;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

/**
 * Consolida por UF:
 *  - soma total/low/med/high
 *  - ordena mapa de cidades (clientes HIGH) e pega Top N (-Dtop.cities, default 5)
 * Emite Text legível:
 *   STATE \t "Clients: T | Low: x (p%) | Med: y (p%) | High: z (p%) | Top Cities (High): C1:n1 | C2:n2 | ..."
 */
public class StateAggReducer extends Reducer<Text, StateClientAggWritable, Text, Text> {

    private final Text outV = new Text();

    @Override
    protected void reduce(Text state, Iterable<StateClientAggWritable> values, Context ctx)
            throws IOException, InterruptedException {

        long total=0, low=0, med=0, high=0;
        Map<String, Long> cityHigh = new HashMap<>();

        for (StateClientAggWritable v : values) {
            total += v.getTotal();
            low   += v.getLow();
            med   += v.getMed();
            high  += v.getHigh();
            for (Map.Entry<String, Long> e : v.getHighCities().entrySet()) {
                cityHigh.merge(e.getKey(), e.getValue(), Long::sum);
            }
        }

        double pLow = pct(low, total);
        double pMed = pct(med, total);
        double pHigh= pct(high, total);

        int topN = ctx.getConfiguration().getInt("top.cities", 5);
        List<Map.Entry<String, Long>> top = new ArrayList<>(cityHigh.entrySet());
        top.sort((a,b)-> Long.compare(b.getValue(), a.getValue()));
        if (top.size() > topN) top = top.subList(0, topN);

        StringBuilder sb = new StringBuilder();
        sb.append("Clients: ").append(total)
                .append(" | Low: ").append(low).append(String.format(" (%.2f%%)", pLow))
                .append(" | Med: ").append(med).append(String.format(" (%.2f%%)", pMed))
                .append(" | High: ").append(high).append(String.format(" (%.2f%%)", pHigh))
                .append(" | Top Cities (High): ");

        for (int i=0;i<top.size();i++){
            if (i>0) sb.append(" | ");
            sb.append(top.get(i).getKey()).append(": ").append(top.get(i).getValue());
        }

        outV.set(sb.toString());
        ctx.write(state, outV);
    }

    private static double pct(long x, long T){ return (T>0)? (x*100.0/T) : 0.0; }
}
