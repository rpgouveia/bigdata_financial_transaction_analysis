package routines.advanced.rfmbyuf;

import java.io.IOException;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

/**
 * Lê linhas: STATE \t "1:low:med:high|CITY=1[,CITY2=x...]"
 * Emite:
 *   KEY: STATE
 *   VAL: StateClientAggWritable(total, low, med, high, mapaDeCidadesHigh)
 */
public class StateAggMapper extends Mapper<LongWritable, Text, Text, StateClientAggWritable> {

    private final Text outK = new Text();
    private final StateClientAggWritable outV = new StateClientAggWritable();

    @Override
    protected void map(LongWritable key, Text value, Context ctx) throws IOException, InterruptedException {
        String line = value.toString();
        int tab = line.indexOf('\t');
        if (tab < 0) return;

        String state = line.substring(0, tab).trim();
        String payload = line.substring(tab + 1).trim();
        if (state.isEmpty() || payload.isEmpty()) return;

        // payload: "1:low:med:high|CITY=1,CITY2=2"
        String[] parts = payload.split("\\|", 2);
        String counts = parts[0];
        String citiesPart = (parts.length > 1) ? parts[1] : "";

        String[] c = counts.split(":");
        if (c.length < 4) return;

        long total = parseL(c[0]);
        long low   = parseL(c[1]);
        long med   = parseL(c[2]);
        long high  = parseL(c[3]);

        outV.reset(total, low, med, high);

        if (!citiesPart.isEmpty()) {
            String[] kvs = citiesPart.split(",");
            for (String kv : kvs) {
                kv = kv.trim();
                if (kv.isEmpty()) continue;
                int eq = kv.indexOf('=');
                if (eq <= 0) continue;
                String city = kv.substring(0, eq).trim().toUpperCase();
                long cnt = parseL(kv.substring(eq + 1).trim());
                if (!city.isEmpty() && cnt > 0) {
                    outV.addHighCity(city, cnt);
                }
            }
        }

        outK.set(state);
        ctx.write(outK, outV);
    }

    private static long parseL(String s) {
        try { return Long.parseLong(s.trim()); } catch (Exception e) { return 0L; }
    }
}
