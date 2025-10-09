package routines.advanced.clientbehaviorchipuse;

import java.io.IOException;
import java.util.StringTokenizer;
import org.apache.hadoop.io.*;
import org.apache.hadoop.mapreduce.Mapper;

/**
 * Lê a saída textual do Job 1
 *   No Job1, StateClientAggWritable será materializado via toText() no reducer (ctx.write(Text, Text)).
 *   -> Para isso, ajustamos o Job1: outputValueClass=Text e escreveremos "total:low:med:high|cityA=3,cityB=1"
 */
public class StateAggMapper extends Mapper<LongWritable, Text, Text, StateClientAggWritable> {

    private Text outKey = new Text();

    @Override
    protected void map(LongWritable key, Text value, Context ctx) throws IOException, InterruptedException {
        // Espera: STATE \t total:low:med:high|city1=x,city2=y
        String line = value.toString();
        int tab = line.indexOf('\t');
        if (tab < 0) return;

        String state = line.substring(0, tab).trim();
        String payload = line.substring(tab + 1).trim();

        // split lado esquerdo e mapa de cidades
        String[] parts = payload.split("\\|", 2);
        String[] counts = parts[0].split(":");
        if (counts.length != 4) return;

        long total = Long.parseLong(counts[0]);
        long low   = Long.parseLong(counts[1]);
        long med   = Long.parseLong(counts[2]);
        long high  = Long.parseLong(counts[3]);

        java.util.Map<String, Long> hrCity = new java.util.HashMap<>();
        if (parts.length == 2 && !parts[1].isEmpty()) {
            StringTokenizer tok = new StringTokenizer(parts[1], ",");
            while (tok.hasMoreTokens()) {
                String kv = tok.nextToken();
                int eq = kv.indexOf('=');
                if (eq > 0) {
                    String city = kv.substring(0, eq).trim();
                    long c = Long.parseLong(kv.substring(eq+1).trim());
                    if (!city.isEmpty()) hrCity.put(city, c);
                }
            }
        }

        StateClientAggWritable w = new StateClientAggWritable(total, low, med, high, hrCity);
        outKey.set(state);
        ctx.write(outKey, w);
    }
}
