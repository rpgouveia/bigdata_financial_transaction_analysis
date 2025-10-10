package routines.advanced.merchanthrisk;

import java.io.IOException;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

/**
 * Mapper (JOB 2): lê a linha textual do JOB 1 e reconstrói o agregado por UF
 *
 * Formato da linha do JOB 1 (value):
 *   "1:A:B:C|RL:RM:RH|[CITY=<city>=1|]MER=<merchant_id>:<sumCents>"
 *
 * Parsea:
 *   - bucket de HEALTH (A/B/C) a partir de "1:A:B:C"
 *   - bucket de RISK (LOW/MED/HIGH) a partir de "RL:RM:RH"
 *   - cidade (se veio marcado e risco foi HIGH)
 *   - par (MER=<id>:<soma_em_centavos>) para alimentar top-K por UF
 */
public class StateAggMapper extends Mapper<Object, Text, Text, StateMerchantAggWritable> {

    private final Text outKey = new Text();
    private int topK;

    @Override
    protected void setup(Context ctx) {
        Configuration conf = ctx.getConfiguration();
        topK = conf.getInt("top.merchants.k", 5);
    }

    @Override
    protected void map(Object key, Text value, Context ctx) throws IOException, InterruptedException {
        String line = value.toString();
        int tab = line.indexOf('\t');
        if (tab <= 0) return;

        String uf = line.substring(0, tab).trim();
        String payload = line.substring(tab + 1).trim();
        String[] parts = payload.split("\\|");
        if (parts.length < 3) return;

        String healthStr = parts[0];          // "1:A:B:C"
        String riskStr   = parts[1];          // "RL:RM:RH"

        String city = null;
        String mer  = null;
        long sum    = 0L;

        // percorre blocos opcionais CITY e obrigatório MER
        for (String p : parts) {
            if (p.startsWith("MER=")) {
                String m = p.substring(4);
                int col = m.lastIndexOf(':');
                if (col > 0) {
                    mer = m.substring(0, col);
                    try { sum = Long.parseLong(m.substring(col + 1)); } catch (Exception ignore) {}
                }
            } else if (p.startsWith("CITY=")) {
                String c = p.substring(5);
                int eq = c.lastIndexOf('=');
                city = (eq > 0) ? c.substring(0, eq) : c;
            }
        }

        String healthBucket = parseHealthBucket(healthStr);
        String riskBucket   = parseRiskBucket(riskStr);
        if (healthBucket == null || riskBucket == null || mer == null) return;

        StateMerchantAggWritable agg = new StateMerchantAggWritable();
        agg.setK(topK);
        agg.addOneMerchant(healthBucket, riskBucket, city, mer, sum);

        outKey.set(uf);
        ctx.write(outKey, agg);
    }

    // Helpers de parsing dos buckets
    private static String parseHealthBucket(String s) {
        String[] t = s.split(":"); // "1:A:B:C"
        if (t.length != 4) return null;
        long a = parseLong(t[1]), b = parseLong(t[2]), c = parseLong(t[3]);
        if (a == 1) return "A";
        if (b == 1) return "B";
        if (c == 1) return "C";
        return null;
    }

    private static String parseRiskBucket(String s) {
        String[] t = s.split(":"); // "RL:RM:RH"
        if (t.length != 3) return null;
        long rl = parseLong(t[0]), rm = parseLong(t[1]), rh = parseLong(t[2]);
        if (rl == 1) return "LOW";
        if (rm == 1) return "MED";
        if (rh == 1) return "HIGH";
        return null;
    }

    private static long parseLong(String x) {
        try { return Long.parseLong(x.trim()); } catch (Exception e) { return 0L; }
    }
}
