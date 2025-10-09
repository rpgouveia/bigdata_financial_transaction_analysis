package routines.advanced.clientbehaviorchipuse;

import java.io.IOException;
import java.util.HashMap;
import java.util.Map;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

/**
 * Job 1 - Reducer (versão canal online/swipe):
 *   - agrega por client_id
 *   - calcula onlineRate, errorRate, avg, max
 *   - classifica LOW/MED/HIGH com thresholds configuráveis
 *   - encontra UF/cidade predominantes
 *   - emite por UF (um cliente = uma linha textual):
 *       KEY:   <STATE>
 *       VALUE: "1:low:med:high|CITY=1" (CITY só se HIGH e houver cidade predominante)
 */
public class ClientAggReducer extends Reducer<Text, TransactionMiniWritable, Text, Text> {

    private final Text outKey = new Text();
    private final Text outVal = new Text();

    @Override
    protected void reduce(Text clientId, Iterable<TransactionMiniWritable> values, Context ctx)
            throws IOException, InterruptedException {

        long tx = 0L;
        long onlineCount = 0L;
        long errors = 0L;
        long sumCents = 0L;
        long maxCents = 0L;

        Map<String, Long> stateCount = new HashMap<>();
        Map<String, Long> cityCount  = new HashMap<>();
        Map<String, Long> mccCount   = new HashMap<>();

        for (TransactionMiniWritable v : values) {
            tx++;
            if (v.isOnline())  onlineCount++;
            if (v.isHasError()) errors++;
            long a = v.getAmountCents();
            sumCents += a;
            if (a > maxCents) maxCents = a;

            stateCount.merge(nz(v.getState()), 1L, Long::sum);
            cityCount.merge(nz(v.getCity()),   1L, Long::sum);
            mccCount.merge(nz(v.getMcc()),     1L, Long::sum);
        }

        if (tx == 0) return;

        String topState = topKey(stateCount);
        String topCity  = topKey(cityCount);

        double onlineRate = onlineCount * 1.0 / tx;
        double errorRate  = errors      * 1.0 / tx;
        long   avgCents   = sumCents / tx;

        Configuration conf = ctx.getConfiguration();
        float errHigh = conf.getFloat("risk.error.high", 0.05f);
        float errMed  = conf.getFloat("risk.error.med",  0.02f);
        float onHigh  = conf.getFloat("risk.online.high", 0.80f); // muito online
        float onMed   = conf.getFloat("risk.online.med",  0.60f); // moderadamente online
        long  avgHigh = conf.getLong ("risk.avg_amount.high_cents", 10000L);
        long  maxHigh = conf.getLong ("risk.max_amount.high_cents", 50000L);

        String bucket = classifyBucket(
                onlineRate, errorRate, avgCents, maxCents,
                errHigh, errMed, onHigh, onMed, avgHigh, maxHigh
        );

        if (topState != null && !topState.isEmpty()) {
            outKey.set(topState);
            outVal.set(serializeSingleClient(bucket, topCity));
            ctx.write(outKey, outVal);
        }
    }

    private static String serializeSingleClient(String bucket, String city) {
        long low = 0, med = 0, high = 0;
        if ("LOW".equals(bucket))      low  = 1;
        else if ("MED".equals(bucket)) med  = 1;
        else                           high = 1;

        StringBuilder sb = new StringBuilder();
        sb.append(1).append(":").append(low).append(":").append(med).append(":").append(high).append("|");
        if ("HIGH".equals(bucket) && city != null && !city.isEmpty()) {
            sb.append(city).append("=1");
        }
        return sb.toString();
    }

    private static String classifyBucket(double onlineRate, double errorRate, long avgCents, long maxCents,
                                         float errHigh, float errMed, float onHigh, float onMed,
                                         long avgHigh, long maxHigh) {
        boolean highRisk =
                (errorRate >= errHigh && onlineRate >= onMed) ||
                        (onlineRate >= onHigh) ||
                        (avgCents >= avgHigh) ||
                        (maxCents >= maxHigh);

        if (highRisk) return "HIGH";

        boolean medium =
                (errorRate >= errMed) ||
                        (onlineRate >= onMed);

        return medium ? "MED" : "LOW";
    }

    private static String topKey(Map<String, Long> map) {
        String best = "";
        long bestV = -1L;
        for (Map.Entry<String, Long> e : map.entrySet()) {
            if (e.getValue() > bestV) {
                bestV = e.getValue();
                best = e.getKey();
            }
        }
        return best;
    }

    private static String nz(String s) {
        return (s == null || s.trim().isEmpty()) ? "UNKNOWN" : s.trim().toUpperCase();
    }
}
