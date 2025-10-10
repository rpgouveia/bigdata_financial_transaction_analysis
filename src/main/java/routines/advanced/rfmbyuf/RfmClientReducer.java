package routines.advanced.rfmbyuf;

import java.io.IOException;
import java.time.LocalDate;
import java.time.ZoneOffset;
import java.util.HashMap;
import java.util.Map;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

/**
 * Para cada client_id:
 *  - recencyDays = dias desde a última transação até rfm.reference.date (ou hoje UTC)
 *  - frequency   = número de transações
 *  - monetaryAvg = média em centavos
 * Bucket:
 *  HIGH se (recencyDays <= R_high) && (frequency >= F_high || monetaryAvg >= M_high)
 *  MED  se (recencyDays <= R_med)  || (frequency  >= F_med  || monetaryAvg >= M_med)
 *  LOW  caso contrário
 * Emite por UF predominante: KEY=<STATE>, VALUE="1:low:med:high|CITY=1" (CITY só se HIGH)
 */
public class RfmClientReducer extends Reducer<Text, TransactionRfmWritable, Text, Text> {

    private final Text outK = new Text();
    private final Text outV = new Text();

    @Override
    protected void reduce(Text clientId, Iterable<TransactionRfmWritable> values, Context ctx)
            throws IOException, InterruptedException {

        long tx = 0L;
        long sumCents = 0L;
        long lastTs = Long.MIN_VALUE;

        Map<String, Long> stateCount = new HashMap<>();
        Map<String, Long> cityCount  = new HashMap<>();

        for (TransactionRfmWritable v : values) {
            tx++;
            sumCents += v.getAmountCents();
            if (v.getTimestampMillis() > lastTs) lastTs = v.getTimestampMillis();

            stateCount.merge(nz(v.getState()), 1L, Long::sum);
            cityCount.merge(nz(v.getCity()), 1L, Long::sum);
        }
        if (tx == 0) return;

        String topState = topKey(stateCount);
        String topCity  = topKey(cityCount);

        long avgCents = sumCents / tx;

        // recencyDays: diferença entre referenceDate (UTC) e lastTs
        Configuration conf = ctx.getConfiguration();
        String ref = conf.get("rfm.reference.date", null);
        long refEpochDay;
        if (ref != null && ref.matches("\\d{4}-\\d{2}-\\d{2}")) {
            LocalDate ld = LocalDate.parse(ref);
            refEpochDay = ld.toEpochDay();
        } else {
            refEpochDay = LocalDate.now(ZoneOffset.UTC).toEpochDay();
        }
        long lastEpochDay = java.time.Instant.ofEpochMilli(lastTs).atZone(ZoneOffset.UTC).toLocalDate().toEpochDay();
        long recencyDays = Math.max(0, refEpochDay - lastEpochDay);

        // thresholds
        int  R_high = conf.getInt("rfm.recency.high_days", 30);
        int  R_med  = conf.getInt("rfm.recency.med_days", 90);
        long F_high = conf.getLong("rfm.freq.high", 12);
        long F_med  = conf.getLong("rfm.freq.med", 4);
        long M_high = conf.getLong("rfm.monetary.high_cents", 10000L);
        long M_med  = conf.getLong("rfm.monetary.med_cents", 4000L);

        String bucket = classify(recencyDays, tx, avgCents, R_high, R_med, F_high, F_med, M_high, M_med);

        if (topState != null && !topState.isEmpty()) {
            outK.set(topState);
            outV.set(serialize(bucket, topCity));
            ctx.write(outK, outV);
        }
    }

    private static String classify(long recencyDays, long freq, long avgCents,
                                   int R_high, int R_med, long F_high, long F_med, long M_high, long M_med) {
        boolean HIGH = (recencyDays <= R_high) &&
                (freq >= F_high || avgCents >= M_high);
        if (HIGH) return "HIGH";

        boolean MED = (recencyDays <= R_med) ||
                (freq >= F_med) || (avgCents >= M_med);
        return MED ? "MED" : "LOW";
    }

    private static String serialize(String bucket, String cityIfHigh) {
        long low=0, med=0, high=0;
        if ("LOW".equals(bucket)) low=1; else if ("MED".equals(bucket)) med=1; else high=1;
        StringBuilder sb = new StringBuilder();
        sb.append(1).append(":").append(low).append(":").append(med).append(":").append(high).append("|");
        if ("HIGH".equals(bucket) && cityIfHigh != null && !cityIfHigh.isEmpty()) {
            sb.append(cityIfHigh).append("=1");
        }
        return sb.toString();
    }

    private static String topKey(Map<String, Long> m) {
        String best=""; long bv=-1;
        for (Map.Entry<String, Long> e : m.entrySet()) {
            if (e.getValue() > bv) { bv=e.getValue(); best=e.getKey(); }
        }
        return best;
    }
    private static String nz(String s){ return (s==null||s.trim().isEmpty())?"UNKNOWN":s.trim().toUpperCase(); }
}
