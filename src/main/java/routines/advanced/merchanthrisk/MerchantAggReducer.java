package routines.advanced.merchanthrisk;

import java.io.IOException;
import java.util.HashMap;
import java.util.Map;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

/**
 * Reducer (JOB 1): agrega por merchant_id, calcula métricas e classifica
 *
 * Para cada merchant_id:
 *  - Soma transações, total em centavos, pico (max), taxa de erro e taxa online
 *  - Descobre UF e cidade predominantes (maior frequência)
 *  - Classifica:
 *      HEALTH: A/B/C (por receita e, opcionalmente, avg com mínimo de volume)
 *      RISK  : LOW/MED/HIGH (erros, share online, pico)
 *  - Emite 1 linha textual por UF predominante:
 *      KEY  = <UF>
 *      VALUE= "1:A:B:C|RL:RM:RH|[CITY=<topCity>=1|]MER=<merchant_id>:<sumCents>"
 *
 * Observação:
 *  - Escolhemos formato textual para facilitar cat/grep na saída intermediária.
 *  - JOB 2 faz o parsing e reconstrói o agregado por UF.
 */
public class MerchantAggReducer extends Reducer<Text, TransactionMiniWritable, Text, Text> {

    private final Text outKey = new Text();
    private final Text outVal = new Text();

    @Override
    protected void reduce(Text merchantId, Iterable<TransactionMiniWritable> values, Context ctx)
            throws IOException, InterruptedException {

        long tx = 0, sumCents = 0, maxCents = 0, errors = 0, onlineTx = 0;
        Map<String, Long> stateCount = new HashMap<>();
        Map<String, Long> cityCount  = new HashMap<>();

        // Agregação por merchant
        for (TransactionMiniWritable v : values) {
            long a = v.getAmountCents();
            if (a == Long.MIN_VALUE) continue;
            tx++;
            sumCents += a;
            if (a > maxCents) maxCents = a;
            if (v.isOnline())   onlineTx++;
            if (v.isHasError()) errors++;

            stateCount.merge(nz(v.getState()), 1L, Long::sum);
            cityCount.merge(nz(v.getCity()),   1L, Long::sum);
        }
        if (tx == 0) return;

        String topState = topKey(stateCount);
        String topCity  = topKey(cityCount);
        if (topState == null || topState.isEmpty()) return;

        double errorRate  = errors   * 1.0 / tx;
        double onlineRate = onlineTx * 1.0 / tx;
        long avgCents     = sumCents / tx;

        // Thresholds (parametrizáveis via -D)
        Configuration conf = ctx.getConfiguration();
        long revenueMed  = conf.getLong ("health.revenue.med_cents",  500000L);
        long revenueHigh = conf.getLong ("health.revenue.high_cents", 2000000L);
        long avgMed      = conf.getLong ("health.avg.med_cents", 8000L);
        int  txMinForAvg = conf.getInt  ("health.tx.min_for_avg", 100);

        float errMed  = conf.getFloat("risk.error.med",  0.02f);
        float errHigh = conf.getFloat("risk.error.high", 0.05f);
        float onMed   = conf.getFloat("risk.online.med", 0.70f);
        float onHigh  = conf.getFloat("risk.online.high",0.90f);
        long  maxHigh = conf.getLong ("risk.max.high_cents", 500000L);

        // Classificações
        String health = classifyHealth(sumCents, avgCents, tx, revenueMed, revenueHigh, avgMed, txMinForAvg);
        String risk   = classifyRisk(errorRate, onlineRate, maxCents, errMed, errHigh, onMed, onHigh, maxHigh);

        // Serialização do merchant (1 linha)
        StringBuilder sb = new StringBuilder();
        sb.append(serializeHealth(health)).append("|")
                .append(serializeRisk(risk));
        if ("HIGH".equals(risk) && topCity != null && !topCity.isEmpty()) {
            sb.append("|CITY=").append(topCity).append("=1");
        }
        sb.append("|MER=").append(merchantId.toString()).append(":").append(sumCents);

        outKey.set(topState);
        outVal.set(sb.toString());
        ctx.write(outKey, outVal);
    }

    // ===== Helpers de serialização / regras =====
    private static String serializeHealth(String health) {
        long A=0,B=0,C=0;
        if ("A".equals(health)) A=1; else if ("B".equals(health)) B=1; else C=1;
        return "1:" + A + ":" + B + ":" + C;
    }
    private static String serializeRisk(String risk) {
        long rl=0, rm=0, rh=0;
        if ("LOW".equals(risk)) rl=1; else if ("MED".equals(risk)) rm=1; else rh=1;
        return rl + ":" + rm + ":" + rh;
    }

    private static String classifyHealth(long sumCents, long avgCents, long tx,
                                         long revenueMed, long revenueHigh,
                                         long avgMed, int txMinForAvg) {
        if (sumCents >= revenueHigh) return "A";
        if (sumCents >= revenueMed)  return "B";
        if (tx >= txMinForAvg && avgCents >= avgMed) return "B";
        return "C";
    }

    private static String classifyRisk(double errorRate, double onlineRate, long maxCents,
                                       float errMed, float errHigh, float onMed, float onHigh, long maxHigh) {
        boolean high = (errorRate >= errHigh) || (onlineRate >= onHigh) || (maxCents >= maxHigh);
        if (high) return "HIGH";
        boolean med  = (errorRate >= errMed)  || (onlineRate >= onMed);
        return med ? "MED" : "LOW";
    }

    private static String topKey(Map<String, Long> map) {
        String best = ""; long val = -1;
        for (Map.Entry<String, Long> e : map.entrySet()) {
            if (e.getValue() > val) { val = e.getValue(); best = e.getKey(); }
        }
        return best;
    }
    private static String nz(String s) { return (s == null) ? "" : s.trim().toUpperCase(); }
}
