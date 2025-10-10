package routines.advanced.merchanthrisk;

import java.io.IOException;
import java.util.*;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

/**
 * Reducer (JOB 2): consolida por UF e emite linha final legível
 *
 * Saída por UF (exemplo):
 *   SP    Merchants: 123 | Health A:40 (32.52%) | B:55 (44.71%) | C:28 (22.76%) |
 *         Risk Low:80 (65.04%) | Med:30 (24.39%) | High:13 (10.57%) |
 *         Top Merchants: MER123:$10,500.00 | MER456:$8,200.00 | ...
 *         High-Risk Hotspots: SAO PAULO: 7 | CAMPINAS: 3 | ...
 *
 * Notas:
 *  - Aplica supressão opcional de UF com poucos merchants (-Dmin.uf.merchants).
 *  - Ordena Top-K e Hotspots para exibição.
 */
public class StateAggReducer extends Reducer<Text, StateMerchantAggWritable, Text, Text> {

    private final Text outVal = new Text();
    private int topK;
    private int minUfMerchants;

    @Override
    protected void setup(Context ctx) {
        Configuration conf = ctx.getConfiguration();
        topK = conf.getInt("top.merchants.k", 5);
        minUfMerchants = conf.getInt("min.uf.merchants", 5);
    }

    @Override
    protected void reduce(Text uf, Iterable<StateMerchantAggWritable> vals, Context ctx)
            throws IOException, InterruptedException {

        StateMerchantAggWritable acc = new StateMerchantAggWritable();
        acc.setK(topK);

        for (StateMerchantAggWritable v : vals) {
            acc.merge(v);
        }

        long T = acc.getTotalMerchants();
        if (T < minUfMerchants) return; // opcional: evita ruído estatístico

        long A = acc.getHealthA(), B = acc.getHealthB(), C = acc.getHealthC();
        long RL = acc.getRiskLow(), RM = acc.getRiskMed(), RH = acc.getRiskHigh();

        String pctA = pct(A, T), pctB = pct(B, T), pctC = pct(C, T);
        String pctRL= pct(RL, T), pctRM= pct(RM, T), pctRH= pct(RH, T);

        // Top-K merchants por valor (ordena desc para exibir)
        List<String> ids = acc.getTopIds();
        List<Long>   vs  = acc.getTopVals();
        List<Integer> idx = new ArrayList<>();
        for (int i = 0; i < ids.size(); i++) idx.add(i);
        idx.sort((i,j) -> Long.compare(vs.get(j), vs.get(i)));

        StringBuilder topMer = new StringBuilder();
        for (int r = 0; r < Math.min(topK, idx.size()); r++) {
            if (r > 0) topMer.append(" | ");
            int p = idx.get(r);
            topMer.append(ids.get(p)).append(":$").append(formatDollars(vs.get(p)));
        }

        // Hotspots (cidades com mais merchants HIGH)
        List<Map.Entry<String,Integer>> cities = new ArrayList<>(acc.getHighRiskCityCounts().entrySet());
        cities.sort((a,b) -> b.getValue().compareTo(a.getValue()));
        StringBuilder hot = new StringBuilder();
        for (int i = 0; i < Math.min(5, cities.size()); i++) {
            if (i > 0) hot.append(" | ");
            hot.append(cities.get(i).getKey()).append(": ").append(cities.get(i).getValue());
        }

        String out = String.format(
                "Merchants: %d | Health A:%d (%s) | B:%d (%s) | C:%d (%s) | " +
                        "Risk Low:%d (%s) | Med:%d (%s) | High:%d (%s) | " +
                        "Top Merchants: %s | High-Risk Hotspots: %s",
                T, A, pctA, B, pctB, C, pctC,
                RL, pctRL, RM, pctRM, RH, pctRH,
                topMer.toString(), hot.toString()
        );

        outVal.set(out);
        ctx.write(uf, outVal);
    }

    // ===== utilitários de formatação =====
    private static String pct(long x, long T) {
        return (T > 0) ? String.format("%.2f%%", (x * 100.0) / T) : "0.00%";
    }
    private static String formatDollars(long cents) {
        return String.format("%,.2f", cents / 100.0);
    }
}
