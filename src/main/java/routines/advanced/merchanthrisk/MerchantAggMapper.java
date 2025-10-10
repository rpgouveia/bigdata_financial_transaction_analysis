package routines.advanced.merchanthrisk;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

/**
 * Mapper (JOB 1): lê CSV e emite (merchant_id, TransactionMiniWritable)
 *
 * CSV esperado (12 colunas):
 *  id(0), date(1), client_id(2), card_id(3), amount(4), use_chip(5),
 *  merchant_id(6), merchant_city(7), merchant_state(8), zip(9), mcc(10), errors(11)
 *
 * Notas:
 *  - Ignora cabeçalho.
 *  - Split CSV que respeita aspas.
 *  - Converte amount para centavos (long) e normaliza Online vs Swipe.
 */
public class MerchantAggMapper extends Mapper<LongWritable, Text, Text, TransactionMiniWritable> {

    private final Text outKey = new Text();

    @Override
    protected void map(LongWritable key, Text value, Context ctx) throws IOException, InterruptedException {
        String line = value.toString();

        // Cabeçalho?
        if (line.startsWith("id,") || line.startsWith("\"id\"")) return;

        String[] parts = splitCsv(line);
        if (parts.length < 12) return;

        // Campos relevantes
        String merchantId = nz(parts[6]);
        if (merchantId.isEmpty()) return;

        long amountCents = parseAmountToCents(parts[4]);
        String city  = nz(parts[7]);
        String state = nz(parts[8]);
        String mcc   = nz(parts[10]);

        // "use_chip": agora interpretamos como canal
        // ONLINE TRANSACTION = online; SWIPE TRANSACTION = presencial
        boolean isOnline = isOnline(nz(parts[5]));

        // errors(11): qualquer flag não vazia/None consideramos erro
        boolean hasError = hasError(nz(parts[11]));

        TransactionMiniWritable tw = new TransactionMiniWritable(
                amountCents, isOnline, hasError, city, state, mcc
        );

        outKey.set(merchantId);
        ctx.write(outKey, tw);
    }

    // ===== utilitários =====

    private static boolean isOnline(String useChipUpper) {
        return "ONLINE TRANSACTION".equals(useChipUpper) || "ONLINE".equals(useChipUpper);
    }

    private static boolean hasError(String errUpper) {
        return !(errUpper.isEmpty() || "NONE".equals(errUpper) || "N/A".equals(errUpper));
    }

    private static String nz(String s) {
        if (s == null) return "";
        return s.replace("\"","").trim().toUpperCase();
    }

    private static long parseAmountToCents(String raw) {
        try {
            String c = raw.replace("\"","").replace("$","").replace(",","").trim();
            if (c.isEmpty()) return Long.MIN_VALUE;
            double d = Double.parseDouble(c);
            return Math.round(d * 100.0);
        } catch (Exception e) {
            return Long.MIN_VALUE;
        }
    }

    /** Split CSV simples que respeita aspas */
    private static String[] splitCsv(String line) {
        List<String> res = new ArrayList<>();
        StringBuilder cur = new StringBuilder();
        boolean inQ = false;
        for (int i = 0; i < line.length(); i++) {
            char ch = line.charAt(i);
            if (ch == '\"') inQ = !inQ;
            else if (ch == ',' && !inQ) {
                res.add(cur.toString());
                cur.setLength(0);
            } else cur.append(ch);
        }
        res.add(cur.toString());
        return res.toArray(new String[0]);
    }
}
