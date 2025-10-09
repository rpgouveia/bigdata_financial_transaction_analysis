package routines.advanced.clientbehaviorchipuse;

import java.io.IOException;
import java.math.BigDecimal;
import java.math.RoundingMode;
import java.util.ArrayList;
import java.util.List;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

/**
 * Job 1 - Mapper:
 * Lê CSV, ignora cabeçalho e emite:
 *   KEY:   client_id
 *   VALUE: TransactionMiniWritable{ isOnline, hasError, amountCents, city, state, mcc }
 *
 * Colunas do CSV:
 * id(0), date(1), client_id(2), card_id(3), amount(4), use_chip(5),
 * merchant_id(6), merchant_city(7), merchant_state(8), zip(9), mcc(10), errors(11)
 *
 */
public class ClientAggMapper extends Mapper<LongWritable, Text, Text, TransactionMiniWritable> {

    private final Text outKey = new Text();

    private long recordsProcessed = 0;
    private long validRecords = 0;
    private long headerSkipped = 0;
    private long invalidRecords = 0;

    @Override
    protected void map(LongWritable key, Text value, Context ctx) throws IOException, InterruptedException {
        recordsProcessed++;
        String line = value.toString();

        // ignora cabeçalho
        if (line.startsWith("id,") || line.startsWith("\"id\"")) {
            headerSkipped++;
            return;
        }

        try {
            String[] parts = splitCsv(line);
            if (parts.length < 12) {
                invalidRecords++;
                return;
            }

            String clientId = trimQ(parts[2]);
            String amountRaw = trimQ(parts[4]);
            String channelRaw = trimQ(parts[5]).toUpperCase(); // "SWIPE TRANSACTION" | "ONLINE TRANSACTION"
            String merchantCity = trimQ(parts[7]);
            String merchantState = trimQ(parts[8]);
            String mcc = trimQ(parts[10]);
            String errorsRaw = parts[11];

            if (clientId.isEmpty()) {
                invalidRecords++;
                return;
            }

            boolean isOnline = "ONLINE TRANSACTION".equals(channelRaw);
            // Se vier algo inesperado, você pode decidir: default false (swipe) ou descartar:
            // if (!"ONLINE TRANSACTION".equals(channelRaw) && !"SWIPE TRANSACTION".equals(channelRaw)) { invalidRecords++; return; }

            long amountCents = parseAmountToCents(amountRaw);
            if (amountCents == Long.MIN_VALUE) {
                invalidRecords++;
                return;
            }

            boolean hasError = (errorsRaw != null && !errorsRaw.trim().isEmpty());

            TransactionMiniWritable mini = new TransactionMiniWritable(
                    isOnline, hasError, amountCents, merchantCity, merchantState, mcc
            );

            outKey.set(clientId);
            ctx.write(outKey, mini);
            validRecords++;

        } catch (Exception e) {
            invalidRecords++;
            ctx.setStatus("Erro parse Mapper1: " + e.getMessage());
        }

        if (recordsProcessed % 50000 == 0) {
            ctx.setStatus(String.format("Mapper1 registros=%d ok=%d bad=%d header=%d",
                    recordsProcessed, validRecords, invalidRecords, headerSkipped));
        }
    }

    @Override
    protected void cleanup(Context context) throws IOException, InterruptedException {
        System.out.println("=== ClientAggMapper ===");
        System.out.println("processed=" + recordsProcessed + " valid=" + validRecords +
                " invalid=" + invalidRecords + " header=" + headerSkipped);
        super.cleanup(context);
    }

    // --- helpers ---

    private static String trimQ(String s) {
        if (s == null) return "";
        String t = s.trim();
        if (t.startsWith("\"") && t.endsWith("\"") && t.length() >= 2) {
            t = t.substring(1, t.length() - 1);
        }
        return t.trim();
    }

    private static long parseAmountToCents(String raw) {
        if (raw == null || raw.trim().isEmpty()) return Long.MIN_VALUE;
        try {
            String clean = raw.trim().replace("\"","").replace("$","").replace(" ","").replace(",","");
            if (clean.isEmpty()) return Long.MIN_VALUE;
            BigDecimal bd = new BigDecimal(clean);
            BigDecimal cents = bd.movePointRight(2);
            return cents.setScale(0, RoundingMode.HALF_UP).longValueExact();
        } catch (Exception e) {
            return Long.MIN_VALUE;
        }
    }

    private static String[] splitCsv(String line) {
        List<String> res = new ArrayList<>();
        StringBuilder cur = new StringBuilder();
        boolean inQuotes = false;
        for (int i = 0; i < line.length(); i++) {
            char ch = line.charAt(i);
            if (ch == '\"') {
                inQuotes = !inQuotes;
            } else if (ch == ',' && !inQuotes) {
                res.add(cur.toString());
                cur.setLength(0);
            } else {
                cur.append(ch);
            }
        }
        res.add(cur.toString());
        return res.toArray(new String[0]);
    }
}
