package routines.advanced.rfmbyuf;

import java.io.IOException;
import java.math.BigDecimal;
import java.math.RoundingMode;
import java.util.ArrayList;
import java.util.List;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

/**
 * Mapper Job 1:
 * KEY: client_id
 * VAL: TransactionRfmWritable{ timestampMillis, amountCents, city, state }
 */
public class RfmClientMapper extends Mapper<LongWritable, Text, Text, TransactionRfmWritable> {

    private final Text outKey = new Text();

    @Override
    protected void map(LongWritable key, Text value, Context ctx) throws IOException, InterruptedException {
        String line = value.toString();
        if (line.startsWith("id,") || line.startsWith("\"id\"")) return;

        String[] p = splitCsv(line);
        if (p.length < 12) return;

        String clientId = tq(p[2]);
        if (clientId.isEmpty()) return;

        String dateRaw   = tq(p[1]); // "YYYY-MM-DD HH:mm:ss"
        String amountRaw = tq(p[4]);
        String city      = tq(p[7]);
        String state     = tq(p[8]);

        long ts = parseTimestampMillis(dateRaw);
        if (ts == Long.MIN_VALUE) return;

        long cents = parseAmountToCents(amountRaw);
        if (cents == Long.MIN_VALUE) return;

        TransactionRfmWritable v = new TransactionRfmWritable(ts, cents, city, state);
        outKey.set(clientId);
        ctx.write(outKey, v);
    }

    private static String tq(String s) {
        if (s == null) return "";
        String t = s.trim();
        if (t.startsWith("\"") && t.endsWith("\"") && t.length() >= 2) {
            t = t.substring(1, t.length() - 1);
        }
        return t.trim();
    }

    private static long parseAmountToCents(String raw) {
        try {
            String c = raw.trim().replace("\"","").replace("$","").replace(",","").replace(" ","");
            if (c.isEmpty()) return Long.MIN_VALUE;
            BigDecimal bd = new BigDecimal(c);
            return bd.movePointRight(2).setScale(0, RoundingMode.HALF_UP).longValueExact();
        } catch (Exception e) { return Long.MIN_VALUE; }
    }

    private static long parseTimestampMillis(String dt) {
        try {
            // esperado: "2010-01-01 00:01:00"
            String[] parts = dt.split(" ");
            if (parts.length < 2) return Long.MIN_VALUE;
            String[] date = parts[0].split("-");
            String[] time = parts[1].split(":");
            if (date.length < 3 || time.length < 2) return Long.MIN_VALUE;

            int Y = Integer.parseInt(date[0]);
            int M = Integer.parseInt(date[1]);
            int D = Integer.parseInt(date[2]);
            int h = Integer.parseInt(time[0]);
            int m = Integer.parseInt(time[1]);
            int s = (time.length > 2) ? Integer.parseInt(time[2]) : 0;

            java.time.LocalDateTime ldt = java.time.LocalDateTime.of(Y, M, D, h, m, s);
            return ldt.atZone(java.time.ZoneOffset.UTC).toInstant().toEpochMilli();
        } catch (Exception e) { return Long.MIN_VALUE; }
    }

    private static String[] splitCsv(String line) {
        List<String> res = new ArrayList<>();
        StringBuilder cur = new StringBuilder();
        boolean inQ = false;
        for (int i = 0; i < line.length(); i++) {
            char ch = line.charAt(i);
            if (ch == '\"') inQ = !inQ;
            else if (ch == ',' && !inQ) { res.add(cur.toString()); cur.setLength(0); }
            else cur.append(ch);
        }
        res.add(cur.toString());
        return res.toArray(new String[0]);
    }
}
