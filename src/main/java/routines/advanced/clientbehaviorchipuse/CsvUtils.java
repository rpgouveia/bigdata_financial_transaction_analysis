package routines.advanced.clientbehaviorchipuse;

import java.math.BigDecimal;
import java.math.RoundingMode;
import java.util.ArrayList;
import java.util.List;

class CsvUtils {

    static String[] splitCsv(String line) {
        List<String> result = new ArrayList<>();
        StringBuilder cur = new StringBuilder();
        boolean inQuotes = false;
        for (int i=0;i<line.length();i++) {
            char ch = line.charAt(i);
            if (ch=='"') inQuotes = !inQuotes;
            else if (ch==',' && !inQuotes) { result.add(cur.toString()); cur.setLength(0); }
            else cur.append(ch);
        }
        result.add(cur.toString());
        return result.toArray(new String[0]);
    }

    static String clean(String s) {
        if (s==null) return "";
        return s.trim().replace("\"","").toUpperCase();
    }

    static String city(String s) {
        String v = clean(s);
        if (v.isEmpty() || "NULL".equals(v) || "N/A".equals(v)) return "UNKNOWN";
        return v;
    }

    static String state(String s) {
        String v = clean(s);
        if (v.isEmpty() || "NULL".equals(v) || "N/A".equals(v)) return "UNKNOWN";
        return v;
    }

    static String mcc(String s) {
        String v = clean(s);
        if (v.isEmpty() || "NULL".equals(v) || "N/A".equals(v)) return "UNKNOWN";
        return v;
    }

    static boolean boolFrom01OrTF(String s) {
        if (s == null) return false;
        String v = s.trim().replace("\"","").toLowerCase();
        return v.equals("1") || v.equals("true") || v.equals("t") || v.equals("y") || v.equals("yes");
    }

    static boolean hasError(String s) {
        if (s == null) return false;
        String v = s.trim().replace("\"","");
        return !v.isEmpty(); // qualquer texto em errors = true
    }

    static long amountToCents(String raw) {
        if (raw == null) return Long.MIN_VALUE;
        try {
            String c = raw.trim().replace("\"","").replace("$","").replace(" ","").replace(",","");
            if (c.isEmpty()) return Long.MIN_VALUE;
            BigDecimal bd = new BigDecimal(c).movePointRight(2);
            return bd.setScale(0, RoundingMode.HALF_UP).longValueExact();
        } catch (Exception e) {
            return Long.MIN_VALUE;
        }
    }
}
