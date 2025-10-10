package routines.advanced.rfmbyuf;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import org.apache.hadoop.io.Writable;

/** Evento mínimo para RFM por cliente. */
public class TransactionRfmWritable implements Writable {

    private long timestampMillis;
    private long amountCents;
    private String city;
    private String state;

    public TransactionRfmWritable() {}

    public TransactionRfmWritable(long ts, long cents, String city, String state) {
        this.timestampMillis = ts;
        this.amountCents = cents;
        this.city = nz(city);
        this.state = nz(state);
    }

    @Override public void write(DataOutput out) throws IOException {
        out.writeLong(timestampMillis);
        out.writeLong(amountCents);
        writeS(out, city);
        writeS(out, state);
    }

    @Override public void readFields(DataInput in) throws IOException {
        timestampMillis = in.readLong();
        amountCents = in.readLong();
        city = readS(in);
        state = readS(in);
    }

    public long getTimestampMillis() { return timestampMillis; }
    public long getAmountCents() { return amountCents; }
    public String getCity() { return city; }
    public String getState() { return state; }

    private static void writeS(DataOutput out, String s) throws IOException {
        if (s == null) { out.writeInt(-1); return; }
        byte[] b = s.getBytes("UTF-8");
        out.writeInt(b.length); out.write(b);
    }

    private static String readS(DataInput in) throws IOException {
        int len = in.readInt(); if (len < 0) return "";
        byte[] b = new byte[len]; in.readFully(b);
        return new String(b, "UTF-8");
    }

    private static String nz(String s) {
        return (s == null || s.trim().isEmpty()) ? "UNKNOWN" : s.trim().toUpperCase();
    }
}
