package routines.advanced.clientbehaviorchipuse;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import org.apache.hadoop.io.Writable;

/**
 * Value do Job 1 (Mapper -> Reducer) com os campos mínimos por transação
 * para perfilar o cliente.
 *
 * Troca principal: campo de canal
 * isOnline (boolean) => true se "ONLINE TRANSACTION", false se "SWIPE TRANSACTION"
 */
public class TransactionMiniWritable implements Writable {

    private boolean isOnline;     // canal: online vs swipe
    private boolean hasError;     // houve erro no registro (coluna errors não vazia)
    private long amountCents;     // valor em centavos
    private String city;          // merchant_city
    private String state;         // merchant_state (UF)
    private String mcc;           // código MCC

    public TransactionMiniWritable() {}

    public TransactionMiniWritable(boolean isOnline, boolean hasError, long amountCents,
                                   String city, String state, String mcc) {
        this.isOnline = isOnline;
        this.hasError = hasError;
        this.amountCents = amountCents;
        this.city = nz(city);
        this.state = nz(state);
        this.mcc = nz(mcc);
    }

    @Override
    public void write(DataOutput out) throws IOException {
        out.writeBoolean(isOnline);
        out.writeBoolean(hasError);
        out.writeLong(amountCents);
        writeString(out, city);
        writeString(out, state);
        writeString(out, mcc);
    }

    @Override
    public void readFields(DataInput in) throws IOException {
        isOnline = in.readBoolean();
        hasError = in.readBoolean();
        amountCents = in.readLong();
        city = readString(in);
        state = readString(in);
        mcc = readString(in);
    }

    // getters / setters
    public boolean isOnline() { return isOnline; }
    public void setOnline(boolean online) { isOnline = online; }

    public boolean isHasError() { return hasError; }
    public void setHasError(boolean hasError) { this.hasError = hasError; }

    public long getAmountCents() { return amountCents; }
    public void setAmountCents(long amountCents) { this.amountCents = amountCents; }

    public String getCity() { return city; }
    public void setCity(String city) { this.city = nz(city); }

    public String getState() { return state; }
    public void setState(String state) { this.state = nz(state); }

    public String getMcc() { return mcc; }
    public void setMcc(String mcc) { this.mcc = nz(mcc); }

    // helpers
    private static String nz(String s) {
        return (s == null || s.trim().isEmpty()) ? "UNKNOWN" : s.trim().toUpperCase();
    }

    private static void writeString(DataOutput out, String s) throws IOException {
        if (s == null) {
            out.writeInt(-1);
        } else {
            byte[] bytes = s.getBytes("UTF-8");
            out.writeInt(bytes.length);
            out.write(bytes);
        }
    }

    private static String readString(DataInput in) throws IOException {
        int len = in.readInt();
        if (len < 0) return "";
        byte[] b = new byte[len];
        in.readFully(b);
        return new String(b, "UTF-8");
        // (já normalizamos em setters quando necessário)
    }
}
