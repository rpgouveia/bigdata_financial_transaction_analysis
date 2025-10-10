package routines.advanced.merchanthrisk;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import org.apache.hadoop.io.Writable;

/**
 * Custom Writable minimalista para eventos de transação
 * Carrega múltiplos atributos para permitir métricas ricas no Reduce:
 *  - amountCents (long)
 *  - online (canal)
 *  - hasError (flag de erro)
 *  - city, state, mcc (para predominâncias/segmentações)
 */
public class TransactionMiniWritable implements Writable {

    private long amountCents;
    private boolean online;
    private boolean hasError;
    private String city;
    private String state;
    private String mcc;

    public TransactionMiniWritable() { }

    public TransactionMiniWritable(long amountCents, boolean online, boolean hasError, String city, String state, String mcc) {
        this.amountCents = amountCents;
        this.online = online;
        this.hasError = hasError;
        this.city = city == null ? "" : city;
        this.state = state == null ? "" : state;
        this.mcc = mcc == null ? "" : mcc;
    }

    public long getAmountCents() { return amountCents; }
    public boolean isOnline() { return online; }
    public boolean isHasError() { return hasError; }
    public String getCity() { return city; }
    public String getState() { return state; }
    public String getMcc() { return mcc; }

    @Override public void write(DataOutput out) throws IOException {
        out.writeLong(amountCents);
        out.writeBoolean(online);
        out.writeBoolean(hasError);
        out.writeUTF(city == null ? "" : city);
        out.writeUTF(state == null ? "" : state);
        out.writeUTF(mcc == null ? "" : mcc);
    }

    @Override public void readFields(DataInput in) throws IOException {
        amountCents = in.readLong();
        online = in.readBoolean();
        hasError = in.readBoolean();
        city = in.readUTF();
        state = in.readUTF();
        mcc = in.readUTF();
    }
}
