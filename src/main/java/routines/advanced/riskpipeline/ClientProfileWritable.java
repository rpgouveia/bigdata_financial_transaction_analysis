package routines.advanced.riskpipeline;

import org.apache.hadoop.io.Writable;
import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

/**
 * Writable que representa o perfil agregado de um cliente.
 * Usado no Step 1 para armazenar estatísticas comportamentais.
 */
public class ClientProfileWritable implements Writable {

    private String clientId;
    private int transactionCount;
    private double totalAmount;
    private double avgAmount;
    private int uniqueCities;
    private int uniqueMccs;
    private int uniqueCards;
    private long firstTransaction;
    private long lastTransaction;
    private int onlineCount;
    private int swipeCount;
    private int errorCount;
    private int chargebackCount;

    // Construtor padrão necessário para serialização
    public ClientProfileWritable() {
        this.clientId = "";
        this.transactionCount = 0;
        this.totalAmount = 0.0;
        this.avgAmount = 0.0;
        this.uniqueCities = 0;
        this.uniqueMccs = 0;
        this.uniqueCards = 0;
        this.firstTransaction = 0L;
        this.lastTransaction = 0L;
        this.onlineCount = 0;
        this.swipeCount = 0;
        this.errorCount = 0;
        this.chargebackCount = 0;
    }

    // Construtor com parâmetros
    public ClientProfileWritable(String clientId, int transactionCount, double totalAmount,
                                 double avgAmount, int uniqueCities, int uniqueMccs,
                                 int uniqueCards, long firstTransaction, long lastTransaction,
                                 int onlineCount, int swipeCount, int errorCount,
                                 int chargebackCount) {
        this.clientId = clientId;
        this.transactionCount = transactionCount;
        this.totalAmount = totalAmount;
        this.avgAmount = avgAmount;
        this.uniqueCities = uniqueCities;
        this.uniqueMccs = uniqueMccs;
        this.uniqueCards = uniqueCards;
        this.firstTransaction = firstTransaction;
        this.lastTransaction = lastTransaction;
        this.onlineCount = onlineCount;
        this.swipeCount = swipeCount;
        this.errorCount = errorCount;
        this.chargebackCount = chargebackCount;
    }

    @Override
    public void write(DataOutput out) throws IOException {
        out.writeUTF(clientId);
        out.writeInt(transactionCount);
        out.writeDouble(totalAmount);
        out.writeDouble(avgAmount);
        out.writeInt(uniqueCities);
        out.writeInt(uniqueMccs);
        out.writeInt(uniqueCards);
        out.writeLong(firstTransaction);
        out.writeLong(lastTransaction);
        out.writeInt(onlineCount);
        out.writeInt(swipeCount);
        out.writeInt(errorCount);
        out.writeInt(chargebackCount);
    }

    @Override
    public void readFields(DataInput in) throws IOException {
        this.clientId = in.readUTF();
        this.transactionCount = in.readInt();
        this.totalAmount = in.readDouble();
        this.avgAmount = in.readDouble();
        this.uniqueCities = in.readInt();
        this.uniqueMccs = in.readInt();
        this.uniqueCards = in.readInt();
        this.firstTransaction = in.readLong();
        this.lastTransaction = in.readLong();
        this.onlineCount = in.readInt();
        this.swipeCount = in.readInt();
        this.errorCount = in.readInt();
        this.chargebackCount = in.readInt();
    }

    @Override
    public String toString() {
        return String.format("%s\t%d\t%.2f\t%.2f\t%d\t%d\t%d\t%d\t%d\t%d\t%d\t%d\t%d",
                clientId, transactionCount, totalAmount, avgAmount, uniqueCities,
                uniqueMccs, uniqueCards, firstTransaction, lastTransaction,
                onlineCount, swipeCount, errorCount, chargebackCount);
    }

    // Getters e Setters
    public String getClientId() { return clientId; }
    public void setClientId(String clientId) { this.clientId = clientId; }

    public int getTransactionCount() { return transactionCount; }
    public void setTransactionCount(int transactionCount) {
        this.transactionCount = transactionCount;
    }

    public double getTotalAmount() { return totalAmount; }
    public void setTotalAmount(double totalAmount) { this.totalAmount = totalAmount; }

    public double getAvgAmount() { return avgAmount; }
    public void setAvgAmount(double avgAmount) { this.avgAmount = avgAmount; }

    public int getUniqueCities() { return uniqueCities; }
    public void setUniqueCities(int uniqueCities) { this.uniqueCities = uniqueCities; }

    public int getUniqueMccs() { return uniqueMccs; }
    public void setUniqueMccs(int uniqueMccs) { this.uniqueMccs = uniqueMccs; }

    public int getUniqueCards() { return uniqueCards; }
    public void setUniqueCards(int uniqueCards) { this.uniqueCards = uniqueCards; }

    public long getFirstTransaction() { return firstTransaction; }
    public void setFirstTransaction(long firstTransaction) {
        this.firstTransaction = firstTransaction;
    }

    public long getLastTransaction() { return lastTransaction; }
    public void setLastTransaction(long lastTransaction) {
        this.lastTransaction = lastTransaction;
    }

    public int getOnlineCount() { return onlineCount; }
    public void setOnlineCount(int onlineCount) { this.onlineCount = onlineCount; }

    public int getSwipeCount() { return swipeCount; }
    public void setSwipeCount(int swipeCount) { this.swipeCount = swipeCount; }

    public int getErrorCount() { return errorCount; }
    public void setErrorCount(int errorCount) { this.errorCount = errorCount; }

    public int getChargebackCount() { return chargebackCount; }
    public void setChargebackCount(int chargebackCount) {
        this.chargebackCount = chargebackCount;
    }
}