package routines.advanced.riskpipeline;

import org.apache.hadoop.io.Writable;
import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

/**
 * Writable que representa a classificação de risco de um cliente.
 * Usado no Step 2 para armazenar score e categoria de risco.
 */
public class ClientRiskWritable implements Writable {

    private String clientId;
    private String riskCategory; // LOW, MEDIUM, HIGH, CRITICAL
    private double riskScore;
    private String riskFactors;
    private int transactionCount;
    private double totalAmount;

    // Construtor padrão necessário para serialização
    public ClientRiskWritable() {
        this.clientId = "";
        this.riskCategory = "";
        this.riskScore = 0.0;
        this.riskFactors = "";
        this.transactionCount = 0;
        this.totalAmount = 0.0;
    }

    // Construtor com parâmetros
    public ClientRiskWritable(String clientId, String riskCategory, double riskScore,
                              String riskFactors, int transactionCount, double totalAmount) {
        this.clientId = clientId;
        this.riskCategory = riskCategory;
        this.riskScore = riskScore;
        this.riskFactors = riskFactors;
        this.transactionCount = transactionCount;
        this.totalAmount = totalAmount;
    }

    @Override
    public void write(DataOutput out) throws IOException {
        out.writeUTF(clientId);
        out.writeUTF(riskCategory);
        out.writeDouble(riskScore);
        out.writeUTF(riskFactors);
        out.writeInt(transactionCount);
        out.writeDouble(totalAmount);
    }

    @Override
    public void readFields(DataInput in) throws IOException {
        this.clientId = in.readUTF();
        this.riskCategory = in.readUTF();
        this.riskScore = in.readDouble();
        this.riskFactors = in.readUTF();
        this.transactionCount = in.readInt();
        this.totalAmount = in.readDouble();
    }

    @Override
    public String toString() {
        return String.format("%s\t%s\t%.2f\t%s\t%d\t%.2f",
                clientId, riskCategory, riskScore, riskFactors,
                transactionCount, totalAmount);
    }

    // Getters e Setters
    public String getClientId() { return clientId; }
    public void setClientId(String clientId) { this.clientId = clientId; }

    public String getRiskCategory() { return riskCategory; }
    public void setRiskCategory(String riskCategory) {
        this.riskCategory = riskCategory;
    }

    public double getRiskScore() { return riskScore; }
    public void setRiskScore(double riskScore) { this.riskScore = riskScore; }

    public String getRiskFactors() { return riskFactors; }
    public void setRiskFactors(String riskFactors) {
        this.riskFactors = riskFactors;
    }

    public int getTransactionCount() { return transactionCount; }
    public void setTransactionCount(int transactionCount) {
        this.transactionCount = transactionCount;
    }

    public double getTotalAmount() { return totalAmount; }
    public void setTotalAmount(double totalAmount) {
        this.totalAmount = totalAmount;
    }
}