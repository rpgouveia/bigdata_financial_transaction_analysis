package routines.advanced.riskanalysis;

import org.apache.hadoop.io.WritableComparable;
import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

/**
 * WritableComparable que representa a classificação de risco de um cliente.
 * Usado no Step 2 para armazenar score e categoria de risco.
 * Implementa comparação por risk score para permitir rankings.
 */
public class ClientRiskWritable implements WritableComparable<ClientRiskWritable> {

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

    /**
     * Método compareTo para ordenação.
     * Compara por riskScore (decrescente - maior risco primeiro).
     */
    @Override
    public int compareTo(ClientRiskWritable other) {
        // Comparar por riskScore (decrescente - maior risco primeiro)
        int scoreComparison = Double.compare(other.riskScore, this.riskScore);
        if (scoreComparison != 0) {
            return scoreComparison;
        }

        // Se empate, comparar por categoria (ordem: CRITICAL > HIGH > MEDIUM > LOW)
        int categoryComparison = compareCategories(other.riskCategory, this.riskCategory);
        if (categoryComparison != 0) {
            return categoryComparison;
        }

        // Se ainda empate, comparar por clientId (alfabético)
        return this.clientId.compareTo(other.clientId);
    }

    /**
     * Compara categorias de risco (CRITICAL > HIGH > MEDIUM > LOW).
     */
    private int compareCategories(String cat1, String cat2) {
        int rank1 = getCategoryRank(cat1);
        int rank2 = getCategoryRank(cat2);
        return Integer.compare(rank1, rank2);
    }

    /**
     * Retorna ranking numérico da categoria (maior = mais crítico).
     */
    private int getCategoryRank(String category) {
        switch (category) {
            case "CRITICAL": return 4;
            case "HIGH": return 3;
            case "MEDIUM": return 2;
            case "LOW": return 1;
            default: return 0;
        }
    }

    /**
     * Equals para comparação.
     */
    @Override
    public boolean equals(Object obj) {
        if (this == obj) return true;
        if (obj == null || getClass() != obj.getClass()) return false;

        ClientRiskWritable that = (ClientRiskWritable) obj;
        return Double.compare(that.riskScore, riskScore) == 0 &&
                clientId.equals(that.clientId) &&
                riskCategory.equals(that.riskCategory);
    }

    /**
     * HashCode para uso em coleções.
     */
    @Override
    public int hashCode() {
        int result = clientId.hashCode();
        result = 31 * result + riskCategory.hashCode();
        result = 31 * result + Double.hashCode(riskScore);
        return result;
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