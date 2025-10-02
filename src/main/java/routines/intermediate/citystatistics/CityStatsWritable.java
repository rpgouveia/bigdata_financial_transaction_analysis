package routines.intermediate.citystatistics;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import org.apache.hadoop.io.Writable;

/**
 * Custom Writable para estatísticas completas de transações por cidade
 * Armazena: contagem, valor total (em centavos) e calcula valor médio
 */
public class CityStatsWritable implements Writable {

    private long transactionCount;      // Número de transações
    private long totalAmountInCents;    // Valor total em centavos (para precisão)

    /**
     * Construtor padrão (necessário para Hadoop)
     */
    public CityStatsWritable() {
        this.transactionCount = 0;
        this.totalAmountInCents = 0;
    }

    /**
     * Construtor com valores iniciais
     */
    public CityStatsWritable(long count, long totalInCents) {
        this.transactionCount = count;
        this.totalAmountInCents = totalInCents;
    }

    // Getters e Setters
    public long getTransactionCount() {
        return transactionCount;
    }

    public void setTransactionCount(long transactionCount) {
        this.transactionCount = transactionCount;
    }

    public long getTotalAmountInCents() {
        return totalAmountInCents;
    }

    public void setTotalAmountInCents(long totalAmountInCents) {
        this.totalAmountInCents = totalAmountInCents;
    }

    /**
     * Calcula o valor médio por transação em centavos
     */
    public long getAverageAmountInCents() {
        if (transactionCount == 0) {
            return 0;
        }
        return totalAmountInCents / transactionCount;
    }

    /**
     * Retorna o total em dólares (para exibição)
     */
    public double getTotalAmountInDollars() {
        return totalAmountInCents / 100.0;
    }

    /**
     * Retorna a média em dólares (para exibição)
     */
    public double getAverageAmountInDollars() {
        return getAverageAmountInCents() / 100.0;
    }

    /**
     * Adiciona uma transação ao objeto
     */
    public void addTransaction(long amountInCents) {
        this.transactionCount++;
        this.totalAmountInCents += amountInCents;
    }

    /**
     * Adiciona os valores de outro CityStatsWritable a este
     * Útil para agregação no Combiner e Reducer
     */
    public void add(CityStatsWritable other) {
        this.transactionCount += other.transactionCount;
        this.totalAmountInCents += other.totalAmountInCents;
    }

    /**
     * Serialização: escreve o objeto para o fluxo de saída
     */
    @Override
    public void write(DataOutput out) throws IOException {
        out.writeLong(transactionCount);
        out.writeLong(totalAmountInCents);
    }

    /**
     * Desserialização: lê o objeto do fluxo de entrada
     */
    @Override
    public void readFields(DataInput in) throws IOException {
        transactionCount = in.readLong();
        totalAmountInCents = in.readLong();
    }

    /**
     * ToString para debugging
     */
    @Override
    public String toString() {
        return String.format("CityStatsWritable{count=%d, total=$%.2f, avg=$%.2f}",
                transactionCount,
                getTotalAmountInDollars(),
                getAverageAmountInDollars());
    }

    /**
     * Formato para output final
     */
    public String toOutputString() {
        return String.format("Transações: %d | Total: $%.2f | Média: $%.2f",
                transactionCount,
                getTotalAmountInDollars(),
                getAverageAmountInDollars());
    }

    /**
     * Formato compacto separado por tabs
     */
    public String toCompactString() {
        return String.format("%d\t%.2f\t%.2f",
                transactionCount,
                getTotalAmountInDollars(),
                getAverageAmountInDollars());
    }

    /**
     * Equals para comparação
     */
    @Override
    public boolean equals(Object obj) {
        if (this == obj) return true;
        if (obj == null || getClass() != obj.getClass()) return false;

        CityStatsWritable that = (CityStatsWritable) obj;
        return transactionCount == that.transactionCount &&
                totalAmountInCents == that.totalAmountInCents;
    }

    /**
     * HashCode para uso em coleções
     */
    @Override
    public int hashCode() {
        int result = (int) (transactionCount ^ (transactionCount >>> 32));
        result = 31 * result + (int) (totalAmountInCents ^ (totalAmountInCents >>> 32));
        return result;
    }

    /**
     * Cria uma cópia do objeto
     */
    public CityStatsWritable copy() {
        return new CityStatsWritable(this.transactionCount, this.totalAmountInCents);
    }
}