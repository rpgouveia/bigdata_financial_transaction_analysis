package routines.intermediate.topcategoriesbycity;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import org.apache.hadoop.io.WritableComparable;
import org.apache.hadoop.io.WritableUtils;

/**
 * Custom Writable para armazenar um código MCC com sua contagem de transações
 * Usado para transmitir dados de categoria entre Mapper e Reducer
 * Implementa WritableComparable para permitir comparação e ordenação
 */
public class MCCTransactionCount implements WritableComparable<MCCTransactionCount> {

    private String mccCode;      // Código MCC (ex: "5812")
    private long count;          // Número de transações

    /**
     * Construtor padrão (necessário para Hadoop)
     */
    public MCCTransactionCount() {
        this.mccCode = "";
        this.count = 0;
    }

    /**
     * Construtor com valores iniciais
     */
    public MCCTransactionCount(String mccCode, long count) {
        this.mccCode = mccCode;
        this.count = count;
    }

    // Getters e Setters
    public String getMccCode() {
        return mccCode;
    }

    public void setMccCode(String mccCode) {
        this.mccCode = mccCode;
    }

    public long getCount() {
        return count;
    }

    public void setCount(long count) {
        this.count = count;
    }

    /**
     * Incrementa o contador
     */
    public void increment() {
        this.count++;
    }

    /**
     * Adiciona uma contagem ao total
     */
    public void add(long amount) {
        this.count += amount;
    }

    /**
     * Serialização: escreve o objeto para o fluxo de saída
     */
    @Override
    public void write(DataOutput out) throws IOException {
        WritableUtils.writeString(out, mccCode);
        out.writeLong(count);
    }

    /**
     * Desserialização: lê o objeto do fluxo de entrada
     */
    @Override
    public void readFields(DataInput in) throws IOException {
        mccCode = WritableUtils.readString(in);
        count = in.readLong();
    }

    /**
     * Método compareTo para ordenação
     * Compara primeiro por contagem (decrescente)
     * Em caso de empate, compara por código MCC (alfabético)
     */
    @Override
    public int compareTo(MCCTransactionCount other) {
        // Comparar por contagem (ordem decrescente - maior primeiro)
        if (this.count != other.count) {
            return Long.compare(other.count, this.count);
        }

        // Se empate, comparar por código MCC (ordem alfabética)
        return this.mccCode.compareTo(other.mccCode);
    }

    /**
     * ToString para debugging
     */
    @Override
    public String toString() {
        return String.format("MCCTransactionCount{mcc=%s, count=%d}", mccCode, count);
    }

    /**
     * Equals para comparação
     */
    @Override
    public boolean equals(Object obj) {
        if (this == obj) return true;
        if (obj == null || getClass() != obj.getClass()) return false;

        MCCTransactionCount that = (MCCTransactionCount) obj;
        return count == that.count && mccCode.equals(that.mccCode);
    }

    /**
     * HashCode para uso em coleções
     */
    @Override
    public int hashCode() {
        int result = mccCode.hashCode();
        result = 31 * result + (int) (count ^ (count >>> 32));
        return result;
    }
}