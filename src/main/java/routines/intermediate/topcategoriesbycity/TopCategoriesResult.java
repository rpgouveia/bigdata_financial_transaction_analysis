package routines.intermediate.topcategoriesbycity;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import org.apache.hadoop.io.WritableComparable;
import org.apache.hadoop.io.WritableUtils;

/**
 * Custom Writable para armazenar o resultado das top N categorias (MCC) com suas contagens
 * Usado para emitir o resultado final do ranking no Reducer
 * Implementa WritableComparable para permitir comparação e ordenação
 */
public class TopCategoriesResult implements WritableComparable<TopCategoriesResult> {

    // Arrays para armazenar até 3 categorias
    private String[] mccCodes;      // Códigos MCC das top categorias
    private long[] counts;          // Contagens correspondentes
    private int size;               // Número de categorias armazenadas (1, 2 ou 3)

    /**
     * Construtor padrão (necessário para Hadoop)
     */
    public TopCategoriesResult() {
        this.mccCodes = new String[3];
        this.counts = new long[3];
        this.size = 0;
    }

    /**
     * Construtor que aceita arrays de códigos e contagens
     */
    public TopCategoriesResult(String[] mccCodes, long[] counts, int size) {
        this.mccCodes = new String[3];
        this.counts = new long[3];
        this.size = Math.min(size, 3);

        for (int i = 0; i < this.size; i++) {
            this.mccCodes[i] = mccCodes[i];
            this.counts[i] = counts[i];
        }
    }

    // Getters
    public String[] getMccCodes() {
        return mccCodes;
    }

    public long[] getCounts() {
        return counts;
    }

    public int getSize() {
        return size;
    }

    public String getMccCode(int index) {
        if (index >= 0 && index < size) {
            return mccCodes[index];
        }
        return null;
    }

    public long getCount(int index) {
        if (index >= 0 && index < size) {
            return counts[index];
        }
        return 0;
    }

    /**
     * Retorna o total de transações em todas as categorias
     */
    public long getTotalCount() {
        long total = 0;
        for (int i = 0; i < size; i++) {
            total += counts[i];
        }
        return total;
    }

    /**
     * Serialização: escreve o objeto para o fluxo de saída
     */
    @Override
    public void write(DataOutput out) throws IOException {
        out.writeInt(size);
        for (int i = 0; i < size; i++) {
            WritableUtils.writeString(out, mccCodes[i]);
            out.writeLong(counts[i]);
        }
    }

    /**
     * Desserialização: lê o objeto do fluxo de entrada
     */
    @Override
    public void readFields(DataInput in) throws IOException {
        size = in.readInt();
        for (int i = 0; i < size; i++) {
            mccCodes[i] = WritableUtils.readString(in);
            counts[i] = in.readLong();
        }
    }

    /**
     * Método compareTo para ordenação
     * Compara pelo total de transações (decrescente)
     */
    @Override
    public int compareTo(TopCategoriesResult other) {
        long thisTotal = this.getTotalCount();
        long otherTotal = other.getTotalCount();
        return Long.compare(otherTotal, thisTotal);
    }

    /**
     * ToString para output formatado legível
     * Formato: Top-1: MCC (Descrição) Count | Top-2: ... | Top-3: ...
     */
    @Override
    public String toString() {
        if (size == 0) {
            return "No categories";
        }

        StringBuilder sb = new StringBuilder();
        for (int i = 0; i < size; i++) {
            if (i > 0) {
                sb.append(" | ");
            }

            String description = MCCDescriptionMapper.getDescription(mccCodes[i]);
            sb.append(String.format("Top-%d: %s (%s) %d",
                    i + 1, mccCodes[i], description, counts[i]));
        }

        return sb.toString();
    }

    /**
     * Formato compacto separado por tabs
     */
    public String toCompactString() {
        StringBuilder sb = new StringBuilder();
        for (int i = 0; i < size; i++) {
            if (i > 0) {
                sb.append("\t");
            }
            sb.append(mccCodes[i]).append(":").append(counts[i]);
        }
        return sb.toString();
    }

    /**
     * Equals para comparação
     */
    @Override
    public boolean equals(Object obj) {
        if (this == obj) return true;
        if (obj == null || getClass() != obj.getClass()) return false;

        TopCategoriesResult that = (TopCategoriesResult) obj;

        if (size != that.size) return false;

        for (int i = 0; i < size; i++) {
            if (!mccCodes[i].equals(that.mccCodes[i]) || counts[i] != that.counts[i]) {
                return false;
            }
        }

        return true;
    }

    /**
     * HashCode para uso em coleções
     */
    @Override
    public int hashCode() {
        int result = size;
        for (int i = 0; i < size; i++) {
            result = 31 * result + mccCodes[i].hashCode();
            result = 31 * result + (int) (counts[i] ^ (counts[i] >>> 32));
        }
        return result;
    }
}