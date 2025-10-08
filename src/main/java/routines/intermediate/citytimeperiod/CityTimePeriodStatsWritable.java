package routines.intermediate.citytimeperiod;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import org.apache.hadoop.io.WritableComparable;

/**
 * Custom Writable para estatísticas de transações por período do dia
 * Divide transações em: Manhã (0h-11h), Tarde (12h-17h), Noite (18h-23h)
 * Implementa WritableComparable para permitir comparação e ordenação
 */
public class CityTimePeriodStatsWritable implements WritableComparable<CityTimePeriodStatsWritable> {

    private long morningCount;      // Transações na manhã (00:00 - 11:59)
    private long afternoonCount;    // Transações na tarde (12:00 - 17:59)
    private long nightCount;        // Transações na noite (18:00 - 23:59)

    /**
     * Construtor padrão (necessário para Hadoop)
     */
    public CityTimePeriodStatsWritable() {
        this.morningCount = 0;
        this.afternoonCount = 0;
        this.nightCount = 0;
    }

    /**
     * Construtor com valores iniciais
     */
    public CityTimePeriodStatsWritable(long morning, long afternoon, long night) {
        this.morningCount = morning;
        this.afternoonCount = afternoon;
        this.nightCount = night;
    }

    // Getters e Setters
    public long getMorningCount() {
        return morningCount;
    }

    public void setMorningCount(long morningCount) {
        this.morningCount = morningCount;
    }

    public long getAfternoonCount() {
        return afternoonCount;
    }

    public void setAfternoonCount(long afternoonCount) {
        this.afternoonCount = afternoonCount;
    }

    public long getNightCount() {
        return nightCount;
    }

    public void setNightCount(long nightCount) {
        this.nightCount = nightCount;
    }

    /**
     * Retorna o total de transações em todos os períodos
     */
    public long getTotalCount() {
        return morningCount + afternoonCount + nightCount;
    }

    /**
     * Incrementa o contador do período apropriado
     */
    public void incrementMorning() {
        this.morningCount++;
    }

    public void incrementAfternoon() {
        this.afternoonCount++;
    }

    public void incrementNight() {
        this.nightCount++;
    }

    /**
     * Adiciona contadores de outro objeto a este
     */
    public void add(CityTimePeriodStatsWritable other) {
        this.morningCount += other.morningCount;
        this.afternoonCount += other.afternoonCount;
        this.nightCount += other.nightCount;
    }

    /**
     * Calcula percentuais para cada período
     */
    public double getMorningPercentage() {
        long total = getTotalCount();
        return total > 0 ? (morningCount * 100.0 / total) : 0.0;
    }

    public double getAfternoonPercentage() {
        long total = getTotalCount();
        return total > 0 ? (afternoonCount * 100.0 / total) : 0.0;
    }

    public double getNightPercentage() {
        long total = getTotalCount();
        return total > 0 ? (nightCount * 100.0 / total) : 0.0;
    }

    /**
     * Identifica o período com maior movimento
     */
    public String getPeakPeriod() {
        if (morningCount >= afternoonCount && morningCount >= nightCount) {
            return "Manhã";
        } else if (afternoonCount >= morningCount && afternoonCount >= nightCount) {
            return "Tarde";
        } else {
            return "Noite";
        }
    }

    /**
     * Identifica o período com menor movimento
     */
    public String getLowestPeriod() {
        if (morningCount <= afternoonCount && morningCount <= nightCount) {
            return "Manhã";
        } else if (afternoonCount <= morningCount && afternoonCount <= nightCount) {
            return "Tarde";
        } else {
            return "Noite";
        }
    }

    /**
     * Serialização: escreve o objeto para o fluxo de saída
     */
    @Override
    public void write(DataOutput out) throws IOException {
        out.writeLong(morningCount);
        out.writeLong(afternoonCount);
        out.writeLong(nightCount);
    }

    /**
     * Desserialização: lê o objeto do fluxo de entrada
     */
    @Override
    public void readFields(DataInput in) throws IOException {
        morningCount = in.readLong();
        afternoonCount = in.readLong();
        nightCount = in.readLong();
    }

    /**
     * Método compareTo para ordenação
     * Compara primeiro por total de transações (decrescente)
     * Em caso de empate, compara período dominante
     */
    @Override
    public int compareTo(CityTimePeriodStatsWritable other) {
        // Comparar por total de transações (ordem decrescente)
        long thisTotal = this.getTotalCount();
        long otherTotal = other.getTotalCount();

        if (thisTotal != otherTotal) {
            return Long.compare(otherTotal, thisTotal);
        }

        // Se empate no total, comparar pelo período com mais movimento
        long thisMax = Math.max(morningCount, Math.max(afternoonCount, nightCount));
        long otherMax = Math.max(other.morningCount, Math.max(other.afternoonCount, other.nightCount));

        return Long.compare(otherMax, thisMax);
    }

    /**
     * ToString para output legível no arquivo
     * Usado pelo Hadoop ao escrever resultados
     */
    @Override
    public String toString() {
        return String.format("Manhã: %d (%.2f%%) | Tarde: %d (%.2f%%) | Noite: %d (%.2f%%) | Total: %d | Pico: %s",
                morningCount, getMorningPercentage(),
                afternoonCount, getAfternoonPercentage(),
                nightCount, getNightPercentage(),
                getTotalCount(),
                getPeakPeriod());
    }

    /**
     * Formato para debugging
     */
    public String toDebugString() {
        return String.format("TimePeriodStats{morning=%d, afternoon=%d, night=%d, total=%d}",
                morningCount, afternoonCount, nightCount, getTotalCount());
    }

    /**
     * Formato compacto separado por tabs
     */
    public String toCompactString() {
        return String.format("%d\t%d\t%d\t%d\t%s",
                morningCount, afternoonCount, nightCount, getTotalCount(), getPeakPeriod());
    }

    /**
     * Equals para comparação
     */
    @Override
    public boolean equals(Object obj) {
        if (this == obj) return true;
        if (obj == null || getClass() != obj.getClass()) return false;

        CityTimePeriodStatsWritable that = (CityTimePeriodStatsWritable) obj;
        return morningCount == that.morningCount &&
                afternoonCount == that.afternoonCount &&
                nightCount == that.nightCount;
    }

    /**
     * HashCode para uso em coleções
     */
    @Override
    public int hashCode() {
        int result = (int) (morningCount ^ (morningCount >>> 32));
        result = 31 * result + (int) (afternoonCount ^ (afternoonCount >>> 32));
        result = 31 * result + (int) (nightCount ^ (nightCount >>> 32));
        return result;
    }

    /**
     * Cria uma cópia do objeto
     */
    public CityTimePeriodStatsWritable copy() {
        return new CityTimePeriodStatsWritable(this.morningCount, this.afternoonCount, this.nightCount);
    }
}