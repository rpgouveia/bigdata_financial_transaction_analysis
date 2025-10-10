package routines.advanced.categorybytimeperiod;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import org.apache.hadoop.io.WritableComparable;
import org.apache.hadoop.io.WritableUtils;

/**
 * Chave composta para agrupar por Cidade + Período do Dia
 * Permite identificar categorias específicas por horário em cada cidade
 * Implementa WritableComparable para sorting e grouping no Hadoop
 */
public class CityPeriodKey implements WritableComparable<CityPeriodKey> {

    private String cityName;      // Nome da cidade
    private String timePeriod;    // Período: MORNING, AFTERNOON, NIGHT

    /**
     * Construtor padrão (necessário para Hadoop)
     */
    public CityPeriodKey() {
        this.cityName = "";
        this.timePeriod = "";
    }

    /**
     * Construtor com valores iniciais
     */
    public CityPeriodKey(String cityName, String timePeriod) {
        this.cityName = cityName;
        this.timePeriod = timePeriod;
    }

    // Getters e Setters
    public String getCityName() {
        return cityName;
    }

    public void setCityName(String cityName) {
        this.cityName = cityName;
    }

    public String getTimePeriod() {
        return timePeriod;
    }

    public void setTimePeriod(String timePeriod) {
        this.timePeriod = timePeriod;
    }

    /**
     * Serialização: escreve o objeto para o fluxo de saída
     */
    @Override
    public void write(DataOutput out) throws IOException {
        WritableUtils.writeString(out, cityName);
        WritableUtils.writeString(out, timePeriod);
    }

    /**
     * Desserialização: lê o objeto do fluxo de entrada
     */
    @Override
    public void readFields(DataInput in) throws IOException {
        cityName = WritableUtils.readString(in);
        timePeriod = WritableUtils.readString(in);
    }

    /**
     * Método compareTo para sorting
     * Ordena primeiro por cidade (alfabético), depois por período (ordem definida)
     */
    @Override
    public int compareTo(CityPeriodKey other) {
        // Comparar por cidade primeiro
        int cityComparison = this.cityName.compareTo(other.cityName);
        if (cityComparison != 0) {
            return cityComparison;
        }

        // Se mesma cidade, comparar por período (ordem: MORNING -> AFTERNOON -> NIGHT)
        return getPeriodOrder(this.timePeriod) - getPeriodOrder(other.timePeriod);
    }

    /**
     * Define ordem dos períodos para sorting
     */
    private int getPeriodOrder(String period) {
        switch (period) {
            case "MORNING":
                return 1;
            case "AFTERNOON":
                return 2;
            case "NIGHT":
                return 3;
            default:
                return 4;
        }
    }

    /**
     * ToString para debugging e output
     */
    @Override
    public String toString() {
        return cityName + "-" + timePeriod;
    }

    /**
     * Formato legível para output final
     */
    public String toDisplayString() {
        String periodName;
        switch (timePeriod) {
            case "MORNING":
                periodName = "Manhã";
                break;
            case "AFTERNOON":
                periodName = "Tarde";
                break;
            case "NIGHT":
                periodName = "Noite";
                break;
            default:
                periodName = timePeriod;
        }
        return String.format("%s [%s]", cityName, periodName);
    }

    /**
     * Equals para comparação
     */
    @Override
    public boolean equals(Object obj) {
        if (this == obj) return true;
        if (obj == null || getClass() != obj.getClass()) return false;

        CityPeriodKey that = (CityPeriodKey) obj;
        return cityName.equals(that.cityName) && timePeriod.equals(that.timePeriod);
    }

    /**
     * HashCode para uso em coleções
     */
    @Override
    public int hashCode() {
        int result = cityName.hashCode();
        result = 31 * result + timePeriod.hashCode();
        return result;
    }
}