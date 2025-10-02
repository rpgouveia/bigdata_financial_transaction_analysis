package routines.intermediate.citytimeperiod;

import java.io.IOException;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

/**
 * Combiner para agregação local de estatísticas temporais por cidade
 * Reduz o volume de dados transferidos para o Reducer final
 */
public class CityTimePeriodCombiner extends Reducer<Text, CityTimePeriodStatsWritable, Text, CityTimePeriodStatsWritable> {

    // Objeto reutilizável para resultado
    private CityTimePeriodStatsWritable result = new CityTimePeriodStatsWritable();

    /**
     * Método reduce do Combiner - agrega estatísticas localmente
     * @param key Nome da cidade
     * @param values Lista de CityTimePeriodStatsWritable para esta cidade no nó local
     * @param context Contexto para emitir resultado pré-agregado
     */
    @Override
    protected void reduce(Text key, Iterable<CityTimePeriodStatsWritable> values, Context context)
            throws IOException, InterruptedException {

        // Resetar contadores
        long totalMorning = 0;
        long totalAfternoon = 0;
        long totalNight = 0;

        // Somar todos os valores locais para esta cidade
        for (CityTimePeriodStatsWritable stats : values) {
            totalMorning += stats.getMorningCount();
            totalAfternoon += stats.getAfternoonCount();
            totalNight += stats.getNightCount();
        }

        // Criar resultado pré-agregado
        result.setMorningCount(totalMorning);
        result.setAfternoonCount(totalAfternoon);
        result.setNightCount(totalNight);

        // Emitir resultado pré-agregado
        context.write(key, result);
    }
}