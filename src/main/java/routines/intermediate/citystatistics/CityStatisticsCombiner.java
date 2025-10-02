package routines.intermediate.citystatistics;

import java.io.IOException;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

/**
 * Combiner para agregação local de estatísticas por cidade
 * Reduz o volume de dados transferidos para o Reducer final
 */
public class CityStatisticsCombiner extends Reducer<Text, CityStatsWritable, Text, CityStatsWritable> {

    // Objeto reutilizável para resultado
    private CityStatsWritable result = new CityStatsWritable();

    /**
     * Método reduce do Combiner - agrega estatísticas localmente
     * @param key Nome da cidade
     * @param values Lista de CityStatsWritable para esta cidade no nó local
     * @param context Contexto para emitir resultado pré-agregado
     */
    @Override
    protected void reduce(Text key, Iterable<CityStatsWritable> values, Context context)
            throws IOException, InterruptedException {

        // Resetar contadores
        long totalCount = 0;
        long totalAmountInCents = 0;

        // Somar todos os valores locais para esta cidade
        for (CityStatsWritable stats : values) {
            totalCount += stats.getTransactionCount();
            totalAmountInCents += stats.getTotalAmountInCents();
        }

        // Criar resultado pré-agregado
        result.setTransactionCount(totalCount);
        result.setTotalAmountInCents(totalAmountInCents);

        // Emitir resultado pré-agregado
        context.write(key, result);
    }
}