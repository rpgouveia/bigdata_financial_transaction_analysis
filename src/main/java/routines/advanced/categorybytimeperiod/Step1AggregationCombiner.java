package routines.advanced.categorybytimeperiod;

import java.io.IOException;
import java.util.HashMap;
import java.util.Map;
import org.apache.hadoop.mapreduce.Reducer;
import routines.intermediate.topcategoriesbycity.MCCTransactionCount;

/**
 * Step 1 Combiner - Agregação local de contagens de MCC por cidade-período
 *
 * Parte do Job 1 (Agregação) do pipeline multi-step.
 * Reduz significativamente o volume de dados transferidos para o Reducer
 * ao agregar contagens localmente em cada nó do cluster.
 *
 * Input:  (CityPeriodKey, MCCTransactionCount) - múltiplas ocorrências
 * Output: (CityPeriodKey, MCCTransactionCount) - agregado localmente
 */
public class Step1AggregationCombiner extends Reducer<CityPeriodKey, MCCTransactionCount, CityPeriodKey, MCCTransactionCount> {

    /**
     * Método reduce do Combiner - agrega contagens de MCC localmente
     *
     * @param key Chave composta (cidade + período)
     * @param values Lista de MCCTransactionCount no nó local
     * @param context Contexto para emitir resultados pré-agregados
     */
    @Override
    protected void reduce(CityPeriodKey key, Iterable<MCCTransactionCount> values, Context context)
            throws IOException, InterruptedException {

        // HashMap para agregar contagens por MCC localmente
        Map<String, Long> localMccCounts = new HashMap<>();

        // Somar todos os valores locais para esta cidade-período
        for (MCCTransactionCount mccCount : values) {
            String mcc = mccCount.getMccCode();
            long count = mccCount.getCount();
            localMccCounts.put(mcc, localMccCounts.getOrDefault(mcc, 0L) + count);
        }

        // Emitir resultados pré-agregados
        for (Map.Entry<String, Long> entry : localMccCounts.entrySet()) {
            MCCTransactionCount aggregated = new MCCTransactionCount(
                    entry.getKey(), entry.getValue());
            context.write(key, aggregated);
        }
    }
}